package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/messaging"
	"github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/ticket"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)

func (s *Store) GetTicket(ctx context.Context, workOrderID uuid.UUID) (*ticket.Ticket, error) {
	rows, err := s.queryAll(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": str(ticketPK(workOrderID))},
		ConsistentRead:            aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}

	var t *ticket.Ticket
	var items []itemRecord
	var events []ticket.Event
	for _, row := range rows {
		sk, _ := row["SK"].(*types.AttributeValueMemberS)
		if sk == nil {
			continue
		}
		switch skKind(sk.Value) {
		case skMeta:
			var r metaRecord
			if err := attributevalue.UnmarshalMap(row, &r); err != nil {
				return nil, fmt.Errorf("decode ticket: %w", err)
			}
			if t, err = r.toTicket(); err != nil {
				return nil, err
			}
		case prefixItem:
			var r itemRecord
			if err := attributevalue.UnmarshalMap(row, &r); err != nil {
				return nil, fmt.Errorf("decode item: %w", err)
			}
			items = append(items, r)
		case prefixEvent:
			var r eventRecord
			if err := attributevalue.UnmarshalMap(row, &r); err != nil {
				return nil, fmt.Errorf("decode event: %w", err)
			}
			events = append(events, r.toEvent())
		}
	}
	if t == nil {
		return nil, ticket.ErrNotFound
	}

	sort.SliceStable(items, func(i, j int) bool { return items[i].Position < items[j].Position })
	for _, r := range items {
		t.Items = append(t.Items, r.toItem())
	}
	t.Timeline = append(t.Timeline, events...)
	return t, nil
}

func (s *Store) ListQueue(ctx context.Context, branch string, phase ticket.Phase) ([]ticket.Ticket, error) {
	rows, err := s.queryAll(ctx, &dynamodb.QueryInput{
		IndexName:                 aws.String(QueueIndex),
		KeyConditionExpression:    aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": str(queuePK(branch, phase))},
		ScanIndexForward:          aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}

	queue := make([]ticket.Ticket, 0, len(rows))
	for _, row := range rows {
		var r metaRecord
		if err := attributevalue.UnmarshalMap(row, &r); err != nil {
			return nil, fmt.Errorf("decode queue entry: %w", err)
		}
		t, err := r.toTicket()
		if err != nil {
			return nil, err
		}
		queue = append(queue, *t)
	}
	return queue, nil
}

func (s *Store) Position(ctx context.Context, t *ticket.Ticket) (int, error) {
	in := &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		IndexName:              aws.String(QueueIndex),
		KeyConditionExpression: aws.String("GSI1PK = :pk AND GSI1SK < :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": str(queuePK(t.Branch, t.Phase)),
			":sk": str(QueueKey(t)),
		},
		Select: types.SelectCount,
	}
	ahead := 0
	for {
		out, err := s.client.Query(ctx, in)
		if err != nil {
			return 0, fmt.Errorf("count queue: %w", err)
		}
		ahead += int(out.Count)
		if len(out.LastEvaluatedKey) == 0 {
			return ahead + 1, nil
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func (s *Store) IsProcessed(ctx context.Context, messageID uuid.UUID) (bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.table),
		Key:                  key(messagePK(messageID), skMessage),
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("PK"),
	})
	if err != nil {
		return false, fmt.Errorf("get message %s: %w", messageID, err)
	}
	return len(out.Item) > 0, nil
}

type conditionSlot int

const (
	slotNone conditionSlot = iota
	slotMessage
	slotTicket
	slotBay
)

func (s *Store) Commit(ctx context.Context, c ticket.Change) error {
	now := s.now()
	var actions []types.TransactWriteItem
	var slots []conditionSlot

	add := func(slot conditionSlot, item types.TransactWriteItem) {
		actions = append(actions, item)
		slots = append(slots, slot)
	}

	if c.Inbound != nil {
		put, err := s.put(messageRecord{
			PK:          messagePK(c.Inbound.MessageID),
			SK:          skMessage,
			Type:        c.Inbound.Type,
			WorkOrderID: c.Inbound.WorkOrderID.String(),
			ProcessedAt: now,
			ExpiresAt:   now.Add(MessageTTL).Unix(),
		}, "attribute_not_exists(PK)")
		if err != nil {
			return err
		}
		add(slotMessage, put)
	}

	version := 0
	if c.Ticket != nil {
		version = c.Ticket.Version + 1
		put, err := s.put(toMeta(c.Ticket, version), "attribute_not_exists(PK)")
		if err != nil {
			return err
		}
		if !c.Create {
			put.Put.ConditionExpression = aws.String("#version = :version")
			put.Put.ExpressionAttributeNames = map[string]string{"#version": "version"}
			put.Put.ExpressionAttributeValues = map[string]types.AttributeValue{":version": &types.AttributeValueMemberN{Value: fmt.Sprint(c.Ticket.Version)}}
		}
		add(slotTicket, put)

		for _, it := range c.Items {
			put, err := s.put(toItem(c.Ticket.WorkOrderID, position(c.Ticket, it.WorkOrderServiceID), it), "")
			if err != nil {
				return err
			}
			add(slotNone, put)
		}
		for i, e := range c.Events {
			put, err := s.put(toEvent(c.Ticket.WorkOrderID, i, e), "")
			if err != nil {
				return err
			}
			add(slotNone, put)
		}
	}

	if c.RequireBayBranch != "" {
		add(slotBay, types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
			TableName:                 aws.String(s.table),
			Key:                       key(bayPK(c.RequireBayBranch), skMeta),
			ConditionExpression:       aws.String("attribute_not_exists(PK) OR #available = :true"),
			ExpressionAttributeNames:  map[string]string{"#available": "available"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":true": &types.AttributeValueMemberBOOL{Value: true}},
		}})
	}

	for i, msg := range c.Outbox {
		rec, err := toOutbox(msg, now.Add(time.Duration(i)))
		if err != nil {
			return err
		}
		put, err := s.put(rec, "")
		if err != nil {
			return err
		}
		add(slotNone, put)
	}

	if len(actions) == 0 {
		return nil
	}

	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: actions})
	if err != nil {
		return translateTransactionError(err, slots)
	}
	if c.Ticket != nil {
		c.Ticket.Version = version
	}
	return nil
}

func (s *Store) put(record any, condition string) (types.TransactWriteItem, error) {
	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		return types.TransactWriteItem{}, fmt.Errorf("encode record: %w", err)
	}
	put := &types.Put{TableName: aws.String(s.table), Item: item}
	if condition != "" {
		put.ConditionExpression = aws.String(condition)
	}
	return types.TransactWriteItem{Put: put}, nil
}

func position(t *ticket.Ticket, id uuid.UUID) int {
	for i, it := range t.Items {
		if it.WorkOrderServiceID == id {
			return i
		}
	}
	return len(t.Items)
}

func toOutbox(msg messaging.OutboxMessage, createdAt time.Time) (outboxRecord, error) {
	body, err := json.Marshal(msg.Envelope)
	if err != nil {
		return outboxRecord{}, fmt.Errorf("encode envelope: %w", err)
	}
	return outboxRecord{
		PK:          outboxPK(msg.ID),
		SK:          skOutbox,
		GSI1PK:      outboxQueue,
		GSI1SK:      keyTime(createdAt) + "#" + msg.ID.String(),
		ID:          msg.ID.String(),
		Destination: msg.Destination,
		Type:        msg.Envelope.Type,
		Envelope:    string(body),
		Carrier:     msg.Carrier,
		CreatedAt:   createdAt,
	}, nil
}

func translateTransactionError(err error, slots []conditionSlot) error {
	var canceled *types.TransactionCanceledException
	if !errors.As(err, &canceled) {
		return fmt.Errorf("transact write: %w", err)
	}
	for i, reason := range canceled.CancellationReasons {
		if aws.ToString(reason.Code) != "ConditionalCheckFailed" || i >= len(slots) {
			continue
		}
		switch slots[i] {
		case slotMessage:
			return ticket.ErrDuplicateMessage
		case slotTicket:
			return ticket.ErrConflict
		case slotBay:
			return ticket.ErrBayUnavailable
		}
	}
	return fmt.Errorf("transact write: %w", err)
}
