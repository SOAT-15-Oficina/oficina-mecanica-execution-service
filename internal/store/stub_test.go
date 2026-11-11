package store

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type stubAPI struct {
	describeOut *dynamodb.DescribeTableOutput
	err         error
	describedAs string

	getOut   *dynamodb.GetItemOutput
	pages    []*dynamodb.QueryOutput
	queries  []*dynamodb.QueryInput
	puts     []*dynamodb.PutItemInput
	updates  []*dynamodb.UpdateItemInput
	transact []*dynamodb.TransactWriteItemsInput
	gets     []*dynamodb.GetItemInput
}

func (s *stubAPI) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	s.describedAs = *in.TableName
	return s.describeOut, s.err
}

func (s *stubAPI) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	s.gets = append(s.gets, in)
	if s.err != nil {
		return nil, s.err
	}
	if s.getOut == nil {
		return &dynamodb.GetItemOutput{}, nil
	}
	return s.getOut, nil
}

func (s *stubAPI) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	s.puts = append(s.puts, in)
	return &dynamodb.PutItemOutput{}, s.err
}

func (s *stubAPI) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	s.updates = append(s.updates, in)
	return &dynamodb.UpdateItemOutput{}, s.err
}

func (s *stubAPI) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	copied := *in
	s.queries = append(s.queries, &copied)
	if s.err != nil {
		return nil, s.err
	}
	if len(s.pages) == 0 {
		return &dynamodb.QueryOutput{}, nil
	}
	page := s.pages[0]
	s.pages = s.pages[1:]
	return page, nil
}

func (s *stubAPI) TransactWriteItems(_ context.Context, in *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	s.transact = append(s.transact, in)
	return &dynamodb.TransactWriteItemsOutput{}, s.err
}

func newStubStore(stub *stubAPI) *Store {
	s := New(stub, "tickets")
	s.now = func() time.Time { return fixedNow }
	return s
}

var fixedNow = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

func row(record any) map[string]types.AttributeValue {
	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		panic(err)
	}
	return item
}

func more() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"PK": str("next")}
}
