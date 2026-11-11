FROM golang:1.26.1-alpine AS build-stage
WORKDIR /app

ARG VERSION=dev

COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
    -ldflags="-s -w -X github.com/SOAT-15-Oficina/oficina-mecanica-execution-service/internal/observability.version=${VERSION}" \
    -o /execution ./cmd/api

FROM scratch AS release-stage
COPY --from=build-stage /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build-stage /execution /execution
USER 65532:65532
ENTRYPOINT ["/execution"]
