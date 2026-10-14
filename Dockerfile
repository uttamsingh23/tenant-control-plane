# Builds both binaries in one image; compose picks the command per service.
FROM golang:1.26.9-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# Alpine rather than distroless so busybox wget is available for the
# compose health check of the api.
FROM alpine:3.22
RUN adduser -D -H -u 10001 app
COPY --from=build /out/api /out/worker /usr/local/bin/
USER app
EXPOSE 8080
CMD ["api"]
