FROM golang:bookworm as builder

RUN go install github.com/swaggo/swag/cmd/swag@latest

ADD index/ /go/app/index/
ADD main.go /go/app/main.go
ADD go.mod /go/app/go.mod
ADD go.sum /go/app/go.sum
RUN cd /go/app && swag init && go build -o ton-index-go ./main.go

FROM ubuntu:jammy
RUN apt-get update && apt install --yes bash curl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /go/app/ton-index-go /usr/local/bin/ton-index-go
COPY entrypoint.sh /app/entrypoint.sh

ENTRYPOINT [ "/app/entrypoint.sh" ]
