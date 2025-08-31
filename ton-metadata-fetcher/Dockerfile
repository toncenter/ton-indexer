FROM golang:bookworm AS builder

ADD images.go /go/app/images.go
ADD ipfs.go /go/app/ipfs.go
ADD main.go /go/app/main.go
ADD overrides.go /go/app/overrides.go
ADD go.mod /go/app/go.mod
ADD go.sum /go/app/go.sum

WORKDIR /go/app
RUN go build -o ton-metadata-fetcher ./*.go

FROM ubuntu:jammy
RUN apt-get update && apt install --yes bash curl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /go/app/ton-metadata-fetcher /usr/local/bin/ton-metadata-fetcher
COPY entrypoint.sh /app/entrypoint.sh
COPY metadata_overrides.json /app/metadata_overrides.json

ENTRYPOINT [ "/app/entrypoint.sh" ]
