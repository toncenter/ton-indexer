module github.com/toncenter/ton-indexer/ton-streaming-go

go 1.24.0

toolchain go1.24.11

require (
	github.com/deckarep/golang-set/v2 v2.7.0
	github.com/gofiber/fiber/v2 v2.52.5
	github.com/gofiber/websocket/v2 v2.2.1
	github.com/redis/go-redis/v9 v9.7.0
	github.com/toncenter/ton-indexer/ton-emulate-go v0.0.0-00010101000000-000000000000
	github.com/toncenter/ton-indexer/ton-index-go v0.0.0
	github.com/valyala/fasthttp v1.58.0
	github.com/vmihailenco/msgpack/v5 v5.4.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/fasthttp/websocket v1.5.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.7.2 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/oasisprotocol/curve25519-voi v0.0.0-20220328075252-7dd334e3daae // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/savsgio/gotils v0.0.0-20230208104028-c358bd845dee // indirect
	github.com/sigurn/crc16 v0.0.0-20240131213347-83fcde1e29d1 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/tcplisten v1.0.0 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xssnick/tonutils-go v1.15.5 // indirect
	golang.org/x/crypto v0.42.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/toncenter/ton-indexer/ton-index-go => ../ton-index-go

replace github.com/toncenter/ton-indexer/ton-emulate-go => ../ton-emulate-go
