module github.com/toncenter/ton-indexer/ton-streaming-go

go 1.26.0

require (
	github.com/deckarep/golang-set/v2 v2.8.0
	github.com/gofiber/fiber/v3 v3.2.0
	github.com/gofiber/contrib/v3/websocket v1.1.2
	github.com/redis/go-redis/v9 v9.18.0
	github.com/toncenter/ton-indexer/ton-emulate-go v0.0.0-00010101000000-000000000000
	github.com/toncenter/ton-indexer/ton-index-go v0.0.0
	github.com/valyala/fasthttp v1.70.0
	github.com/vmihailenco/msgpack/v5 v5.4.1
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/fasthttp/websocket v1.5.12 // indirect
	github.com/gofiber/contrib/v3/websocket v1.1.2 // indirect
	github.com/gofiber/schema v1.7.1 // indirect
	github.com/gofiber/utils/v2 v2.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.9.1 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/lib/pq v1.12.3 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/savsgio/gotils v0.0.0-20250924091648-bce9a52d7761 // indirect
	github.com/tinylib/msgp v1.6.4 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xssnick/tonutils-go v1.16.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
)

replace github.com/toncenter/ton-indexer/ton-index-go => ../ton-index-go

replace github.com/toncenter/ton-indexer/ton-emulate-go => ../ton-emulate-go
