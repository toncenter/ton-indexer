# Metadata imgproxy urls resigner

A utility to regenerate ImgProxy URLs in the address_metadata table with new signing keys.

## Usage

```bash
./resign-improxy-urls \
  -pg "postgresql://user:password@host:port/database" \
  -imgproxy-key "hex_encoded_key" \
  -imgproxy-salt "hex_encoded_salt" \
  -ipfs-server-url "https://ipfs.io/ipfs" \
  -batch-size 1000 \
  -from "optional_resume_address"
```

## Parameters

- `-pg`: PostgreSQL connection string
- `-imgproxy-key`: New ImgProxy key (hex encoded) 
- `-imgproxy-salt`: New ImgProxy salt (hex encoded)
- `-ipfs-server-url`: IPFS gateway URL (default: https://ipfs.io/ipfs)
- `-batch-size`: Records per batch (default: 1000)
- `-from`: Resume from specific address (optional)

## Example

```bash
# Start processing
./resign-improxy-urls -pg "postgresql://localhost/mydb" -imgproxy-key "abc123" -imgproxy-salt "def456"

# If interrupted, resume with:
./resign-improxy-urls -pg "postgresql://localhost/mydb" -imgproxy-key "abc123" -imgproxy-salt "def456" -from "0:ABCDEF..."
```

## Build

```bash
go build -o resign-improxy-urls main.go
``` 