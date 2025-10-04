package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ImgProxyUrlBuilder struct {
	key                []byte
	salt               []byte
	ipfsResolveBaseUrl string
}

func NewImgProxyUrlBuilder(key, salt []byte, ipfsResolveBaseUrl string) *ImgProxyUrlBuilder {
	return &ImgProxyUrlBuilder{key: key, salt: salt, ipfsResolveBaseUrl: ipfsResolveBaseUrl}
}

func (b *ImgProxyUrlBuilder) BuildUrl(src string, preset string) string {
	var path string
	if isIpfs(src) {
		src = strings.TrimPrefix(src, "ipfs://")
		src = fmt.Sprintf("%s/%s", b.ipfsResolveBaseUrl, src)
	}
	encodedUrl := base64.RawURLEncoding.EncodeToString([]byte(src))
	path = fmt.Sprintf("/pr:%s/%s", preset, encodedUrl)
	mac := hmac.New(sha256.New, b.key)
	mac.Write(b.salt)
	mac.Write([]byte(path))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("/%s%s", signature, path)
}

func isIpfs(path string) bool {
	return strings.HasPrefix(path, "ipfs://")
}

type ImageUrlUpdater struct {
	pool          *pgxpool.Pool
	urlBuilder    *ImgProxyUrlBuilder
	batchSize     int
	fromAddress   string
	lastProcessed string
}

type MetadataRecord struct {
	Address string
	Type    string
	Image   *string
	Extra   map[string]interface{}
}

func NewImageUrlUpdater(pool *pgxpool.Pool, urlBuilder *ImgProxyUrlBuilder, batchSize int, fromAddress string) *ImageUrlUpdater {
	return &ImageUrlUpdater{
		pool:          pool,
		urlBuilder:    urlBuilder,
		batchSize:     batchSize,
		fromAddress:   fromAddress,
		lastProcessed: fromAddress,
	}
}

func (u *ImageUrlUpdater) getTotalRecordsCount(ctx context.Context) (int64, error) {
	var count int64
	err := u.pool.QueryRow(ctx, `
		SELECT COUNT(*) 
		FROM address_metadata 
		WHERE valid = true AND (image IS NOT NULL OR (extra ? '_image_small'))
	`).Scan(&count)
	return count, err
}

func (u *ImageUrlUpdater) fetchMetadataBatch(ctx context.Context, lastAddress string) ([]MetadataRecord, error) {
	var query string
	var args []interface{}

	if lastAddress == "" {
		query = `
			SELECT address, type, image, extra 
			FROM address_metadata 
			WHERE valid = true AND (image IS NOT NULL OR (extra ? '_image_small'))
			ORDER BY address
			LIMIT $1
		`
		args = []interface{}{u.batchSize}
	} else {
		query = `
			SELECT address, type, image, extra 
			FROM address_metadata 
			WHERE valid = true AND (image IS NOT NULL OR (extra ? '_image_small'))
			AND address > $1
			ORDER BY address
			LIMIT $2
		`
		args = []interface{}{lastAddress, u.batchSize}
	}

	rows, err := u.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata batch: %v", err)
	}
	defer rows.Close()

	var records []MetadataRecord
	for rows.Next() {
		var record MetadataRecord
		var extraBytes []byte

		err := rows.Scan(&record.Address, &record.Type, &record.Image, &extraBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		if extraBytes != nil {
			if err := json.Unmarshal(extraBytes, &record.Extra); err != nil {
				log.Printf("Warning: failed to unmarshal extra data for %s/%s: %v",
					record.Address, record.Type, err)
				record.Extra = make(map[string]interface{})
			}
		} else {
			record.Extra = make(map[string]interface{})
		}

		records = append(records, record)
	}

	return records, rows.Err()
}

func (u *ImageUrlUpdater) updateMetadataRecord(ctx context.Context, record *MetadataRecord) error {
	updated := false

	if record.Image != nil && *record.Image != "" {
		if record.Extra == nil {
			record.Extra = make(map[string]interface{})
		}

		record.Extra["_image_small"] = u.urlBuilder.BuildUrl(*record.Image, "small")
		record.Extra["_image_medium"] = u.urlBuilder.BuildUrl(*record.Image, "medium")
		record.Extra["_image_big"] = u.urlBuilder.BuildUrl(*record.Image, "big")
		updated = true
	}

	if !updated {
		return nil
	}

	extraBytes, err := json.Marshal(record.Extra)
	if err != nil {
		return fmt.Errorf("failed to marshal extra data: %v", err)
	}

	_, err = u.pool.Exec(ctx, `
		UPDATE address_metadata
		SET extra = $1 
		WHERE address = $2 AND type = $3
	`, extraBytes, record.Address, record.Type)

	if err != nil {
		return fmt.Errorf("failed to update record: %v", err)
	}

	return nil
}

func (u *ImageUrlUpdater) printProgress(processed, total int64, startTime time.Time) {
	elapsed := time.Since(startTime)
	rate := float64(processed) / elapsed.Seconds()
	percentage := float64(processed) / float64(total) * 100

	eta := time.Duration(0)
	if rate > 0 {
		remaining := total - processed
		eta = time.Duration(float64(remaining)/rate) * time.Second
	}

	fmt.Printf("\rProgress: %d/%d (%.1f%%) | Rate: %.1f records/sec | Elapsed: %v | ETA: %v",
		processed, total, percentage, rate, elapsed.Truncate(time.Second), eta.Truncate(time.Second))
}

func (u *ImageUrlUpdater) UpdateAllImageUrls(ctx context.Context) error {
	log.Println("Starting image URL update process...")

	totalRecords, err := u.getTotalRecordsCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get total records count: %v", err)
	}

	if totalRecords == 0 {
		log.Println("No records found with images to update")
		return nil
	}

	log.Printf("Found %d records to process", totalRecords)

	if u.fromAddress != "" {
		log.Printf("Resuming from address: %s", u.fromAddress)
	}

	var processed int64
	var processingErrors int64
	startTime := time.Now()

	lastAddress := u.fromAddress
	for processed < totalRecords {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			log.Printf("\nProcess interrupted! Last processed address: %s", u.lastProcessed)
			log.Printf("To resume, use: --from %s", u.lastProcessed)
			return ctx.Err()
		default:
		}

		records, err := u.fetchMetadataBatch(ctx, lastAddress)
		if err != nil {
			return fmt.Errorf("failed to fetch batch after address %s: %v", lastAddress, err)
		}

		if len(records) == 0 {
			break
		}

		for _, record := range records {
			// Check for context cancellation during processing
			select {
			case <-ctx.Done():
				log.Printf("\nProcess interrupted! Last processed address: %s", u.lastProcessed)
				log.Printf("To resume, use: --from %s", u.lastProcessed)
				return ctx.Err()
			default:
			}

			if err := u.updateMetadataRecord(ctx, &record); err != nil {
				log.Printf("Error updating record %s/%s: %v", record.Address, record.Type, err)
				processingErrors++
			}

			processed++
			lastAddress = record.Address
			u.lastProcessed = record.Address

			if processed%100 == 0 || processed == totalRecords {
				u.printProgress(processed, totalRecords, startTime)
			}
		}
	}

	fmt.Println()

	duration := time.Since(startTime)

	log.Printf("Update completed!")
	log.Printf("Total processed: %d records", processed)
	log.Printf("Errors: %d", processingErrors)
	log.Printf("Duration: %v", duration.Truncate(time.Second))

	return nil
}

func initializeDb(ctx context.Context, pgDsn string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(pgDsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %v", err)
	}

	config.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %v", err)
	}

	return pool, nil
}

func main() {
	var pgDsn string
	var imgproxyKey string
	var imgproxySalt string
	var ipfsServerUrl string
	var batchSize int
	var fromAddress string

	flag.StringVar(&pgDsn, "pg", "postgresql://localhost:5432", "PostgreSQL connection string")
	flag.StringVar(&imgproxyKey, "imgproxy-key", "", "New ImgProxy key (hex encoded)")
	flag.StringVar(&imgproxySalt, "imgproxy-salt", "", "New ImgProxy salt (hex encoded)")
	flag.StringVar(&ipfsServerUrl, "ipfs-server-url", "https://ipfs.io/ipfs", "IPFS gateway server URL")
	flag.IntVar(&batchSize, "batch-size", 1000, "Number of records to process in each batch")
	flag.StringVar(&fromAddress, "from", "", "Resume processing from this address (optional)")
	flag.Parse()

	if imgproxyKey == "" || imgproxySalt == "" {
		log.Fatal("Both imgproxy-key and imgproxy-salt must be provided")
	}

	// Decode the new key and salt
	key, err := hex.DecodeString(imgproxyKey)
	if err != nil {
		log.Fatal("Failed to decode imgproxy key: ", err)
	}

	salt, err := hex.DecodeString(imgproxySalt)
	if err != nil {
		log.Fatal("Failed to decode imgproxy salt: ", err)
	}

	urlBuilder := NewImgProxyUrlBuilder(key, salt, ipfsServerUrl)

	// Set up context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received interrupt signal, shutting down gracefully...")
		cancel()
	}()

	pool, err := initializeDb(ctx, pgDsn)
	if err != nil {
		log.Fatal("Error initializing database connection: ", err)
	}
	defer pool.Close()

	updater := NewImageUrlUpdater(pool, urlBuilder, batchSize, fromAddress)

	if err := updater.UpdateAllImageUrls(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Println("Process was interrupted by user")
			os.Exit(0)
		}
		log.Fatal("Error updating image URLs: ", err)
	}
}
