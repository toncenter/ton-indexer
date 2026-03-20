package parse

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"
)

var marketplaceCache = make(map[string]string)

func GetMarketplaceName(marketplaceAddress *AccountAddress, collectionAddress *AccountAddress) (bool, string) {
	if marketplaceAddress != nil {
		if name, exists := marketplaceCache[marketplaceAddress.String()]; exists {
			return true, name
		} else {
			return false, ""
		}
	}

	if collectionAddress != nil {
		if name, exists := marketplaceCache[collectionAddress.String()]; exists {
			return true, name
		}
	}

	return false, ""
}

func LoadMarketplaceCache(pool *pgxpool.Pool) error {
	ctx := context.Background()
	query := `SELECT address, name FROM marketplace_names`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		log.Printf("Warning: Failed to load marketplace cache: %v", err)
		return err
	}
	defer rows.Close()

	marketplaceCache = make(map[string]string)
	for rows.Next() {
		var address, name string
		if err := rows.Scan(&address, &name); err != nil {
			log.Printf("Warning: Failed to scan marketplace row: %v", err)
			continue
		}
		marketplaceCache[address] = name
	}

	log.Printf("Loaded %d marketplace names into cache", len(marketplaceCache))
	return nil
}
