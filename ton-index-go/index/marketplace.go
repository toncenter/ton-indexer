package index

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
)

var MarketplaceCache = make(map[string]string)

func LoadMarketplaceCache(pool *pgxpool.Pool) error {
	ctx := context.Background()
	query := `SELECT address, name FROM marketplace_names`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		log.Printf("Warning: Failed to load marketplace cache: %v", err)
		return err
	}
	defer rows.Close()

	MarketplaceCache = make(map[string]string)
	for rows.Next() {
		var address, name string
		if err := rows.Scan(&address, &name); err != nil {
			log.Printf("Warning: Failed to scan marketplace row: %v", err)
			continue
		}
		MarketplaceCache[address] = name
	}

	log.Printf("Loaded %d marketplace names into cache", len(MarketplaceCache))
	return nil
}

func GetMarketplaceName(marketplaceAddress *AccountAddress, collectionAddress *AccountAddress) (bool, string) {
	if marketplaceAddress != nil {
		marketplaceAddr := string(*marketplaceAddress)
		if name, exists := MarketplaceCache[marketplaceAddr]; exists {
			return true, name
		}
	}

	if collectionAddress != nil {
		collectionAddr := string(*collectionAddress)
		if name, exists := MarketplaceCache[collectionAddr]; exists {
			return true, name
		}
	}

	return false, ""
}
