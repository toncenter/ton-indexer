package crud

import "github.com/toncenter/ton-indexer/ton-index-go/index/models"

// EnrichmentReader provides the address information used to enrich streaming
// and emulation responses without exposing the underlying storage backend.
type EnrichmentReader interface {
	QueryAddressBookByAddresses([]models.AccountAddress, models.RequestSettings) (models.AddressBook, error)
	QueryMetadataByAddresses([]models.AccountAddress, models.RequestSettings) (models.Metadata, error)
}

var _ EnrichmentReader = (*DbClient)(nil)
var _ EnrichmentReader = (*KvrocksStore)(nil)

// NewEnrichmentReader selects Kvrocks whenever it is configured. PostgreSQL is
// used only when there is no Kvrocks store.
func NewEnrichmentReader(pgDSN string, maxConns int, minConns int, kvrocks *KvrocksStore) (EnrichmentReader, error) {
	if kvrocks != nil {
		return kvrocks, nil
	}
	if pgDSN == "" {
		return nil, nil
	}
	return NewDbClient(pgDSN, maxConns, minConns, nil)
}

func (s *KvrocksStore) QueryAddressBookByAddresses(
	addrList []models.AccountAddress,
	settings models.RequestSettings,
) (models.AddressBook, error) {
	return QueryAddressBookImplKvrocks(addrList, s, settings)
}

func (s *KvrocksStore) QueryMetadataByAddresses(
	addrList []models.AccountAddress,
	settings models.RequestSettings,
) (models.Metadata, error) {
	return QueryMetadataImplKvrocks(addrList, settings, s)
}
