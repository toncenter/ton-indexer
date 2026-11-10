package validators

import (
	"context"
	_ "embed"
	"net/url"
)

//go:embed affiliations_schema.sql
var AffiliationSchema string

func (s Store) LoadAffiliations(ctx context.Context) ([]Affiliation, error) {
	data, err := s.CH.Query(ctx, `SELECT * FROM gramstat.validator_stake_affiliations FINAL
 WHERE verification_version={version:String} FORMAT JSONEachRow`, url.Values{"param_version": {AffiliationVersion}})
	if err != nil {
		return nil, err
	}
	return decodeRows[Affiliation](data)
}

func (s Store) WriteAffiliations(ctx context.Context, rows []Affiliation) error {
	if len(rows) == 0 {
		return nil
	}
	for _, a := range rows {
		if err := validateAffiliation(a); err != nil {
			return err
		}
	}
	data, err := encodeRows(rows)
	if err != nil {
		return err
	}
	return s.CH.InsertJSON(ctx, "gramstat.validator_stake_affiliations", data)
}
