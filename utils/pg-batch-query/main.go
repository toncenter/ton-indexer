package main

import (
	"context"
	"flag"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/schollz/progressbar/v3"
)

var pbar *progressbar.ProgressBar

func GetMaxLT(pgDsn string) (int64, error) {
	ctx := context.Background()
	db, err := pgx.Connect(ctx, pgDsn)
	if err != nil {
		log.Fatalf("failed to open database connection: %s", err.Error())
	}
	defer db.Close(ctx)

	sql := `select max(end_lt) from actions;`
	row := db.QueryRow(ctx, sql)
	var maxLt int64
	err = row.Scan(&maxLt)
	if err != nil {
		return 0, err
	}
	return maxLt, nil
}

func ProcessBatch(db *pgx.Conn, currLt int64) (int64, error) {
}

func main() {
	var pgDsn string
	var batchSize int
	var currLt int64

	flag.StringVar(&pgDsn, "pg", "postgresql://localhost:5432", "PostgreSQL connection string")
	flag.IntVar(&batchSize, "processes", 1000, "Parallel processes")
	flag.Int64Var(&currLt, "fromLt", 0, "Start from lt")
	flag.Parse()

	if currLt == 0 {
		log.Println("reading maxLt from db")
		currLtDb, err := GetMaxLT(pgDsn)
		if err != nil {
			log.Fatal(err)
		}
		currLt = currLtDb
	} else {
		log.Printf("maxLt was specified")
	}
	log.Println("maxLt:", currLt)

	pbar = progressbar.NewOptions(int(currLt), progressbar.OptionFullWidth(), progressbar.OptionShowCount(), progressbar.OptionShowIts())

	prevLt := currLt

	// logic
	ctx := context.Background()
	db, err := pgx.Connect(ctx, pgDsn)
	if err != nil {
		log.Fatalf("failed to open database connection: %s", err.Error())
	}
	defer db.Close(ctx)

	for currLt, err := ProcessBatch(db, currLt); currLt > 0; {
		if err != nil {
			log.Fatal(err)
		}
		err := pbar.Add(int(prevLt - currLt))
		if err != nil {
			log.Fatal(err)
		}
		prevLt = currLt
	}
	log.Println("finished")
}
