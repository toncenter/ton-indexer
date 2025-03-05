package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/semaphore"
)

var prepare_sql string = `
create table if not exists  broken_traces_roots(tx_hash tonhash, ignore bool default false, primary key (tx_hash));

create or replace function delete_broken_trace(my_trace_id tonhash) returns tonhash
    language plpgsql
as
$$
begin
    delete from trace_edges where trace_id = my_trace_id;
    delete from traces where trace_id = my_trace_id;

	insert into broken_traces_roots(tx_hash)
    select trace_get_root(my_trace_id)
    on conflict do nothing;
    return my_trace_id;
end;
$$;

create or replace function trace_get_root(transaction_hash tonhash) returns tonhash
    parallel safe
    language plpgsql
as
$$
declare
    parent tonhash;
    current tonhash;
    msg tonhash;
begin
    current := transaction_hash;
    parent := transaction_hash;
    while parent is not NULL loop
        select msg_hash into msg from messages where tx_hash = current and direction = 'in';
        select tx_hash into parent from messages where msg_hash = msg and direction = 'out';
        if parent is not null then current := parent; end if;
    end loop;
    return current;
end;
$$;

create or replace function trace_get_transactions(root_tx_hash tonhash) returns tonhash[]
    parallel safe
    language sql
as
$$
with recursive cte as (
    select hash as tx_hash from transactions where hash = $1
    union all
    select M2.tx_hash
    from messages M1
        join cte TT on TT.tx_hash = M1.tx_hash
        join messages M2
        on M1.msg_hash = M2.msg_hash and M1.direction = 'out' and
            M2.direction = 'in'
)
select array_agg(cte.tx_hash) from cte;
$$;

create or replace function rebuild_trace(root_tx_hash tonhash) returns tonhash
    language plpgsql
as
$$
declare
    new_trace_id tonhash;
    flag bool;
    txs tonhash[];
    new_trace_external_hash tonhash;
    new_trace_start_seqno int;
    new_trace_start_lt bigint;
    new_trace_start_utime int;
    new_trace_end_seqno int;
    new_trace_end_lt bigint;
    new_trace_end_utime int;
    new_trace_nodes int;
    new_trace_edges int;
    new_trace_pending_edges int;
begin
    new_trace_id := root_tx_hash;
    select msg_hash,
        source is null or source = '0:0000000000000000000000000000000000000000000000000000000000000000' 
            or source = '-1:0000000000000000000000000000000000000000000000000000000000000000'
    into new_trace_external_hash, flag
    from messages where tx_hash = root_tx_hash and direction = 'in';
    if not flag then
        new_trace_id := trace_get_root(root_tx_hash);
        insert into broken_traces_roots(tx_hash)
        values (new_trace_id)
        on conflict do nothing;
    end if;
    
    -- get transactions
    with recursive cte as (
        select hash as tx_hash from transactions where hash = $1
        union all
        select M2.tx_hash
        from messages M1
            join cte TT on TT.tx_hash = M1.tx_hash
            join messages M2
            on M1.msg_hash = M2.msg_hash and M1.direction = 'out' and
                M2.direction = 'in'
    )
    select array_agg(cte.tx_hash) into txs from cte;

    -- get meta and update transactions
    update transactions set trace_id = new_trace_id where hash = any(txs);
    update messages set trace_id = new_trace_id where tx_hash = any(txs);

    select
        count(*), min(mc_block_seqno), max(mc_block_seqno), min(lt), max(lt), min(now), max(now)
    into new_trace_nodes, new_trace_start_seqno, new_trace_end_seqno, new_trace_start_lt,
        new_trace_end_lt, new_trace_start_utime, new_trace_end_utime
    from transactions where trace_id = new_trace_id;

    -- build edges
    delete from trace_edges where trace_id = new_trace_id;
    delete from traces where trace_id = new_trace_id;

    insert into traces(trace_id) values (new_trace_id) on conflict do nothing;
    insert into trace_edges(trace_id, msg_hash, left_tx, right_tx, incomplete, broken)
    select
        new_trace_id, msg_hash,
        max(case when direction = 'out' then tx_hash end) as left_tx,
        max(case when direction = 'in' then tx_hash end) as right_tx,
        bool_or(source is not NULL and source = '0:0000000000000000000000000000000000000000000000000000000000000000' 
            and source = '-1:0000000000000000000000000000000000000000000000000000000000000000') 
        and max(case when direction = 'out' then tx_hash end) is null as incomplete,
        bool_or(destination is not NULL) and max(case when direction = 'in' then tx_hash end) is null as broken
    from messages
    where trace_id = new_trace_id
    group by trace_id, msg_hash;

    select count(*), sum((incomplete)::int) into new_trace_edges, new_trace_pending_edges
        from trace_edges where trace_id = new_trace_id;

    update traces set
        external_hash=new_trace_external_hash,
        mc_seqno_start=new_trace_start_seqno,
        mc_seqno_end=new_trace_end_seqno,
        start_lt=new_trace_start_lt,
        start_utime=new_trace_start_utime,
        end_lt=new_trace_end_lt,
        end_utime=new_trace_end_utime,
        state=(case when new_trace_pending_edges = 0 then 'complete' else 'pending' end)::trace_state,
        pending_edges_=new_trace_pending_edges,
        edges_=new_trace_edges,
        nodes_=new_trace_nodes,
        classification_state='unclassified'
    where trace_id = new_trace_id;
    return root_tx_hash;
end
$$;
`

func getCountRecords(pg_dsn string, sql string) int {
	var total int

	ctx := context.Background()
	db, err := pgx.Connect(ctx, pg_dsn)
	if err != nil {
		log.Fatalf("failed to open database connection: %s", err.Error())
	}
	defer db.Close(ctx)

	err = db.QueryRow(ctx, sql).Scan(&total)
	if err != nil {
		log.Fatalf("failed to read count: %s", err.Error())
	}
	return total
}

var pbar *progressbar.ProgressBar
var gate *semaphore.Weighted

func ProcessBatch(pg_dsn string, values []string) {
	defer gate.Release(1)

	ctx := context.Background()
	db, err := pgx.Connect(ctx, pg_dsn)
	if err != nil {
		log.Fatalf("failed to open database connection: %s", err.Error())
	}
	defer db.Close(ctx)

	sql := `with tx_hashes as (select rebuild_trace(tx_hash) as hash from (values %s) as T(tx_hash))
	delete from broken_traces_roots where tx_hash in (select hash from tx_hashes);`

	values_str := ``
	for _, v := range values {
		if len(values_str) > 0 {
			values_str += `,`
		}
		values_str += fmt.Sprintf("('%s')", v)
	}
	sql = fmt.Sprintf(sql, values_str)

	ct, err := db.Exec(ctx, sql)
	if err != nil {
		log.Fatalf("failed to process batch: %s", err.Error())
	}
	pbar.Add(int(ct.RowsAffected()))
	pbar.Add(len(values))
	db.Close(ctx)
}

func main() {
	var pg_dsn string
	var batch_size int
	var processes int
	var total int
	flag.StringVar(&pg_dsn, "pg", "postgresql://localhost:5432", "PostgreSQL connection string")
	flag.IntVar(&batch_size, "batch", 100, "Size of batch")
	flag.IntVar(&processes, "processes", 32, "Set number of parallel queries")
	flag.IntVar(&total, "total", 0, "Total rows")
	flag.Parse()

	gate = semaphore.NewWeighted(int64(processes))

	is_finished := false

	for !is_finished {
		total = getCountRecords(pg_dsn, `select count(*) from broken_traces_roots where not ignore;`)
		pbar = progressbar.NewOptions(total, progressbar.OptionFullWidth(), progressbar.OptionShowCount(), progressbar.OptionShowIts())

		log.Printf("rebuilding %d traces...", total)
		ctx := context.Background()
		db, err := pgx.Connect(ctx, pg_dsn)
		if err != nil {
			log.Fatalf("failed to open database connection: %s", err.Error())
		}
		defer db.Close(ctx)

		_, err = db.Exec(ctx, prepare_sql)
		if err != nil {
			log.Fatalf("failed to prepare database: %s", err.Error())
		}

		rows, err := db.Query(ctx, `select tx_hash from broken_traces_roots where not ignore;`)
		if err != nil {
			log.Fatalf("failed to read batch: %s", err.Error())
		}

		batch := []string{}
		for rows.Next() {
			var tx_hash string
			if err = rows.Scan(&tx_hash); err != nil {
				log.Fatalf("failed to read row: %s", err.Error())
			}
			batch = append(batch, tx_hash)

			if len(batch) >= batch_size {
				err = gate.Acquire(context.Background(), 1)
				if err != nil {
					log.Fatalf("failed to acquire worker: %s", err.Error())
				}

				go ProcessBatch(pg_dsn, batch)
				batch = []string{}
			}
		}
		if rows.Err() != nil {
			log.Fatalf("failed to finish reading rows: %s", err.Error())
		}
		if len(batch) > 0 {
			err = gate.Acquire(context.Background(), 1)
			if err != nil {
				log.Fatalf("failed to acquire worker: %s", err.Error())
			}

			go ProcessBatch(pg_dsn, batch)
		}
		db.Close(ctx)
		time.Sleep(time.Second)
	}
}
