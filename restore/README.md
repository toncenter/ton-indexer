# Resotre database from dump

* Make backup: 

        docker run --rm -it --network ton-indexer_internal -v `pwd`/dump:/output postgres:14 bash -c "pg_dump --format=custom -h postgres -U postgres --file=/output/index.db ton_index --verbose"
* Restore backup: 
        
        docker run --rm -it --network ton-indexer_internal -v `pwd`:/input postgres:14 bash -c "pg_restore --format=custom -h postgres -U postgres --clean --dbname=ton_index --no-owner --file=/input/index.db ton_index --verbose"