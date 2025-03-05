# PGTON extension

This extension adds two custom types in PostgreSQL:
* tonhash: acts like a base64 string of size 44 bytes, but is stored in 32 bytes.
* tonaddr: acts like a TON address in raw-form string of size 66-67 bytes, but stored in 36 bytes.

Note: the functionality may be extended later.

## Build extension

Run the following commands on machine with PostgreSQL:
* Install dependencies: `sudo apt update && sudo apt install postgresql-server-dev-all cmake`.
* Create build directory: `mkdir build && cd build`.
* Configure and build: `cmake -DCMAKE_BUILD_TYPE=Release .. && make -j32 pgton`.
* Install binaries: `cd pgton && sudo make install`.
* Drop existing database and start TON Index worker with flag `--custom-types` to create a scheme with custom types.
