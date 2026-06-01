# PGTON extension

This extension adds three custom types in PostgreSQL:
* tonhash: acts like a base64 string of size 44 bytes, accepts base64url and hex input, but is stored in 32 bytes.
* tonaddr: acts like a TON address in raw-form string. It accepts `addr_none`, `<workchain>:<64 hex>`, `ext$<length>:<hex data>`, and `var$<workchain>:<hex data>`. For non-nibble `addr_var` lengths, use TON's trailing `_` hex bitstring marker. Output hex is uppercase.
* tonbytes: acts like a generic base64 string, but is stored as variable-length raw bytes.

Note: the functionality may be extended later.

## Build extension

Run the following commands on machine with PostgreSQL:
* Install dependencies: `sudo apt update && sudo apt install postgresql-server-dev-all cmake`.
* Create build directory: `mkdir build && cd build`.
* Configure and build: `cmake -DPGTON=ON -DCMAKE_BUILD_TYPE=Release -GNinja .. && ninja pgton`.
* Install binaries: `cd build/ton-index-worker/pgton && sudo cmake --install .`.
* Drop existing database and start TON Index worker with flag `--custom-types` to create a scheme with custom types.
