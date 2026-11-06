# Elector cache fixture

`elector-code.b64` is the deployed mainnet Elector code fetched from TON Center
v2 `getAddressInformation` for `-1:3333…3333` during the cache investigation.
Its representation hash is `Yhf4csmfr8uHDywRo2L1kzm+lQlfcNALnP8vbc1p090=`
(hex `6217f872c99fafcb870f2c11a362f59339be95095f70d00b9cff2f6dcd69d3dd`).
Tests verify this hash and run the real getter on synthetic storage; they need
no network or NodeDB. No live account data or keys are included.

Method 86698 (`participant_list_extended`) and its CALLREF body were disassembled
with TON's `Disasm.fif`. They read only c4's first dictionary reference
(`cur_elect`), unpack its scalar fields, and enumerate the members dictionary.
There are no time, address, config, random or library accesses in that closure.
The top-level dispatcher is SETCP0 / DICTPUSHCONST / DICTIGETJMPZ / THROWARG.
The cache recognizes this exact code hash; other Elector versions bypass it.
