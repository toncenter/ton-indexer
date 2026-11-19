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
The cache recognizes only the exact audited selector and method/CALLREF graph;
other selectors and getter closures bypass it. Its parsed-result key still includes
the full code hash, partitioning otherwise identical getters across code versions.

`elector-code-2020.b64` was extracted read-only from mainnet NodeDB at MC 3400000
on 2026-11-07. Its representation hash is
`ru0Xh4xjMGnjqPUkty8uJC1IoKWFCo0fbEUI/o/EgVc=`. The selector matches the above
pattern. Method 86698's entire reachable cell graph is byte-identical to that in
the first fixture: its representation hash is
`4YQSc9XqovH2ggyt2+7Gd6h6ecwftLK8/2vXIdL/RHA=` (528 bits and one direct reference).
Together with the exact 80-bit/one-reference dispatcher check, that transfers the
audited getter's c4-only dependency proof to versions with changes in unrelated
methods. It does not generalize to arbitrary dispatchers or older list-only ABIs.
Tests assert these identities and run the same cache/uncached-VM parity cases
on both real bytecodes.
