# Backcompat API Tests

This suite compares a `reference` deployment and a `devel` deployment of the TON Index API by:

- loading the Swagger contract from [https://toncenter.com/api/v3/doc.json](https://toncenter.com/api/v3/doc.json),
- generating stable read-only requests from live indexed data where required parameters are needed,
- sending the same request to both environments concurrently,
- asserting that status codes and JSON payloads match exactly.

## What runs by default

The default catalog focuses on read-only endpoints and avoids the most volatile or side-effecting operations. It includes generated coverage for:

- blocks, transactions, traces, actions, messages
- account state, metadata, address book, wallet state, wallet information
- decode GET and POST
- DNS, jetton, NFT, multisig, vesting endpoints when the reference environment has seed data available

If the reference environment does not currently expose the necessary seed data for a case, that case is skipped instead of failing.

## What is intentionally not in the default catalog

- `GET /api/v3/pendingActions`
- `GET /api/v3/pendingTraces`
- `GET /api/v3/pendingTransactions`
- `POST /api/v3/message`
- payload-heavy endpoints that need environment-specific bodies such as `POST /api/v3/estimateFee` and `POST /api/v3/runGetMethod`

Those can still be covered with a custom JSON case file.

## Running

```bash
BACKCOMPAT_REFERENCE_BASE_URL=https://reference.example.com \
BACKCOMPAT_DEVEL_BASE_URL=https://devel.example.com \
BACKCOMPAT_API_KEY=your-key \
.venv/bin/pytest tests/backcompat -q
```

`BACKCOMPAT_REFERENCE_BASE_URL` and `BACKCOMPAT_DEVEL_BASE_URL` may point either to the host root or directly to `/api/v3`.

To write ad hoc checks directly in Python, edit [test_methods.py](/Users/ruslixag/Developer/ton-indexer/tests/backcompat/test_methods.py). It already contains examples for fixed params, param chaining from another request, and parametrized multi-input tests.

## Options

- `--reference-base-url`
- `--devel-base-url`
- `--api-key`
- `--reference-api-key`
- `--devel-api-key`
- `--backcompat-swagger-url`
- `--backcompat-timeout`
- `--backcompat-custom-cases`
- `--backcompat-allow-unsafe`
- `--backcompat-no-verify-tls`

Equivalent environment variables are supported:

- `BACKCOMPAT_REFERENCE_BASE_URL`
- `BACKCOMPAT_DEVEL_BASE_URL`
- `BACKCOMPAT_API_KEY`
- `BACKCOMPAT_REFERENCE_API_KEY`
- `BACKCOMPAT_DEVEL_API_KEY`
- `BACKCOMPAT_SWAGGER_URL`
- `BACKCOMPAT_TIMEOUT`
- `BACKCOMPAT_CUSTOM_CASES`
- `BACKCOMPAT_ALLOW_UNSAFE`
- `BACKCOMPAT_NO_VERIFY_TLS`

## Custom cases

Point `--backcompat-custom-cases` at a JSON file containing an array of cases. Each case supports:

- `id`
- `description`
- `method`
- `path`
- `query`
- `body`
- `expected_status`
- `ignore_paths`
- `unsafe`

`ignore_paths` uses JSON Pointer syntax. Example: `/metadata/*/token_info`

See [custom_cases.example.json](/Users/ruslixag/Developer/ton-indexer/tests/backcompat/custom_cases.example.json) for the expected format.
