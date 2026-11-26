#pragma once

#include <string>

namespace clickhouse {
class Client;
}

namespace dex_snapshots {

// Restore removal identities before starting interface detection; throws on a ClickHouse error.
void restore_pool_cache(clickhouse::Client& client, const std::string& database);

}
