#include "PoolCache.h"

#include <clickhouse/client.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

#include "smc-interfaces/DexPools.h"
#include "td/utils/logging.h"

namespace dex_snapshots {

void restore_pool_cache(clickhouse::Client& client, const std::string& database) {
  // pool_latest identifies the rows to read, but only pool_snapshots has the full pool identity.
  client.Select("SELECT pool, CAST(dex AS String), dex_version, CAST(pool_type AS String), router, asset0, asset1 FROM " +
                    database + ".pool_snapshots WHERE (pool, mc_seqno, math_version) IN "
                    "(SELECT pool, bitShiftRight(version, 16), bitAnd(version, 65535) FROM " +
                    database + ".pool_latest FINAL)",
                [](const clickhouse::Block& block) {
    for (std::size_t i = 0; i < block.GetRowCount(); ++i) {
      auto string_at = [&](std::size_t column) {
        return std::string(block[column]->AsStrict<clickhouse::ColumnString>()->At(i));
      };
      // A row with a bad address is skipped, so it cannot stop the indexer at each start.
      auto address = block::StdAddress::parse(string_at(0));
      const auto router = string_at(4);
      auto router_address = block::StdAddress::parse(router);
      if (address.is_error() || (!router.empty() && router_address.is_error())) {
        LOG(WARNING) << "Skipping pool row with a bad address: " << string_at(0) << " router " << router;
        continue;
      }
      schema::DexPoolSnapshot identity;
      identity.address = address.move_as_ok();
      identity.dex = string_at(1);
      identity.dex_version = block[2]->AsStrict<clickhouse::ColumnUInt8>()->At(i);
      identity.pool_type = string_at(3);
      if (!router.empty()) {
        identity.router = router_address.move_as_ok();
      }
      identity.asset0 = string_at(5);
      identity.asset1 = string_at(6);
      dex_pool_cache().restore_identity(std::move(identity));
    }
  });
}

}
