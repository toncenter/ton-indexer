#include "td/utils/logging.h"
#include <tuple>
#include <optional>
#include <pqxx/pqxx>


struct Version {
  int major;
  int minor;
  int patch;

  bool operator<(const Version &other) const {
    return std::tie(major, minor, patch) < std::tie(other.major, other.minor, other.patch);
  }

  bool operator==(const Version &other) const {
    return std::tie(major, minor, patch) == std::tie(other.major, other.minor, other.patch);
  }

  std::string str() const {
    auto res = PSLICE() << major << "." << minor << "." << patch;
    return res.str();
  }
};

// Defines the latest version of the database schema.
const Version latest_version{1, 2, 2};

std::optional<Version> get_current_db_version(const std::string& connection_string) {
  try {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);
    auto [major, minor, patch] = txn.query1<int, int, int>("SELECT major, minor, patch FROM ton_db_version LIMIT 1");
    Version version{major, minor, patch};
    LOG(INFO) << "Current database version: " << version.str();
    return version;
  } catch (const pqxx::undefined_table &e) {
    LOG(INFO) << "Database version table does not exist, assuming it is new database.";
    return std::nullopt;
  }
}