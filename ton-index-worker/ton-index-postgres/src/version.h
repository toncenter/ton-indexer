#include <tuple>
#include <string>

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

  [[nodiscard]] std::string str() const {
    return std::to_string(major) + "." + std::to_string(minor) + "." + std::to_string(patch);
  }
};

// Defines the latest version of the database schema.
constexpr Version latest_version{1, 3, 0};
