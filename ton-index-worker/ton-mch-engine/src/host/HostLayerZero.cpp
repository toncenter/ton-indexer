// LayerZero dst_oapp compare.
#include "host/HostImpls.h"

#include <cctype>
#include <string>
#include <vector>

namespace mch {

// Literal lowercase-hex string compare: one side is the 64-digit zero-padded
// raw hash, the other is the minimal hex digits. A hash with a leading zero
// byte deliberately does not match its padded form. Null on either side is false.
EvalResult layerzero_dst_oapp_matches(BuildEnv &, const std::vector<Value> &args) {
  if (args[0].is_null() || args[1].is_null()) {
    return rt_ok(Value::make_bool(false));
  }
  auto py_str_tail = [](const Value &v) -> std::string {
    std::string s;
    if (v.t == VType::Account) {
      s = v.addr_none ? "addr_none" : v.str;  // canonical UPPER; lowered below
    } else {
      s = v.str;
    }
    std::string out = s.size() > 2 ? s.substr(2) : std::string();
    for (char &c : out) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return out;
  };
  return rt_ok(Value::make_bool(py_str_tail(args[0]) == py_str_tail(args[1])));
}

}  // namespace mch
