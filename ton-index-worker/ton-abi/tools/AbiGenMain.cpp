// ton-abi-gen: CLI front end for the ton-abi emitter. Loads one or
// more Tolk ABI JSON files, resolves each via AbiKernel, and emits the committed
// C++ struct pair generated/<contract_snake>_gen.{h,cpp}.
//
//   ton-abi-gen <abi.json>... --out-dir <dir>   write the pairs
//   ton-abi-gen <abi.json>... --out-dir <dir> --check
//                                               regen to memory + byte-compare
//                                               the committed files; exit != 0
//                                               on drift or absence.
//
// Errors to stderr, non-zero exit. No aborts (fail-closed td::Status).

#include "AbiEmit.h"
#include "AbiKernel.h"
#include "AbiLoader.h"

#include "td/utils/Status.h"

#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace {

td::Result<std::string> read_file(const std::string &path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return td::Status::Error("cannot open " + path);
  }
  std::stringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

bool write_file(const std::string &path, const std::string &content) {
  std::ofstream out(path, std::ios::binary);
  if (!out) {
    return false;
  }
  out << content;
  return static_cast<bool>(out);
}

int fail(const std::string &msg) {
  std::fprintf(stderr, "ton-abi-gen: %s\n", msg.c_str());
  return 1;
}

// The fixture stem: input path basename minus a trailing ".abi.json" / ".json".
// Used for the output filename + namespace so distinct fixtures that share a
// contract_name (both err-cont-on-stack-* are "Err") don't collide.
std::string fixture_stem(const std::string &path) {
  std::string base = path;
  auto slash = base.find_last_of("/\\");
  if (slash != std::string::npos) {
    base = base.substr(slash + 1);
  }
  for (const char *suf : {".abi.json", ".json"}) {
    std::string s = suf;
    if (base.size() > s.size() && base.compare(base.size() - s.size(), s.size(), s) == 0) {
      base = base.substr(0, base.size() - s.size());
      break;
    }
  }
  return base;
}

td::Result<ton_abi::GeneratedFiles> gen_one(const std::string &abi_path) {
  TRY_RESULT(json, read_file(abi_path));
  TRY_RESULT(abi, ton_abi::load_abi_from_json(json));
  // AbiKernel holds a non-owning pointer into `abi`; keep both alive by moving
  // abi into a heap slot the kernel + caller share via the returned pair. Here
  // we resolve + emit fully before `abi` dies, so a stack local is fine.
  auto abi_box = std::make_unique<ton_abi::ContractABI>(std::move(abi));
  TRY_RESULT(kernel, ton_abi::AbiKernel::create(*abi_box));
  TRY_RESULT(files, ton_abi::emit_abi(*abi_box, kernel, fixture_stem(abi_path)));
  return files;
}

}  // namespace

int main(int argc, char **argv) {
  std::vector<std::string> inputs;
  std::string out_dir;
  bool check = false;

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--out-dir") {
      if (++i >= argc) return fail("--out-dir needs an argument");
      out_dir = argv[i];
    } else if (a == "--check") {
      check = true;
    } else {
      inputs.push_back(a);
    }
  }
  if (inputs.empty()) {
    return fail("usage: ton-abi-gen <abi.json>... --out-dir <dir> [--check]");
  }
  if (out_dir.empty()) {
    return fail("--out-dir is required");
  }

  int drift = 0;
  for (const auto &path : inputs) {
    auto r = gen_one(path);
    if (r.is_error()) {
      return fail(path + ": " + r.error().message().str());
    }
    auto files = r.move_as_ok();
    std::string h_path = out_dir + "/" + files.contract_snake + "_gen.h";
    std::string c_path = out_dir + "/" + files.contract_snake + "_gen.cpp";

    if (check) {
      auto rh = read_file(h_path);
      auto rc = read_file(c_path);
      if (rh.is_error() || rc.is_error()) {
        std::fprintf(stderr, "ton-abi-gen --check: missing generated file for %s\n", files.contract_snake.c_str());
        drift = 1;
        continue;
      }
      if (rh.move_as_ok() != files.header || rc.move_as_ok() != files.source) {
        std::fprintf(stderr, "ton-abi-gen --check: DRIFT in %s_gen.{h,cpp}\n", files.contract_snake.c_str());
        drift = 1;
      }
    } else {
      if (!write_file(h_path, files.header) || !write_file(c_path, files.source)) {
        return fail("cannot write output for " + files.contract_snake);
      }
      std::fprintf(stderr, "ton-abi-gen: wrote %s_gen.{h,cpp}\n", files.contract_snake.c_str());
    }
  }
  return drift;
}
