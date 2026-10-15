#include "FirehoseOutput.h"

#include <cstdio>
#include <zstd.h>
#include "td/utils/base64.h"
#include "td/utils/filesystem.h"
#include "td/utils/format.h"
#include "td/utils/misc.h"
#include "td/utils/port/path.h"
#include "sf/bstream/v1/bstream.pb.h"

namespace {

const std::string kPayloadTypeUrl = std::string("type.googleapis.com/") + kFirehoseBlockType;
constexpr std::uint64_t kBundleSize = 100;

// Writes through a temporary file, so readers never see a partial file.
td::Status write_file_atomically(const std::string& path, td::Slice data) {
  auto tmp_path = path + ".tmp";
  TRY_STATUS(td::write_file(tmp_path, data));
  return td::rename(tmp_path, path);
}

class FireProtocolOutput : public FirehoseOutput {
 public:
  FireProtocolOutput() {
    std::string init = std::string("FIRE INIT 3.0 ") + kFirehoseBlockType + "\n";
    std::fwrite(init.data(), 1, init.size(), stdout);
    std::fflush(stdout);
  }

  td::Status write(const FirehoseBlock& block) override {
    // FIRE BLOCK <num> <id> <parent_num> <parent_id> <lib_num> <timestamp_ns> <payload_base64>
    // Plain concatenation: PSTRING truncates long strings to its fixed buffer.
    std::string line = "FIRE BLOCK " + std::to_string(block.number) + " " + block.id + " " +
                       std::to_string(block.parent_num) + " " + block.parent_id + " " + std::to_string(block.number) +
                       " " + std::to_string(static_cast<std::uint64_t>(block.gen_utime) * 1000000000ULL) + " " +
                       td::base64_encode(block.payload) + "\n";
    if (std::fwrite(line.data(), 1, line.size(), stdout) != line.size() || std::fflush(stdout) != 0) {
      return td::Status::Error("failed to write to stdout");
    }
    return td::Status::OK();
  }
};

// A merged-blocks file is a zstd-compressed dbin stream: the "dbin" magic,
// format version 1, the payload type, then length-prefixed sf.bstream.v1.Block.
class MergedBlocksOutput : public FirehoseOutput {
 public:
  explicit MergedBlocksOutput(std::string dir) : dir_(std::move(dir)) {
  }

  ~MergedBlocksOutput() override {
    if (has_bundle_) {
      LOG(WARNING) << "Dropping incomplete merged-blocks bundle " << bundle_base_;
    }
  }

  td::Status write(const FirehoseBlock& block) override {
    if (!has_bundle_) {
      start_bundle(block.number - block.number % kBundleSize);
    } else if (block.number != last_number_ + 1) {
      return td::Status::Error(PSLICE() << "block " << block.number << " does not follow " << last_number_);
    }
    append_block(block);
    last_number_ = block.number;
    if ((block.number + 1) % kBundleSize == 0) {
      return flush_bundle();
    }
    return td::Status::OK();
  }

 private:
  std::string dir_;
  bool has_bundle_{false};
  std::uint64_t bundle_base_{0};
  std::uint64_t last_number_{0};
  std::string data_;

  void start_bundle(std::uint64_t base) {
    has_bundle_ = true;
    bundle_base_ = base;
    data_ = "dbin";
    data_.push_back(1);
    data_.push_back(static_cast<char>((kPayloadTypeUrl.size() >> 8) & 0xff));
    data_.push_back(static_cast<char>(kPayloadTypeUrl.size() & 0xff));
    data_ += kPayloadTypeUrl;
  }

  void append_block(const FirehoseBlock& block) {
    sf::bstream::v1::Block envelope;
    envelope.set_number(block.number);
    envelope.set_id(block.id);
    envelope.set_parent_id(block.parent_id);
    envelope.set_parent_num(block.parent_num);
    envelope.set_lib_num(block.number);
    envelope.mutable_timestamp()->set_seconds(block.gen_utime);
    envelope.mutable_payload()->set_type_url(kPayloadTypeUrl);
    envelope.mutable_payload()->set_value(block.payload);
    auto message = envelope.SerializeAsString();
    auto size = static_cast<std::uint32_t>(message.size());
    for (int shift = 24; shift >= 0; shift -= 8) {
      data_.push_back(static_cast<char>((size >> shift) & 0xff));
    }
    data_ += message;
  }

  td::Status flush_bundle() {
    std::string compressed(ZSTD_compressBound(data_.size()), '\0');
    auto size = ZSTD_compress(compressed.data(), compressed.size(), data_.data(), data_.size(), 3);
    if (ZSTD_isError(size)) {
      return td::Status::Error(PSLICE() << "zstd: " << ZSTD_getErrorName(size));
    }
    compressed.resize(size);
    auto path = PSTRING() << dir_ << "/" << td::lpad0(std::to_string(bundle_base_), 10) << ".dbin.zst";
    TRY_STATUS(write_file_atomically(path, compressed));
    has_bundle_ = false;
    data_.clear();
    return td::Status::OK();
  }
};

class JsonOutput : public FirehoseOutput {
 public:
  explicit JsonOutput(std::string dir) : dir_(std::move(dir)) {
  }

  td::Status write(const FirehoseBlock& block) override {
    TRY_RESULT(json, firehose_payload_to_json(block.payload));
    return write_file_atomically(PSTRING() << dir_ << "/" << block.number << ".json", json);
  }

 private:
  std::string dir_;
};

}  // namespace

std::unique_ptr<FirehoseOutput> make_fire_protocol_output() {
  return std::make_unique<FireProtocolOutput>();
}

std::unique_ptr<FirehoseOutput> make_merged_blocks_output(std::string dir) {
  return std::make_unique<MergedBlocksOutput>(std::move(dir));
}

std::unique_ptr<FirehoseOutput> make_json_output(std::string dir) {
  return std::make_unique<JsonOutput>(std::move(dir));
}
