#include "FirehoseOutput.h"

#include <cstdio>
#include <vector>
#include <zstd.h>
#include "td/actor/actor.h"
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

  void write(const FirehoseBlock& block, td::Promise<td::Unit> written) override {
    // FIRE BLOCK <num> <id> <parent_num> <parent_id> <lib_num> <timestamp_ns> <payload_base64>
    // Plain concatenation: PSTRING truncates long strings to its fixed buffer.
    std::string line = "FIRE BLOCK " + std::to_string(block.number) + " " + block.id + " " +
                       std::to_string(block.parent_num) + " " + block.parent_id + " " + std::to_string(block.number) +
                       " " + std::to_string(static_cast<std::uint64_t>(block.gen_utime) * 1000000000ULL) + " " +
                       td::base64_encode(block.payload) + "\n";
    if (std::fwrite(line.data(), 1, line.size(), stdout) != line.size() || std::fflush(stdout) != 0) {
      return written.set_error(td::Status::Error("failed to write to stdout"));
    }
    written.set_value(td::Unit());
  }
};

// Compresses and writes one merged-blocks bundle on whatever scheduler thread
// runs it: zstd over a bundle of heavy blocks takes long enough to hold up the
// actor that writes blocks in order. Its blocks are written when the file is.
class WriteBundle : public td::actor::Actor {
 public:
  WriteBundle(std::string path, std::string data, std::vector<td::Promise<td::Unit>> written)
      : path_(std::move(path)), data_(std::move(data)), written_(std::move(written)) {
  }

  void start_up() override {
    auto status = compress_and_write();
    for (auto& promise : written_) {
      if (status.is_ok()) {
        promise.set_value(td::Unit());
      } else {
        promise.set_error(status.clone());
      }
    }
    stop();
  }

 private:
  std::string path_;
  std::string data_;
  std::vector<td::Promise<td::Unit>> written_;

  td::Status compress_and_write() {
    std::string compressed(ZSTD_compressBound(data_.size()), '\0');
    auto size = ZSTD_compress(compressed.data(), compressed.size(), data_.data(), data_.size(), 3);
    if (ZSTD_isError(size)) {
      return td::Status::Error(PSLICE() << "zstd: " << ZSTD_getErrorName(size));
    }
    compressed.resize(size);
    return write_file_atomically(path_, compressed);
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

  void write(const FirehoseBlock& block, td::Promise<td::Unit> written) override {
    if (!has_bundle_) {
      start_bundle(block.number - block.number % kBundleSize);
    } else if (block.number != last_number_ + 1) {
      return written.set_error(
          td::Status::Error(PSLICE() << "block " << block.number << " does not follow " << last_number_));
    }
    append_block(block);
    written_.push_back(std::move(written));
    last_number_ = block.number;
    if ((block.number + 1) % kBundleSize == 0) {
      flush_bundle();
    }
  }

 private:
  std::string dir_;
  bool has_bundle_{false};
  std::uint64_t bundle_base_{0};
  std::uint64_t last_number_{0};
  std::string data_;
  std::vector<td::Promise<td::Unit>> written_;  // of the blocks in the bundle

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

  void flush_bundle() {
    auto path = PSTRING() << dir_ << "/" << td::lpad0(std::to_string(bundle_base_), 10) << ".dbin.zst";
    td::actor::create_actor<WriteBundle>("writebundle", std::move(path), std::move(data_), std::move(written_))
        .release();
    has_bundle_ = false;
    data_.clear();
    written_.clear();
  }
};

class JsonOutput : public FirehoseOutput {
 public:
  explicit JsonOutput(std::string dir) : dir_(std::move(dir)) {
  }

  void write(const FirehoseBlock& block, td::Promise<td::Unit> written) override {
    auto json = firehose_payload_to_json(block.payload);
    if (json.is_error()) {
      return written.set_error(json.move_as_error());
    }
    auto status = write_file_atomically(PSTRING() << dir_ << "/" << block.number << ".json", json.ok());
    if (status.is_error()) {
      return written.set_error(std::move(status));
    }
    written.set_value(td::Unit());
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
