#pragma once
#include <memory>
#include <string>
#include "td/utils/Status.h"
#include "FirehoseBlock.h"

// Receives blocks strictly in masterchain seqno order.
class FirehoseOutput {
 public:
  virtual ~FirehoseOutput() = default;
  virtual td::Status write(const FirehoseBlock& block) = 0;
};

// The Firehose console protocol on stdout, read by firecore's reader-node.
std::unique_ptr<FirehoseOutput> make_fire_protocol_output();

// Merged-blocks bundles of 100 blocks in a directory, the layout firecore reads
// with --common-merged-blocks-store-url=file://<dir>. Only whole bundles are written.
std::unique_ptr<FirehoseOutput> make_merged_blocks_output(std::string dir);

// One <mc_seqno>.json file per block, to compare blocks with API v3.
std::unique_ptr<FirehoseOutput> make_json_output(std::string dir);
