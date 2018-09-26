#pragma once

#include <cinttypes>
#include <vector>

#include "base/abstract_partition_manager.hpp"
#include "base/magic.hpp"
#include "base/third_party/sarray.h"

#include "glog/logging.h"

namespace csci5570 {

class RangePartitionManager : public AbstractPartitionManager {
 public:
  RangePartitionManager(const std::vector<uint32_t>& server_thread_ids, const std::vector<third_party::Range>& ranges)
      : AbstractPartitionManager(server_thread_ids) {
    ranges_ = ranges;
    for (int i = 0; i < server_thread_ids.size(); i++) {
      sliceRange_.insert(std::make_pair(ranges_[i], std::move(server_thread_ids[i])));
    }
  }

  bool keyInRange(const Key& key, third_party::Range& range) const {
    return (key > range.begin() && key < range.end());
  }

  uint32_t keyToThreadId(const Key& key) const {
    for (int i = 0; i < ranges_.size(); i++) {
      if ((key > ranges_[i].begin() && key < ranges_[i].end())) {
        return sliceRange_.at(ranges_[i]);
      };
    }
  }

  void Slice(const Keys& keys, std::vector<std::pair<int, Keys>>* sliced) const override {
    for (int i = 0; i < keys.size(); i++) {
      bool flag = false;
      int thread_id = keyToThreadId(keys[i]);
      for (int j = 0; j < (*sliced).size(); j++) {
        if ((*sliced)[j].first == thread_id) {
          (*sliced)[j].second.push_back(keys[i]);
          flag = true;
        }
        if (!flag) {
          (*sliced)[j].first = thread_id;
          (*sliced)[j].second.push_back(keys[i]);
        }
      }
    }
  }

  void Slice(const KVPairs& kvs, std::vector<std::pair<int, KVPairs>>* sliced) const override {
    for (int i = 0; i < kvs.first.size(); i++) {
      sliced->at(i).first = server_thread_ids_[i];
      sliced->at(i).second.first = kvs.first.segment(ranges_[i].begin(), ranges_[i].end());
      sliced->at(i).second.second = kvs.second.segment(ranges_[i].begin(), ranges_[i].end());
    }
  }

 private:
  std::vector<third_party::Range> ranges_;
  std::map<third_party::Range, uint32_t> sliceRange_;
};

}  // namespace csci5570
