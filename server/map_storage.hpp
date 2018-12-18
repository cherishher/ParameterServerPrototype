#pragma once

#include "base/message.hpp"
#include "server/abstract_storage.hpp"
#include "hdfs/hdfs.h"
#include <fstream>
#include<string>

#include "glog/logging.h"

#include <map>

namespace csci5570 {

template <typename Val>
class MapStorage : public AbstractStorage {
 public:
  MapStorage() = default;

  virtual void SubAdd(const third_party::SArray<Key>& typed_keys, const third_party::SArray<char>& vals) override {
    auto typed_vals = third_party::SArray<Val>(vals);
    CHECK_EQ(typed_keys.size(), typed_vals.size());
    // TODO
    // what if there are some same keys.
    for (int i = 0; i < typed_keys.size(); i++) {
      storage_[typed_keys[i]] = typed_vals[i];
    }
  }

  virtual third_party::SArray<char> SubGet(const third_party::SArray<Key>& typed_keys) override {
    third_party::SArray<Val> reply_vals(typed_keys.size());
    // TODO
    for (int i = 0; i < typed_keys.size(); i++) {
      // what if there is some keys with null value;
      reply_vals[i] = storage_.find(typed_keys[i])->second;
    }
    // TODO done.
    return third_party::SArray<char>(reply_vals);
  }

  virtual void Backup(int model_id) {
    std::ofstream outfile("/data/model.dat");
    outfile << model_id << "\n";
    for (auto iter = storage_.begin(); iter != storage_.end(); ++iter) {
      std::ostringstream ss;
      ss << iter->second;
      auto value = ss.str();
      outfile << iter->first << " " << value << "\n";
    }
    outfile.close();
  }

  // virtual void Backup(int model_id) {
  //   auto hdfs = hdfsWrite("hdfs:///127.0.0.1");
  //   hdfs
  // }

  virtual void FinishIter() override {}

 private:
  std::map<Key, Val> storage_;
};

}  // namespace csci5570
