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
      	for(int i = 0; i < server_thread_ids.size(); i++){
      		rangesAndThreadId.push_back(std::pair<third_party::Range,uint32_t>(ranges[i],server_thread_ids[i]));
      	}
      }

  void Slice(const Keys& keys, std::vector<std::pair<int, Keys>>* sliced) const override {

  	bool existTag = false;
  	int sliceid;
  	int threadid;

  	for(int i = 0; i < keys.size(); i++){

  		existTag = false;

  		// find thread id
  		for(int i1 = 0; i1 < rangesAndThreadId.size(); i1++){
	  		if(keys[i]>= rangesAndThreadId[i1].first.begin() and keys[i] <rangesAndThreadId[i1].first.end()){
	  			threadid = rangesAndThreadId[i1].second;
	  		}
  		}

  		// check if exist in sliced
  		for(int i2 = 0; i2 < sliced->size(); i2++){
  			if(threadid == (*sliced)[i2].first){
  				existTag = true;
  				sliceid = i2;
				break;
  			}
  		}

  		// insert values to slice
  		if(!existTag){
  			// if do not find it in slice
  			Keys tempkey;
  			tempkey.push_back(keys[i]);
  			std::pair<int, Keys> temppair(threadid, tempkey);
  			sliced->push_back(temppair);
  		} else {
  			// if the server already in slice
  			(*sliced)[sliceid].second.push_back(keys[i]);
  		}
  	}

  }

  void Slice(const KVPairs& kvs, std::vector<std::pair<int, KVPairs>>* sliced) const override {
  	bool existTag = false;
  	int sliceid;
  	int threadid;

  	int totalSize = kvs.first.size();
  	for(int i = 0; i < totalSize; i++){

  		existTag = false;

  		// find thread id
  		for(int i1 = 0; i1 < rangesAndThreadId.size(); i1++){
	  		if(kvs.first[i]>= rangesAndThreadId[i1].first.begin() and kvs.first[i] <rangesAndThreadId[i1].first.end()){
	  			threadid = rangesAndThreadId[i1].second;
	  		}
  		}

  		// check if exist in sliced
  		for(int i2 = 0; i2 < sliced->size(); i2++){
  			if(threadid == (*sliced)[i2].first){
  				existTag = true;
  				sliceid = i2;
				break;
  			}
  		}

  		// insert values to slice
  		if(!existTag){
  			// if do not find it in slice
  			Keys tempkey;
  			tempkey.push_back(kvs.first[i]);
  			third_party::SArray<double> tempvalue;
  			tempvalue.push_back(kvs.second[i]);

  			std::pair<int, KVPairs> temppair(threadid, KVPairs(tempkey,tempvalue));
  			sliced->push_back(temppair);
  		} else {
  			// if the server already in slice
  			(*sliced)[sliceid].second.first.push_back(kvs.first[i]);
  			(*sliced)[sliceid].second.second.push_back(kvs.second[i]);
  		}
  	}
  }

  std::vector<third_party::Range> GetRanges(){
	  return ranges_;
  }

 private:
  std::vector<third_party::Range> ranges_;
  // std::map<uint32_t, <third_party::Range>> rangesMap;
  std::vector<std::pair<third_party::Range,uint32_t>> rangesAndThreadId;
};

}  // namespace csci5570
