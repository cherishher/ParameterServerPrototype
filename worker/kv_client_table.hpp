#pragma once

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "base/abstract_partition_manager.hpp"
#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"
#include "worker/abstract_callback_runner.hpp"

#include <cinttypes>
#include <vector>
#include <ctime>


namespace csci5570 {
  
  /**
   * Provides the API to users, and implements the worker-side abstraction of model
   * Each model in one application is uniquely handled by one KVClientTable
   *
   * @param Val type of model parameter values
   */
  template <typename Val>
  class KVClientTable {
  public:
    /**
     * @param app_thread_id       user thread id
     * @param model_id            model id
     * @param sender_queue        the work queue of a sender communication thread
     * @param partition_manager   model partition manager
     * @param callback_runner     callback runner to handle received replies from servers
     */
    KVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const sender_queue,
                  const AbstractPartitionManager* const partition_manager, AbstractCallbackRunner* const callback_runner)
    : app_thread_id_(app_thread_id),
    model_id_(model_id),
    sender_queue_(sender_queue),
    partition_manager_(partition_manager),
    callback_runner_(callback_runner){};
    
    // ========== API ========== //
    void Clock() {
      Message msg;
      msg.meta.flag = Flag::kClock;
      msg.meta.model_id = model_id_;
      msg.meta.sender = app_thread_id_;
      auto sids = partition_manager_->GetServerThreadIds();
      for (auto sid : sids) {
        msg.meta.recver = sid;
        sender_queue_->Push(msg);
      }
    }
    // vector version
    void Add(const std::vector<Key>& keys, const std::vector<Val>& vals) {
      std::vector<std::pair<int, AbstractPartitionManager::KVPairs>> sliced;
      third_party::SArray<Key> tmp1;
      for (int i = 0; i< keys.size(); i++) {
        tmp1.push_back(keys[i]);
      }
      third_party::SArray<Val> tmp2;
      for (int i = 0; i< vals.size(); i++) {
        tmp2.push_back(vals[i]);
      }
      partition_manager_->Slice(std::make_pair(tmp1, tmp2), &sliced);
      std::map<int,int> indicator_; //cash if we receive the acknownledgement or not
      std::map<int,int> tracker_;
      for (int i = 0; i < sliced.size(); i++) {
        indicator_[sliced[i].first] = 0;
        tracker_[sliced[i].first] = 0;
      }
      time_t start_time = time(NULL);
      time_t last_round_time = start_time;
      for (int i = 0; i < sliced.size(); i++) {
        if(indicator_[sliced[i].first] == 1){
          continue;
        }
        Message msg;
        msg.meta.sender = app_thread_id_;
        msg.meta.recver = sliced[i].first;
        msg.meta.model_id = model_id_;
        msg.meta.flag = Flag::kAdd;
        msg.meta.timestamp = start_time;
        third_party::SArray<Key> keys(sliced[i].second.first);
        third_party::SArray<Val> vals(sliced[i].second.second);
        msg.AddData(keys);
        msg.AddData(vals);
        sender_queue_->Push(msg);
      }
      callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_, [indicator_](Message& msg)mutable{
        auto it = indicator_.find(msg.meta.sender);
        if (it != indicator_.end())
          it->second = 1;
      });
      callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_, [](){
        return;
      });
      callback_runner_->NewRequest(app_thread_id_, model_id_, tracker_);
      callback_runner_->WaitRequest(app_thread_id_, model_id_, [this, sliced, indicator_, start_time, last_round_time]()mutable{
        time_t current_time = time(NULL);
        //not expire, return.
        if (current_time - last_round_time < ttl_) {
          return;
        }
        //expire, resend what we not get ack.
        last_round_time = current_time;
        for (int i = 0; i < sliced.size(); i++) {
          if(indicator_[sliced[i].first] == 1){
            continue;
          }
          Message msg;
          msg.meta.sender = app_thread_id_;
          msg.meta.recver = sliced[i].first;
          msg.meta.model_id = model_id_;
          msg.meta.flag = Flag::kAdd;
          msg.meta.timestamp = start_time;
          third_party::SArray<Key> keys(sliced[i].second.first);
          third_party::SArray<Val> vals(sliced[i].second.second);
          msg.AddData(keys);
          msg.AddData(vals);
          sender_queue_->Push(msg);
        }
      });
    }
    void Get(const std::vector<Key>& keys, std::vector<Val>* vals) {
      std::vector<std::pair<int, AbstractPartitionManager::Keys>> sliced;
      third_party::SArray<Key> tmp;
      for (int i = 0; i< keys.size(); i++) {
        tmp.push_back(keys[i]);
      }
      partition_manager_->Slice(tmp, &sliced);
      std::map<int,int> indicator_; //cash if we receive the acknownledgement or not
      std::map<int,int> tracker_;
      for (int i = 0; i < sliced.size(); i++) {
        indicator_[sliced[i].first] = 0;
        tracker_[sliced[i].first] = 0;
      }
      time_t start_time = time(NULL);
      time_t last_round_time = start_time;
      for (int i = 0; i < sliced.size(); i++) {
        if(indicator_[sliced[i].first] == 1){
          continue;
        }
        Message msg;
        msg.meta.sender = app_thread_id_;
        msg.meta.recver = sliced[i].first;
        msg.meta.model_id = model_id_;
        msg.meta.flag = Flag::kGet;
        msg.meta.timestamp = start_time;
        third_party::SArray<Key> keys(sliced[i].second);
        msg.AddData(keys);
        sender_queue_->Push(msg);
      }
      callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_, [vals, indicator_](Message& msg)mutable{
        auto it = indicator_.find(msg.meta.sender);
        if (it != indicator_.end()){
          if(it->second == 0){
            third_party::SArray<Val> tmp(msg.data[1]);
            for (int i = 0; i< tmp.size(); i++) {
              vals->push_back(tmp[i]);
            }
          }
          it->second = 1;
        }
      });
      callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_, [](){
        return;
      });
      callback_runner_->NewRequest(app_thread_id_, model_id_, tracker_);
      callback_runner_->WaitRequest(app_thread_id_, model_id_, [this, sliced, indicator_, start_time, last_round_time]()mutable{
        time_t current_time = time(NULL);
        //not expire, return.
        if (current_time - last_round_time < ttl_) {
          return;
        }
        //expire, resend what we not get ack, update last round time.
        last_round_time = current_time;
        for (int i = 0; i < sliced.size(); i++) {
          if(indicator_[sliced[i].first] == 1){
            continue;
          }
          Message msg;
          msg.meta.sender = app_thread_id_;
          msg.meta.recver = sliced[i].first;
          msg.meta.model_id = model_id_;
          msg.meta.flag = Flag::kGet;
          msg.meta.timestamp = start_time;
          third_party::SArray<Key> keys(sliced[i].second);
          msg.AddData(keys);
          sender_queue_->Push(msg);
        }
      });
    }
    // sarray version
    void Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) {
      std::vector<std::pair<int, AbstractPartitionManager::KVPairs>> sliced;
      partition_manager_->Slice(std::make_pair(keys, vals), &sliced);
      std::map<int,int> indicator_; //cash if we receive the acknownledgement or not
      std::map<int,int> tracker_;
      for (int i = 0; i < sliced.size(); i++) {
        indicator_[sliced[i].first] = 0;
        tracker_[sliced[i].first] = 0;
      }
      time_t start_time = time(NULL);
      time_t last_round_time = start_time;
      for (int i = 0; i < sliced.size(); i++) {
        if(indicator_[sliced[i].first] == 1){
          continue;
        }
        Message msg;
        msg.meta.sender = app_thread_id_;
        msg.meta.recver = sliced[i].first;
        msg.meta.model_id = model_id_;
        msg.meta.flag = Flag::kAdd;
        msg.meta.timestamp = start_time;
        third_party::SArray<Key> keys(sliced[i].second.first);
        third_party::SArray<Val> vals(sliced[i].second.second);
        msg.AddData(keys);
        msg.AddData(vals);
        sender_queue_->Push(msg);
      }
      callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_, [indicator_](Message& msg)mutable{
        auto it = indicator_.find(msg.meta.sender);
        if (it != indicator_.end())
          it->second = 1;
      });
      callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_, [](){
        return;
      });
      callback_runner_->NewRequest(app_thread_id_, model_id_, tracker_);
      callback_runner_->WaitRequest(app_thread_id_, model_id_, [this, sliced, indicator_, start_time, last_round_time]()mutable{
        time_t current_time = time(NULL);
        //not expire, return.
        if (current_time - last_round_time < ttl_) {
          return;
        }
        //expire, resend what we not get ack, update last round time.
        last_round_time = current_time;
        for (int i = 0; i < sliced.size(); i++) {
          if(indicator_[sliced[i].first] == 1){
            continue;
          }
          Message msg;
          msg.meta.sender = app_thread_id_;
          msg.meta.recver = sliced[i].first;
          msg.meta.model_id = model_id_;
          msg.meta.flag = Flag::kAdd;
          msg.meta.timestamp = start_time;
          third_party::SArray<Key> keys(sliced[i].second.first);
          third_party::SArray<Val> vals(sliced[i].second.second);
          msg.AddData(keys);
          msg.AddData(vals);
          sender_queue_->Push(msg);
        }
      });
    }
    void Get(const third_party::SArray<Key>& keys, third_party::SArray<Val>* vals) {
      std::vector<std::pair<int, AbstractPartitionManager::Keys>> sliced;
      partition_manager_->Slice(keys, &sliced);
      std::map<int,int> indicator_; //cash if we receive the acknownledgement or not
      std::map<int,int> tracker_;
      for (int i = 0; i < sliced.size(); i++) {
        indicator_[sliced[i].first] = 0;
        tracker_[sliced[i].first] = 0;
      }
      time_t start_time = time(NULL);
      time_t last_round_time = start_time;
      for (int i = 0; i < sliced.size(); i++) {
        if(indicator_[sliced[i].first] == 1){
          continue;
        }
        Message msg;
        msg.meta.sender = app_thread_id_;
        msg.meta.recver = sliced[i].first;
        msg.meta.model_id = model_id_;
        msg.meta.flag = Flag::kGet;
        msg.meta.timestamp = start_time;
        third_party::SArray<Key> keys(sliced[i].second);
        msg.AddData(keys);
        sender_queue_->Push(msg);
      }
      callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_, [vals, indicator_](Message& msg)mutable{
        auto it = indicator_.find(msg.meta.sender);
        if (it != indicator_.end()){
          if(it->second == 0){
            third_party::SArray<Val> tmp(msg.data[1]);
            for (int i = 0; i< tmp.size(); i++) {
              vals->push_back(tmp[i]);
            }
          }
          it->second = 1;
        }
      });
      callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_, [](){
        return;
      });
      callback_runner_->NewRequest(app_thread_id_, model_id_, tracker_);
      callback_runner_->WaitRequest(app_thread_id_, model_id_, [this, sliced, indicator_, start_time, last_round_time]()mutable{
        time_t current_time = time(NULL);
        //not expire, return.
        if (current_time - last_round_time < ttl_) {
          return;
        }
        //expire, resend what we not get ack, update last round time.
        last_round_time = current_time;
        for (int i = 0; i < sliced.size(); i++) {
          if(indicator_[sliced[i].first] == 1){
            continue;
          }
          Message msg;
          msg.meta.sender = app_thread_id_;
          msg.meta.recver = sliced[i].first;
          msg.meta.model_id = model_id_;
          msg.meta.flag = Flag::kGet;
          msg.meta.timestamp = start_time;
          third_party::SArray<Key> keys(sliced[i].second);
          msg.AddData(keys);
          sender_queue_->Push(msg);
        }
      });
    }
    // ========== API ========== //
    
  private:
    uint32_t app_thread_id_;  // identifies the user thread
    uint32_t model_id_;       // identifies the model on servers
    uint32_t sequence_number_ = 0;  //sequence number for add request
    double ttl_  = 10; //time to live
    
    ThreadsafeQueue<Message>* const sender_queue_;             // not owned
    AbstractCallbackRunner* const callback_runner_;            // not owned
    const AbstractPartitionManager* const partition_manager_;  // not owned
    
  };  // class KVClientTable
  
}  // namespace csci5570
