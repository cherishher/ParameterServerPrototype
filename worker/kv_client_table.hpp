#pragma once

#include "base/abstract_partition_manager.hpp"
#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"
#include "worker/abstract_callback_runner.hpp"

#include <cinttypes>
#include <vector>

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
  void Clock();
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
      for (int i = 0; i < sliced.size(); i++) {
          Message msg;
          msg.meta.sender = app_thread_id_;
          msg.meta.recver = sliced[i].first;
          msg.meta.model_id = model_id_;
          msg.meta.flag = Flag::kAdd;
          third_party::SArray<Key> keys(sliced[i].second.first);
          third_party::SArray<float> vals(sliced[i].second.second);
          msg.AddData(keys);
          msg.AddData(vals);
          sender_queue_->Push(msg);
      }
  }
  void Get(const std::vector<Key>& keys, std::vector<Val>* vals) {
      std::vector<std::pair<int, AbstractPartitionManager::Keys>> sliced;
      third_party::SArray<Key> tmp;
      for (int i = 0; i< keys.size(); i++) {
          tmp.push_back(keys[i]);
      }
      partition_manager_->Slice(tmp, &sliced);
      for (int i = 0; i < sliced.size(); i++) {
          Message msg;
          msg.meta.sender = app_thread_id_;
          msg.meta.recver = sliced[i].first;
          msg.meta.model_id = model_id_;
          msg.meta.flag = Flag::kGet;
          third_party::SArray<Key> keys(sliced[i].second);
          msg.AddData(keys);
          sender_queue_->Push(msg);
      }
      callback_runner_->NewRequest(app_thread_id_, model_id_, sliced.size());
      callback_runner_->WaitRequest(app_thread_id_, model_id_);
  }
  // sarray version
  void Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) {
      std::vector<std::pair<int, AbstractPartitionManager::KVPairs>> sliced;
      partition_manager_->Slice(std::make_pair(keys, vals), &sliced);
      for (int i = 0; i < sliced.size(); i++) {
          Message msg;
          msg.meta.sender = app_thread_id_;
          msg.meta.recver = sliced[i].first;
          msg.meta.model_id = model_id_;
          msg.meta.flag = Flag::kAdd;
          third_party::SArray<Key> keys(sliced[i].second.first);
          third_party::SArray<float> vals(sliced[i].second.second);
          msg.AddData(keys);
          msg.AddData(vals);
          sender_queue_->Push(msg);
      }
  }
  void Get(const third_party::SArray<Key>& keys, third_party::SArray<Val>* vals) {
      std::vector<std::pair<int, AbstractPartitionManager::Keys>> sliced;
      partition_manager_->Slice(keys, &sliced);
      for (int i = 0; i < sliced.size(); i++) {
          Message msg;
          msg.meta.sender = app_thread_id_;
          msg.meta.recver = sliced[i].first;
          msg.meta.model_id = model_id_;
          msg.meta.flag = Flag::kGet;
          third_party::SArray<Key> keys(sliced[i].second);
          msg.AddData(keys);
          sender_queue_->Push(msg);
      }
      callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_, KVClientTable::Receive);
      callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_, KVClientTable::FinishReceive);
      callback_runner_->NewRequest(app_thread_id_, model_id_, sliced.size());
      callback_runner_->WaitRequest(app_thread_id_, model_id_);
      vals = value_cash_;
      value_cash_->clear();
  }
  // ========== API ========== //

 private:
  uint32_t app_thread_id_;  // identifies the user thread
  uint32_t model_id_;       // identifies the model on servers

  ThreadsafeQueue<Message>* const sender_queue_;             // not owned
  AbstractCallbackRunner* const callback_runner_;            // not owned
  const AbstractPartitionManager* const partition_manager_;  // not owned
  
  third_party::SArray<Key>* value_cash_;

  void *Receive(Message& msg){
    for (int i = 0; i < msg.data[1].size(); i++) {
      value_cash_->push_back(msg.data[1][i]);
    }
  }
  void *FinishReceive(){
      //TODO:Need to do anything?
  }

};  // class KVClientTable

}  // namespace csci5570
