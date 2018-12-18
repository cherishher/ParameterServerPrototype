#pragma once

#include <functional>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "base/message.hpp"

namespace csci5570 {

class AbstractCallbackRunner {
 public:
  /**
   * Register callbacks for receiving a message
   */
  virtual void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id,
                                  const std::function<void(Message&)>& recv_handle) = 0;
  /**
   * Register callbacks for when all expected responses are received
   */
  virtual void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                        const std::function<void()>& recv_finish_handle) = 0;

  /**
   * Register a new request which expects to receive <expected_responses> responses
   */
  virtual void NewRequest(uint32_t app_thread_id, uint32_t model_id, std::map<int,int> indicator) = 0;

  /**
   * Return when the request is completed
   */
  virtual void WaitRequest(uint32_t app_thread_id, uint32_t model_id) = 0;

  /**
   * Used by the worker threads on receival of messages and to invoke callbacks
   */
  virtual void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) = 0;
};  // class AbstractCallbackRunner

class DefaultCallbackRunner: public AbstractCallbackRunner {
  public:
    DefaultCallbackRunner() {}
    void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id,
                                  const std::function<void(Message&)>& recv_handle) {
      recv_handle_map_[app_thread_id][model_id] = recv_handle;
    }
    void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                        const std::function<void()>& recv_finish_handle) {
      recv_finish_handle_map_[app_thread_id][model_id] = recv_finish_handle;
    }
    void NewRequest(uint32_t app_thread_id, uint32_t model_id, std::map<int,int> indicator) {
      trackers_[app_thread_id][model_id] = indicator;
    }
    void WaitRequest(uint32_t app_thread_id, uint32_t model_id) {
      std::unique_lock<std::mutex> lk(mu_);
      cond_.wait(lk, [this, app_thread_id, model_id] {
        auto &tracker = trackers_[app_thread_id][model_id];
        std::map<int, int>::iterator it;
        for (it = tracker.begin(); it != tracker.end(); it++)
        {
          if(it->second == 0){
            return false;
          }
        }
        return true;
      });
    }
    void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) {
      bool recv_finish = true;
      auto &tracker = trackers_[app_thread_id][model_id];
      recv_handle_map_[app_thread_id][model_id](msg);
      
      {
        std::lock_guard<std::mutex> lk(mu_);
        auto it1 = tracker.find(msg.meta.sender);
        if (it1 != tracker.end())
          it1->second = 1;
        std::map<int, int>::iterator it;
        for (it = tracker.begin(); it != tracker.end(); it++)
        {
          if(it->second == 0){
            recv_finish = false;
          }
        }
      }

      if (recv_finish) {
        recv_finish_handle_map_[app_thread_id][model_id]();
      }
      {
        std::lock_guard<std::mutex> lk(mu_);
        if (recv_finish) {
          cond_.notify_all();
        }
      }
    }
  private:
    std::map<uint32_t, std::map<uint32_t, std::function<void(Message&)>>> recv_handle_map_;
    std::map<uint32_t, std::map<uint32_t, std::function<void()>>> recv_finish_handle_map_;
    std::map<uint32_t, std::map<uint32_t, std::map<int,int>>> trackers_;

    std::mutex mu_;
    std::condition_variable cond_;
};
}  // namespace csci5570
