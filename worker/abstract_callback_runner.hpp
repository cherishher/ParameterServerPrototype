#pragma once

#include <functional>

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
  virtual void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) = 0;

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
    void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) {
      trackers_[app_thread_id][model_id] = {expected_responses, 0};
    }
    void WaitRequest(uint32_t app_thread_id, uint32_t model_id) {
      std::unique_lock<std::mutex> lk(mu_);
      cond_.wait(lk, [this, app_thread_id, model_id] {
        auto &tracker = trackers_[app_thread_id][model_id];
          return tracker.first == tracker.second;
        });
    }
    void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) {
      printf("have we done here?\n");
      bool recv_finish = false;
      auto &tracker = trackers_[app_thread_id][model_id];
      {
        std::lock_guard<std::mutex> lk(mu_);
        recv_finish = tracker.first == tracker.second + 1 ? true : false;
      }
      recv_handle_map_[app_thread_id][model_id](msg);
      if (recv_finish) {
        recv_finish_handle_map_[app_thread_id][model_id]();
      }
      {
        std::lock_guard<std::mutex> lk(mu_);
        tracker.second += 1;
        if (recv_finish) {
          cond_.notify_all();
        }
      }
    }
  private:
    std::map<uint32_t, std::map<uint32_t, std::function<void(Message&)>>> recv_handle_map_;
    std::map<uint32_t, std::map<uint32_t, std::function<void()>>> recv_finish_handle_map_;
    std::map<uint32_t, std::map<uint32_t, std::pair<int, int>>> trackers_;

    std::mutex mu_;
    std::condition_variable cond_;
};

}  // namespace csci5570
