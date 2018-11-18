#pragma once

#include "base/actor_model.hpp"
#include "base/threadsafe_queue.hpp"
#include "glog/logging.h"

#include <thread>
#include <unordered_map>

namespace csci5570 {

class HeartbeatServerThread : public Actor {
  public:
    HeartbeatServerThread(uint32_t server_id, ThreadsafeQueue<Message>* reply_queue): Actor(server_id),
                                                          reply_queue_(reply_queue) {}

  protected:
    virtual void Main() override {
      LOG(INFO) << "HeartbeatServerThread " << id_ << "is running";
      Message m;
      while (true) {
        GetWorkQueue()->WaitAndPop(&m);
        if (m.meta.flag == Flag::kExit) break;
        switch (m.meta.flag) {
          case Flag::kHeartbeat:
            Message reply = m;
            reply.meta.recver = m.meta.sender;
            reply.meta.sender = m.meta.recver;
            reply_queue_->Push(reply);
            break;
        }
      }
    }

    ThreadsafeQueue<Message>* reply_queue_;     // not owned, the queue where reply messages are put
};

}  // namespace csci5570
