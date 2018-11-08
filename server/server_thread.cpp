#include "server/server_thread.hpp"

#include "glog/logging.h"
#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"

namespace csci5570 {
    
void ServerThread::RegisterModel(uint32_t model_id, std::unique_ptr<AbstractModel>&& model) {
    //insert the model if model_id not exist, assign new model if model_id exist
    models_.insert(std::make_pair(model_id, std::move(model)));
}
    
AbstractModel* ServerThread::GetModel(uint32_t model_id) {
    //find the model_id, return the value.
    auto search = models_.find(model_id);
    if (search != models_.end()) {
        //Found, return the value
        return search->second.get();
    } else {
        //Not Found
        return nullptr;
    }
}
    
void ServerThread::Main() {
    //We might have to know which model need to process.
    auto* work_queue = this->GetWorkQueue();
    while (true) {
        Message m;
        work_queue->WaitAndPop(&m);
        int id = m.meta.model_id;
        if(m.meta.flag == Flag::kExit){
          return;
        }
        auto* ptr = GetModel(id);
        if(ptr == nullptr){
          continue;
        }
        switch (m.meta.flag) {
            case Flag::kExit:
                return;
            case Flag::kBarrier:
                break;
            case Flag::kResetWorkerInModel:
                ptr->ResetWorker(m);
                break;
            case Flag::kClock:
                ptr->Clock(m);
                break;
            case Flag::kAdd:
                ptr->Add(m);
                break;
            case Flag::kGet:
                ptr->Get(m);
                break;
            default:
                //error, no such message flags;
                break;
        }
    }
}
}  // namespace csci5570
