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
        switch (m.meta.flag) {
            case Flag::kExit:
                this->GetModel(m.meta.model_id); //might need remove
                break;
            case Flag::kBarrier:
                this->GetModel(m.meta.model_id); //might need barrier
                break;
            case Flag::kResetWorkerInModel:
                this->GetModel(m.meta.model_id)->ResetWorker(m);
                break;
            case Flag::kClock:
                this->GetModel(m.meta.model_id)->Clock(m);
                break;
            case Flag::kAdd:
                this->GetModel(m.meta.model_id)->Add(m);
                break;
            case Flag::kGet:
                this->GetModel(m.meta.model_id)->Get(m);
                break;
            default:
                //error, no such message flags;
                break;
        }
    }
}
}  // namespace csci5570
