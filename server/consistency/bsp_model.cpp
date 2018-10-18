#include "server/consistency/bsp_model.hpp"
#include "glog/logging.h"

namespace csci5570 {

BSPModel::BSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                   ThreadsafeQueue<Message>* reply_queue) {
	this->model_id = model_id;
	this->reply_queue_ = reply_queue;
	this->storage_ = storage_ptr;
  // TODO
}

void BSPModel::Clock(Message& msg) {
  // TODO
}

void BSPModel::Add(Message& msg) {
  // TODO
}

void BSPModel::Get(Message& msg) {
  // TODO
}

int BSPModel::GetProgress(int tid) {
  // TODO
}

int BSPModel::GetGetPendingSize() {
  // TODO
}

int BSPModel::GetAddPendingSize() {
  // TODO
}

void BSPModel::ResetWorker(Message& msg) {
  // TODO
}

}  // namespace csci5570
