#include "server/consistency/asp_model.hpp"
#include "glog/logging.h"

namespace csci5570 {

ASPModel::ASPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                   ThreadsafeQueue<Message>* reply_queue) {
  // TODO
  this->model_id_ = model_id;
  this->reply_queue_ = reply_queue;
  this->storage_ = std::move(storage_ptr);
}

void ASPModel::Clock(Message& msg) {
  // TODO
  if (!progress_tracker_.CheckThreadValid(msg.meta.sender))
    return;
  int tid = msg.meta.sender;
  progress_tracker_.AdvanceAndGetChangedMinClock(tid);
  if (progress_tracker_.GetMinClock() % 10 == 0){
    this->Backup();
  }
}

void ASPModel::Add(Message& msg) {
  // TODO
  if (!progress_tracker_.CheckThreadValid(msg.meta.sender)) return;
  Message message = storage_->Add(msg);
  reply_queue_->Push(message);
}

void ASPModel::Get(Message& msg) {
  // TODO
  if (!progress_tracker_.CheckThreadValid(msg.meta.sender))
    return;
  Message message = storage_->Get(msg);
  // add round info
  message.meta.round = GetProgress(msg.meta.sender);
  reply_queue_->Push(message);
}

int ASPModel::GetProgress(int tid) {
  // TODO
  return progress_tracker_.GetProgress(tid);
}

void ASPModel::ResetWorker(Message& msg) {
  // TODO
  third_party::SArray<uint32_t> tids(msg.data[0]);
  std::vector<uint32_t> tidvector;
  for (auto tid : tids) {
    tidvector.push_back(tid);
  }
  progress_tracker_.Init(tidvector);
  Message message;
  message.meta.model_id = model_id_;
  message.meta.recver = msg.meta.sender;
  message.meta.sender = msg.meta.recver;
  message.meta.flag = Flag::kResetWorkerInModel;
  reply_queue_->Push(message);
}

void ASPModel::Backup() {
  storage_->Backup(model_id_);
  progress_tracker_.Backup(model_id_);
}

int ASPModel::Recovery() {
  storage_->Recovery(model_id_);
  int min_clock = progress_tracker_.Recovery(model_id_);
  return min_clock;
}

}  // namespace csci5570
