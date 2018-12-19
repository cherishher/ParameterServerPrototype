#include "server/consistency/bsp_model.hpp"
#include "glog/logging.h"

namespace csci5570 {

BSPModel::BSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                   ThreadsafeQueue<Message>* reply_queue) {
	this->model_id_ = model_id;
	this->reply_queue_ = reply_queue;
	this->storage_ = std::move(storage_ptr);
  // TODO
}

void BSPModel::Clock(Message& msg) {
  // TODO
	if (!progress_tracker_.CheckThreadValid(msg.meta.sender)) return;
	int tid = msg.meta.sender; // the worker that finish its iteration
 	if (progress_tracker_.CheckThreadValid(tid)) {
  		int temp = progress_tracker_.AdvanceAndGetChangedMinClock(tid); // increase its progress

  		// if the slowest progress equal to the fastest progress
	  	if (temp != -1) { // the min_clock (slowest progress has been moved forward), we synchronize it every time

	  		// handle the add/get buffer
	  		for (size_t i = 0; i < add_buffer_.size(); i++){
          Message reply = storage_->Add(add_buffer_[i]);
          reply_queue_->Push(reply);
	  		}
	  		add_buffer_.clear();

	  		for (size_t j = 0; j < get_buffer_.size(); j++){
	  			Message reply = storage_->Get(get_buffer_[j]);
	  			reply_queue_->Push(reply);
	  		}
	  		get_buffer_.clear();
	  	}       
  	}
}

void BSPModel::Add(Message& msg) {
  // TODO
	if (!progress_tracker_.CheckThreadValid(msg.meta.sender)) return;
	add_buffer_.push_back(msg);
	// int tid = msg.meta.sender;
	// if(progress_tracker_.GetProgress(tid) == progress_tracker_.GetMinClock()){
	// 	storage_->Add(msg);
	// } else {
	// 	add_buffer_.push_back(msg);
	// }
}

void BSPModel::Get(Message& msg) {
  // TODO
	if (!progress_tracker_.CheckThreadValid(msg.meta.sender)) return;
	int tid = msg.meta.sender;
	if(progress_tracker_.GetProgress(tid) == progress_tracker_.GetMinClock()){
		Message reply = storage_->Get(msg);
		// add round info
  	reply.meta.round = GetProgress(msg.meta.sender);
	  reply_queue_->Push(reply);
	} else {
		get_buffer_.push_back(msg);
	}
}

int BSPModel::GetProgress(int tid) {
  // TODO
	return progress_tracker_.GetProgress(tid);
}

int BSPModel::GetGetPendingSize() {
  // TODO
	return get_buffer_.size();
}

int BSPModel::GetAddPendingSize() {
  // TODO
	return add_buffer_.size();
}

void BSPModel::ResetWorker(Message& msg) {
  // TODO

	third_party::SArray<uint32_t> tids(msg.data[0]); // use copy constrictor to do type transfer
	std::vector<uint32_t> tids_v;
	for(int i = 0; i < tids.size(); i++){ // the the Init parameter only accept vector!
		tids_v.push_back(tids[i]);
	}
	progress_tracker_.Init(tids_v);
	Message message;
  message.meta.model_id = model_id_;
  message.meta.recver = msg.meta.sender;
  message.meta.sender = msg.meta.recver;
  message.meta.flag = Flag::kResetWorkerInModel;
  reply_queue_->Push(message);
}

void BSPModel::Backup() {
  storage_->Backup(model_id_);
  progress_tracker_.Backup(model_id_);
}

void BSPModel::Recovery() {
  storage_->Recovery(model_id_);
  progress_tracker_.Recovery(model_id_);
}


}  // namespace csci5570
