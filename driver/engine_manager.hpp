#pragma once

#include <ctime>
#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <deque>

namespace csci5570 {

class EngineManager : public Engine{
 public:

	EngineManager(const Node& node, const std::vector<Node>& nodes, bool isPrimary = true){
		this->node_ = node;
		this->nodes_ = nodes;
		this->isPrimary_ = isPrimary;
	}

	void StartEM(){
		StartAllEngines();
		id_mapper_.reset(new SimpleIdMapper(node_, nodes_));
    	id_mapper_->Init(1); // num_server_threads_per_node = 1
    	mailbox_.reset(new Mailbox(node_, nodes_, id_mapper_.get()));
    	mailbox_->Start();
    	sender_.reset(new Sender(mailbox_.get()));
    	sender_->Start();

    	// see if it needs to be changed !!!
		std::vector<uint32_t> sids = id_mapper_->GetServerThreadsForId(node_.id);
		HeartBeatServerThread_ = new ServerThread(sids.size()-1);

    	StartHeartBeat();
	}

	void StopEM(){
		StopAllEngines();
		sender_->Stop();
		mailbox_->Stop();
		StopHeartBeat();
	}

	StartAllEngines(){
		for (int i = 0; i < nodes.size(); i++){
			if (node_ != nodes_[i]){
	  			std::unique_ptr<Engine> ptr(new Engine(nodes_[i], nodes_));
	  			ptr->StartEverything();
      			engine_group_.push_back(std::move(ptr));
  			}
		}
	}

	StopAllEngines(){
		for (int i = 0; i < nodes.size(); i++){
			if (node_ != nodes_[i]){
      			engine_group_[i].get()->StopEverything();
  			}
		}
	}

	void HeartBeat(){
		
		// call get thread_id with node_id as parameter for communication
		uint32_t from_node_thread_id = id_mapper->GetHeartBeatThreadForId(node_.id);
		time_t timestamp = time(NULL);

		for(int i = 0; i < nodes_.size(); i++) {

			uint32_t to_node_thread_id = id_mapper->GetHeartBeatThreadForId(nodes_[i].id);

			Message msg // the heartbeat message
		    msg.meta.flag = Flag::kHeartbeat;
		    msg.meta.sender = from_node_thread_id;
		    msg.meta.recver = to_node_thread_id;
		    msg.meta.timestamp = timestamp;

			sender_.get()->GetMessageQueue()->Push(msg);
			heartbeat_count_[to_node_thread_id].push_back(timestamp);
		}
		sender_->Send();

		// add this time stamp to all the nodes's already sent heartbeat_count_

	}

	bool HeartBeatDetection(int threshold = 4){
		
		// get heartbeat server thread's messages
      	HeartBeatServerThread_.get()->GetWorkQueue();

		// check message queue, put it into maps
		ThreadsafeQueue<Message>* queue;
		while(queue->Size() > 0){
			Message msg;
			queue->WaitAndPop(msg);

			// check maps
			if(heartbeat_count_.find(msg.sender) == heartbeat_count_.end()){
				deque<time_t> deq;
				heartbeat_count_.insert(pair<uint32_t, deque<time_t>>(msg.sender, new deque<time_t>));
			} else {
				for (int i = 0; i < heartbeat_count_[msg.sender].size(); i++){
					if(heartbeat_count_[msg.sender][i] < msg.timestamp){
						heartbeat_count_.pop_front();
					}
				}
			}
			// if more than 3, restart that engine.
		}

		for(std::map<uint32_t, deque<time_t>>::iterator it = heartbeat_count_.begin(); it != heartbeat_count_.end(); ++it){
			if(it->second.size() >= threshold){
				// restart this engine...
			}
		}
	}

	// timer in seconds
	bool StartHeartBeat(int interval = 10){
		if (expired_ == false){
  			// timer is currently running, please expire it first...
            return false;
        }
        expired_ = false;

        heartbeat_count_.clear();
        for(int i = 0; i < nodes_.size(); i++){
        	heartbeat_count_.insert(pair<uint32_t, deque<time_t>>(id_mapper->GetHeartBeatThreadForId(nodes_[i].id), new deque<time_t>));
        }

        std::thread([this, interval](){
            while (!try_to_expire_){
	            std::this_thread::sleep_for(std::chrono::seconds(interval));
	            HeartBeat();
	            HeartBeatDetection();
            }
            // stop timer task
            {
                std::lock_guard<std::mutex> locker(mutex_);
                expired_ = true;
                expired_cond_.notify_one();
            }
        }).detach();
        return true;
	}

	bool StopHeartBeat(){
		if (expired_){
            return false;
        }
        if (try_to_expire_){
             // timer is trying to expire, please wait...
            return false;
        }

        try_to_expire_ = true;

        {
            std::unique_lock<std::mutex> locker(mutex_);
            expired_cond_.wait(locker, [this]{return expired_ == true; });
            if (expired_ == true){
                try_to_expire_ = false;
                return true;
            }
        }
	}

 private:
 	// Node node_;
 	// std::vector<Node> nodes_; // the EM will occupy an entire Node! the nodes includes all the nodes.
	// std::unique_ptr<Mailbox> mailbox_; // inherit from engine
	// std::unique_ptr<Sender> sender_; // inherit from engine
	
	bool isPrimary_ = true; // denote the primary engine manager if yes, or the secondary engine manager if false
	map<uint32_t, deque<time_t>> heartbeat_count_; // thread_id, each engine's heartbeat message

	std::unique_ptr<ServerThread> HeartBeatServerThread_;

	std::vector<std::unique_ptr<Engine>> engine_group_;

	// about the timer taks
	std::atomic<bool> expired_;
	std::atomic<bool> try_to_expire_;
	std::mutex mutex_;
	std::condition_variable expired_cond_;
};

} 

// Message Type