#pragma once

namespace csci5570 {

class EngineManager {
 public:
  EngineManager(const Node& node);
 private:
	ThreadsafeQueue<Message> message_queue_;
	bool isPrimary;
};

} 