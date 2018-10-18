#include "base/message.hpp"

#include <unordered_map>
#include "server/util/pending_buffer.hpp"

namespace csci5570 {
std::vector<Message> PendingBuffer::Pop(const int clock) {
  auto pos = buffer_.find(clock);
  if ( pos == buffer_.end() ) {
    printf("the pending buffer does not has this clock\n");
  } else {
    auto msgs = pos->second;
    buffer_.erase(clock);
    return msgs;
  }
}

void PendingBuffer::Push(const int clock, Message& msg) {
  auto pos = buffer_.find(clock);
  if ( pos == buffer_.end() ) { // no such clock in the buffer
    std::vector<Message> tmp;
    tmp.push_back(msg);
    buffer_.insert(std::make_pair(clock, tmp));
  } else {
    (pos->second).push_back(msg);
  }
}

int PendingBuffer::Size(const int progress) { // this would return the pending buffer size of the specific progress clock
  auto pos = buffer_.find(progress);
  if ( pos == buffer_.end() ) { // no such clock in the buffer
    return 0;
  } else {
    std::vector<Message> msgs = pos->second;
    return msgs.size();
  }
}

}  // namespace csci5570
