#include "comm/sender.hpp"

namespace csci5570 {
Sender::Sender(AbstractMailbox* mailbox) : mailbox_(mailbox) {}

void Sender::Start() {
  sender_thread_ = std::thread([this] { Send(); });
}

void Sender::Send() {
  int count = 0;
  while (true) {
    count ++;
    Message to_send;
    send_message_queue_.WaitAndPop(&to_send);
    printf("=========================================\n");
    printf("sender get number %d message to send\n", count);
    printf("sender is %d\n",to_send.meta.sender);
    printf("receiver is %d\n",to_send.meta.recver);
    printf("model_id is %d\n",to_send.meta.model_id);
    printf("flag is %d\n",to_send.meta.flag);
    printf("=========================================\n");
    if (to_send.meta.flag == Flag::kExit)
      break;
    mailbox_->Send(to_send);
  }
}

ThreadsafeQueue<Message>* Sender::GetMessageQueue() { return &send_message_queue_; }

void Sender::Stop() {
  Message stop_msg;
  stop_msg.meta.flag = Flag::kExit;
  send_message_queue_.Push(stop_msg);
  sender_thread_.join();
}

}  // namespace csci5570
