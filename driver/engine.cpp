#include "driver/engine.hpp"

#include <vector>

#include "base/abstract_partition_manager.hpp"
#include "base/node.hpp"
#include "comm/mailbox.hpp"
#include "comm/sender.hpp"
#include "driver/ml_task.hpp"
#include "driver/simple_id_mapper.hpp"
#include "driver/worker_spec.hpp"
#include "server/server_thread.hpp"
#include "worker/abstract_callback_runner.hpp"
#include "worker/worker_thread.hpp"

namespace csci5570 {

/**
   * The flow of starting the engine:
   * 1. Create an id_mapper and a mailbox
   * 2. Start Sender
   * 3. Create ServerThreads and WorkerThreads
   * 4. Register the threads to mailbox through ThreadsafeQueue
   * 5. Start the communication threads: bind and connect to all other nodes
   *
   * @param num_server_threads_per_node the number of server threads to start on each node
   */
void Engine::StartEverything(int num_server_threads_per_node) {
  // 1. create an id_mapper
  CreateIdMapper(num_server_threads_per_node);
  // 2. create an mailbox
  CreateMailbox();
  // 3. start sender
  StartSender();
  // 4. create/start server threads and register them into ThreadsafeQueue
  StartServerThreads();
  for (int i = 0; i < server_thread_group_.size(); ++i) {
    ThreadsafeQueue<Message>* queue = server_thread_group_[i]->GetWorkQueue(); // GetWorkQueue defined in Actor
    mailbox_->RegisterQueue(server_thread_group_[i]->GetId(), queue); // GetID defiend in Actor
  }
  // 5. create/start worker threads and register them by ThreadsafeQueue
  StartWorkerThreads();
  ThreadsafeQueue<Message>* queue = worker_thread_->GetWorkQueue();
  mailbox_->RegisterQueue(worker_thread_->GetId(), queue);
  // 6. start communication
  StartMailbox();
}

void Engine::CreateIdMapper(int num_server_threads_per_node) {
  //id_mapper_ = std::unique_ptr<SimpleIdMapper>(new SimpleIdMapper(node_, nodes_));
  id_mapper_.reset(new SimpleIdMapper(node_, nodes_)); // using . instead of ->
  id_mapper_->Init(num_server_threads_per_node);
}

void Engine::CreateMailbox() {
  // mailbox_.reset(new Mailbox(node_, nodes_, id_mapper_));
  mailbox_.reset(new Mailbox(node_, nodes_, id_mapper_.get())); // get() return the pointer
}

void Engine::StartServerThreads() {
  std::vector<uint32_t> sids = id_mapper_->GetServerThreadsForId(node_.id);
  for (int i = 0; i < sids.size(); i++){
    server_thread_group_.push_back(new ServerThread(sids[i]));
  }
  for (int i = 0; i < server_thread_group_.size(); i++){
    server_thread_group_[i].Start();
  }
}
void Engine::StartWorkerThreads() { // ? s?
  uint32_t wid = id_mapper_->AllocateWorkerThread(node_.id);
  //std::vector<uint32_t> wids = id_mapper_->GetWorkerThreadsForId(node_.id);
  // for (int i = 0; i < wids.size(); i++){
  //   worker_thread_group_.push_back(new AbstractWorkerThread(wid));
  // }
  callback_runner_.reset(new AbstractCallbackRunner());
  worker_thread_.reset(new AbstractWorkerThread(wid, callback_runner_.get())); // need to modify worker_thread!!! call_back logic
  worker_thread_->Start();
}

void Engine::StartMailbox() {
  mailbox_->Start();
}

void Engine::StartSender() {
  sender_.reset(new Sender(mailbox_.get()));
  sender_->Start();
}

/**
   * The flow of stopping the engine:
   * 1. Stop the Sender
   * 2. Stop the mailbox: by Barrier() and then exit
   * 3. The mailbox will stop the corresponding registered threads
   * 4. Stop the ServerThreads and WorkerThreads
   */
void Engine::StopEverything() {
  // 1. Stop sender
  StopSender();
  // 2. Stop mailbox
  Barrier();
  StopMailbox();
  // 3. Stop server thread
  StopServerThreads();
  // 4. Stop worker thread
  StopWorkerThreads();
}

void Engine::StopServerThreads() {
  for(int i = 0; i < server_thread_group_.size(); i++){
    server_thread_group_[i].Stop();
  }
}
void Engine::StopWorkerThreads() {
  worker_thread_->Stop();
}
void Engine::StopSender() {
  sender_->Stop();
}
void Engine::StopMailbox() {
  mailbox_->Stop();
}

void Engine::Barrier() {
  mailbox_->Barrier();
}

// This is for user worker thread
// WorkerAlloc defined in ml_task; including uint32_t node_id and uint32_t num_workers;
WorkerSpec Engine::AllocateWorkers(const std::vector<WorkerAlloc>& worker_alloc) {
  WorkerSpec worker_spec(worker_alloc);
  for (int i = 0; i < worker_alloc.size(); i++) {
    uint32_t node_id = worker_alloc[i].node_id;

    auto wids = worker_spec.GetLocalWorkers(node_id);

    for (auto wid: wids) {
      uint32_t uid = id_mapper_->AllocateWorkerThread(node_.id);
      worker_spec.InsertWorkerIdThreadId(wid, uid);
    }
  }
  return worker_spec;
}

void Engine::InitTable(uint32_t table_id, const std::vector<uint32_t>& worker_ids) {
  // TODO
}

void Engine::Run(const MLTask& task) {
  std::vector<uint32_t> model_ids = task.GetTables();
  auto wokrallocs = taks.GetWorkerAlloc();
  WorkerSpec workerspec = AllocateWorkers(workallocs);

  for(auto mid : model_ids){
    InitTable(mid, workerspec.GetAllThreadIds());
  }
}

void Engine::RegisterPartitionManager(uint32_t table_id, std::unique_ptr<AbstractPartitionManager> partition_manager) {
  partition_manager_map_[table_id] = partition_manager;
}

}  // namespace csci5570
