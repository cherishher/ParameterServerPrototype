#include "driver/simple_id_mapper.hpp"

#include <cinttypes>
#include <vector>

#include "base/node.hpp"

namespace csci5570 {

SimpleIdMapper::SimpleIdMapper(Node node, const std::vector<Node>& nodes) {
  node_ = node;
  nodes_ = nodes;
}

uint32_t SimpleIdMapper::GetNodeIdForThread(uint32_t tid) { return tid / kMaxThreadsPerNode; }

void SimpleIdMapper::Init(int num_server_threads_per_node) {
  if (num_server_threads_per_node >= 1 && num_server_threads_per_node <= kMaxThreadsPerNode) {
    for (auto node : nodes_) {
      for (int i = 0; i < num_server_threads_per_node; i++) {
        // node2server_.insert(std::make_pair(node.id,node.id*kMaxThreadsPerNode+i));
        std::vector<uint32_t> serverThreads;
        serverThreads.push_back(node.id * kMaxThreadsPerNode+i);
        node2server_[node.id] = serverThreads;
      }
      std::vector<uint32_t> workerHelperThread;
      workerHelperThread.push_back(kWorkerHelperThreadId + node.id * kMaxThreadsPerNode);
      node2worker_helper_.insert(std::make_pair(node.id, workerHelperThread));

      std::set<uint32_t> workerThreads;
      //workerThreads.insert(node_.id*kMaxThreadsPerNode+kWorkerHelperThreadId+1);
      node2worker_.insert(std::make_pair(node_.id,workerThreads));
    };
  }
}

uint32_t SimpleIdMapper::AllocateWorkerThread(uint32_t node_id) {
  uint32_t allocateId;
  try {
    if (node2worker_[node_id].size() >= 0 && node2worker_[node_id].size() < kMaxThreadsPerNode - kMaxBgThreadsPerNode) {
      std::set<uint32_t> workerSet = node2worker_[node_id];
      if(workerSet.size()< kMaxThreadsPerNode - kMaxBgThreadsPerNode){
        uint32_t start = node_id * kMaxThreadsPerNode + kMaxBgThreadsPerNode;
        uint32_t end = (node_id+1) * kMaxThreadsPerNode;
        for(uint32_t id = start ;id < end ; id++){
          if (workerSet.find(id) == workerSet.end()) {
            allocateId = id;
            workerSet.insert(allocateId);
            node2worker_[node_id] = workerSet;
            break;
          }
        }
      }
    }else{
      allocateId == -1;
    }
  } catch (const std::exception& e) {
    allocateId = -1;
  }
  return allocateId;
}

void SimpleIdMapper::DeallocateWorkerThread(uint32_t node_id, uint32_t tid) {
  if (node2worker_[node_id].size() != 0) {
    node2worker_[node_id].erase(tid);
  }
}

std::vector<uint32_t> SimpleIdMapper::GetServerThreadsForId(uint32_t node_id) { return node2server_[node_id]; }
std::vector<uint32_t> SimpleIdMapper::GetWorkerHelperThreadsForId(uint32_t node_id) {
  return node2worker_helper_[node_id];
}
std::vector<uint32_t> SimpleIdMapper::GetWorkerThreadsForId(uint32_t node_id) {
  std::vector<uint32_t> workerThreads;
  for (auto worker : node2worker_[node_id]) {
    workerThreads.push_back(worker);
  }
  return workerThreads;
}

std::vector<uint32_t> SimpleIdMapper::GetAllServerThreads() {
  std::vector<uint32_t> serverThreads;
  for (auto node : nodes_) {
    std::vector<uint32_t> currentServerThreads = this->GetServerThreadsForId(node.id);
    serverThreads.insert(serverThreads.end(), currentServerThreads.begin(), currentServerThreads.end());
  }

  return serverThreads;
}

const uint32_t SimpleIdMapper::kMaxNodeId;
const uint32_t SimpleIdMapper::kMaxThreadsPerNode;
const uint32_t SimpleIdMapper::kMaxBgThreadsPerNode;
const uint32_t SimpleIdMapper::kWorkerHelperThreadId;

}  // namespace csci5570
