#include <cmath>

#include <iostream>
#include <random>
#include <thread>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "lib/data_loader.hpp"
#include "lib/svm_sample.hpp"
#include "lib/parser.hpp"
#include "lib/batchiterator.hpp"

#include "boost/utility/string_ref.hpp"

#include "base/serialization.hpp"
#include "io/coordinator.hpp"
#include "io/hdfs_assigner.hpp"
#include "io/hdfs_file_splitter.hpp"
#include "io/line_input_format.hpp"

#include "driver/engine_manager.hpp"

//. Define arguments
DEFINE_int32(my_id, -1, "the process id of this program");
DEFINE_string(config_file, "", "The config file path");
// Data loading config
DEFINE_string(hdfs_namenode, "proj10", "The hdfs namenode hostname");
DEFINE_int32(hdfs_namenode_port, 9000, "The hdfs port");
DEFINE_int32(hdfs_master_port, 23489, "A port number for the hdfs assigner host");
DEFINE_int32(n_loaders_per_node, 1, "The number of loaders per node");
DEFINE_string(input, "", "The hdfs input url");
DEFINE_int32(n_features, 10, "The number of feature in the dataset");
// Traing config
DEFINE_int32(n_workers_per_node, 1, "The number of workers per node");
DEFINE_int32(n_iters, 10, "The number of interattions");

namespace csci5570{
  std::vector<double> perception(const std::vector<lib::SVMSample>& samples, const std::vector<Key>& keys, const std::vector<double>& vals) {
    std::vector<double> deltas(keys);
    for (auto sample : samples) {
      auto& x = sample.x_;
      double y = sample.y_;
      double predict = 0.;
      int idx = 0;
      for (auto& field : x) {
        while (keys[idx] < field.first)
          ++idx;
        predict += vals[idx] * field.second;
      }
      predict += vals.back();
      int predictLabel;
      if (predict >= 0) {
        predictLabel = 1;
      } else {
        predictLabel = -1;
      }
      
      if(predictLabel * y <= 0){
        idx = 0;
        for (auto& field : x) {
          deltas[idx] = field.second * y;
          ++idx;
        }
      }
    }
    return deltas;
  }
  
  double correct_rate(const std::vector<lib::SVMSample>& samples, const std::vector<Key>& keys,const std::vector<double>& vals) {
    int total = samples.size();
    double n = 0;
    for (auto sample : samples) {
      auto& x = sample.x_;
      double y = sample.y_;
      double predict = 0.;
      int idx = 0;
      for (auto& field : x) {
        while (keys[idx] < field.first)
          ++idx;
        predict += vals[idx] * field.second;
      }
      predict += vals.back();
      int predict_;
      if (predict >= 0) {
        predict_ = 1;
      } else {
        predict_ = -1;
      }
      if (predict_ == y) {
        n++;
      }
    }
    double result = n / total;
    return result;
  }
  
  void SVMTest(uint32_t node_id, int num_of_node, int node_port, int master_port, int epoch){
    using DataStore = std::vector<lib::SVMSample>;
    using Parser = lib::Parser<lib::SVMSample, DataStore>;
    using Parse = std::function<lib::SVMSample(boost::string_ref, int)>;
    
    DataStore data_store;
    lib::SVMSample svm_sample;
    // Parser svm_parser();
    auto svm_parse = Parser::parse_libsvm;
    
    int n_features = 10;
    std::string url = "hdfs:///datasets/classification/a9";   // Do not change
    std::string hdfs_namenode = "proj10";                        // Do not change
    std::string master_host = "proj" + std::to_string(node_id);  // Set to worker name
    std::string worker_host = "proj" + std::to_string(node_id);  // Set to worker name
    int hdfs_namenode_port = 9000;                               // Do not change
    
    int workers_per_node = 5;
    lib::DataLoader<lib::SVMSample, DataStore> data_loader;
    data_loader.load<Parse>(url, hdfs_namenode, master_host, worker_host, hdfs_namenode_port, master_port, n_features,
                            svm_parse, &data_store);
    uint32_t n = node_id;
    Node node{n, "proj" + std::to_string(node_id), node_port};
    std::vector<Node> nodes;
    for (uint32_t i = 0; i < num_of_node; i++) {
      Node nodet{10 - i, "proj" + std::to_string(10 - i), node_port};
      nodes.push_back(nodet);
    }
    LOG(INFO) << node.hostname;
    Engine engine(node, nodes);
    engine.StartEverything();
    
    // Create table on the server side
    const auto kTable = engine.CreateTable<double>(ModelType::ASP, StorageType::Map);
    
    // Specify task
    MLTask task;
    task.SetTables({kTable});
    std::vector<WorkerAlloc> worker_alloc;
    for (int i = 0; i < num_of_node; i++) {
      worker_alloc.push_back({nodes[i].id, static_cast<uint32_t>(workers_per_node)});
      // woker_alloc.push_back({nodes[i].id, 1});
    }
    task.SetWorkerAlloc(worker_alloc);
    
    LOG(INFO) << node_id << " before learning";
    task.SetLambda([kTable, &data_store](const Info& info) {
      BatchIterator<lib::SVMSample> batch(data_store);
      auto keys_data = batch.NextBatch(2000);
      std::vector<lib::SVMSample> datasample = keys_data.second;
      auto keys = keys_data.first;
      std::vector<double> vals;
      KVClientTable<double> table(info.thread_id, kTable, info.send_queue,
                                  info.partition_manager_map.find(kTable)->second, info.callback_runner);
      std::vector<double> deltas(keys.size(), 0.);
      table.Add(keys, deltas);
      table.Get(keys, &vals);
      auto correctrate = correct_rate(datasample, keys, vals);
      LOG(INFO) << correctrate;
    });
    engine.Run(task);
    engine.Barrier();
    for (int ep = 0; ep < epoch; ep++) {
      LOG(INFO) << node_id << " start new iteration, which is " << ep <<"";
      LOG(INFO) << node_id << " is learning";
      task.SetLambda([kTable, &data_store](const Info& info) {
        BatchIterator<lib::SVMSample> batch(data_store);
        for (int iter = 0; iter < 10; ++iter) {
          auto keys_data = batch.NextBatch(2000);
          std::vector<lib::SVMSample> datasample = keys_data.second;
          auto keys = keys_data.first;
          std::vector<double> vals;
          KVClientTable<double> table(info.thread_id, kTable, info.send_queue,
                                      info.partition_manager_map.find(kTable)->second, info.callback_runner);
          table.Get(keys, &vals);
          auto delta = perception(datasample, keys, vals);
          table.Add(keys, delta);
        }
      });
      engine.Barrier();
      engine.Run(task);
      LOG(INFO) << node_id << " After learning";
      task.SetLambda([kTable, &data_store](const Info& info) {
        BatchIterator<lib::SVMSample> batch(data_store);
        auto keys_data = batch.NextBatch(2000);
        std::vector<lib::SVMSample> datasample = keys_data.second;
        auto keys = keys_data.first;
        std::vector<double> vals;
        KVClientTable<double> table(info.thread_id, kTable, info.send_queue,
                                    info.partition_manager_map.find(kTable)->second, info.callback_runner);
        table.Get(keys, &vals);
        auto correctrate = correct_rate(datasample, keys, vals);
        LOG(INFO) << correctrate;
      });
      engine.Run(task);
      engine.Barrier();
    }
    LOG(INFO) << "Node " << node_id << " complete";
    engine.StopEverything();
  }
};// namespace csci5570

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_stderrthreshold = 0;
  FLAGS_colorlogtostderr = true;
  uint32_t node_id;
  node_id = atoi(argv[1]);
  int num_of_node = atoi(argv[2]);
  int node_port = atoi(argv[3]);
  int master_port = atoi(argv[4]);
  LOG(INFO) << "num of node:" << num_of_node;
  LOG(INFO) << "Engine start at " << node_id;
  LOG(INFO) << "Node port " << node_port;
  LOG(INFO) << "Master port " << master_port;
  csci5570::SVMTest(node_id, num_of_node, node_port, master_port, 10);
}
