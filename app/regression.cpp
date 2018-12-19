#include <cmath>

#include <iostream>
#include <random>
#include <thread>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "lib/abstract_data_loader.hpp"
#include "lib/labeled_sample.hpp"
#include "lib/parser.hpp"

#include "driver/engine_manager.hpp"

using namespace csci5570;

using Sample = double;
using DataStore = std::vector<Sample>;

DEFINE_string(config_file, "", "The config file path");
DEFINE_string(input, "", "The hdfs input url");

double alpha = 0.1;
double lambda = 0.1;


double mul(std::vector<int> &pt, std::vector<double> &theta) {
  double sum = 0;
  for (int i = 0; i < pt.size(); ++i) {
    sum += pt[i] * theta[i];
  }
  return sum;
}

int h(std::vector<int> &pt, std::vector<double> &theta) {
  double sum = mul(pt, theta);
  return (1 / (1 + exp(-sum)) < 0.5) ? : 0 : 1;
}

double cost(std::vector<int> &pt, std::vector<double> &theta, int pt_label) {
  double sum = h(pt, theta);
  return pt_label == 1 ? -log(sum) : -log(1 - sum);
}

double j(std::vector<std::vector<int>> &samples, std::vector<double> &theta) {
  double sum = 0;
  for (int i = 0; i < samples.size(); ++i) {
    sum += cost(samples[i].x_, theta, samples[i].y_);
  }
  double temp = 0;
  for (int i = 2; i < theta.size(); ++i) {
    temp += theta[i] * theta[i];
  }
  return sum / samples.size() + lambda * temp / 2 / samples.size();
}

// x_[0] = 1
void update_theta(std::vector<std::vector<int>> &samples, std::vector<double> &theta) {
  std::vector<int> diff;
  
  for (int i = 0; i < samples.size(); ++i) {
    diff.push_back(h(samples[i].x_, theta) - samples[i].y_);
  }
  
  for (int i = 0; i < theta.size(); ++i) {
    double sum = 0;
    for (int j = 0; j < samples.size(); ++j) {
      sum += diff[j] * samples[j].x_[i];
    }
    sum /= samples.size();
    if (i != 0) sum += lambda * theta[i] / samples.size();
    
    theta[i] -= alpha * sum;
  }
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  FLAGS_stderrthreshold = 0;
  FLAGS_colorlogtostderr = true;
  
  LOG(INFO) << FLAGS_config_file;
  
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  
  // 1. Start system
  engine.StartEverything();
  
  // 1.1 Create table
  const auto kTableId = engine.CreateTable<double>(ModelType::SSP, StorageType::Map);  // table 0
  
  // 1.2 Load data
  engine.Barrier();
  
  // 2. Start training task
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});     // Use table 0
  task.SetLambda([kTableId](const Info& info) {
    LOG(INFO) << info.DebugString();
    
    KVClientTable<double> table = info.CreateKVClientTable<double>(kTableId);
    
    for (int i = 0; i < 1e3; ++i) {
      std::vector<Key> keys{1};
      
      std::vector<double> ret;
      table.Get(keys, &ret);
      LOG(INFO) << ret[0];
      
      std::vector<double> vals{0.5};
      table.Add(keys, vals);
      
      table.Clock();
    }
  });
  
  engine.Run(task);
  
  // 3. Stop
  engine.StopEverything();
  
  
  EngineManager em(node, {node});
  std::cout << "test\n";
  return 0;
}
