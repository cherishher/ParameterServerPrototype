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

// assume the matrix A is m*n where m>>n (tall and skinny)
// it's reasonable to assume that, for example a Netflix movie
// system, may include a lot of users but less than 1k movies

// overall algorithm:
// 1. compute A'A (A transpose * A)
// 2. compute top K singular values of A'A on a single machine
// 3. compute U = AVsigma-1

// 1. compute A'A
// input: a row of A
void ATA(std::vector<double> &row, int row_id){
  map<vector<int>,double> products;
  vector<int> key;
  for(int i = 0; i < row.size(); i++){
    for(int j = i; j < row.size(); j++)
      if (row[i] != 0 && row[j] != 0){
        double value = row[i] * row[j];
        // emit (key:<i,j>, value: row[i]*row[j])
        // also emit (key<j,i>) as well
        key.clear();
        key.push_back(i);
        key.push_back(j);
        products.insert(key,value);
        key.clear();
        key.push_back(j);
        key.push_back(i);
        products.insert(key,value);
      }
  }
}

void aggregate(){
  
}

// 2. compute top K singular values of A'A on a single machine
void topK(std::vector<vector<double>> &matrix){
  // this can be done in a single machine
  // then just call the lib is OK
}

void computeU(){

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
