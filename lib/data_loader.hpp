#pragma once
#include <thread>
#include <vector>
#include "base/serialization.hpp"
#include "boost/utility/string_ref.hpp"
#include "io/coordinator.hpp"
#include "io/hdfs_assigner.hpp"
#include "io/hdfs_file_splitter.hpp"
#include "io/line_input_format.hpp"
#include "lib/abstract_data_loader.hpp"
#include "lib/labeled_sample.hpp"
// Modified by hym
#include "glog/logging.h"
#include "lib/parser.hpp"
namespace csci5570 {
  namespace lib {
    
    template <typename Sample, typename DataStore>
    class DataLoader : public AbstractDataLoader<Sample, DataStore> {
    public:
      template <typename Parse>  // e.g. std::function<Sample(boost::string_ref, int)>
      static void load(std::string hdfs_namenode, int hdfs_namenode_port, int master_port, std::string url, int n_features,
                       Parse parse, DataStore* datastore) {
        // static void load(std::string url, int n_features, Parse parse, DataStore* datastore) {
        // 1. Connect to the data source, e.g. HDFS, via the modules in io
        // 2. Extract and parse lines
        // 3. Put samples into datastore
        
        // std::string hdfs_namenode = FLAGS_hdfs_namenode;
        // int hdfs_namenode_port = FLAGS_hdfs_namenode_port;
        // int master_port = FLAGS_hdfs_master_port;  // use a random port number to avoid collision with other users
        zmq::context_t zmq_context(1);
        
        // 1. Spawn the HDFS block assigner thread on the master
        std::thread master_thread([&zmq_context, master_port, hdfs_namenode_port, hdfs_namenode] {
          HDFSBlockAssigner hdfs_block_assigner(hdfs_namenode, hdfs_namenode_port, &zmq_context, master_port);
          hdfs_block_assigner.Serve();
        });
        
        // 2. Prepare meta info for the master and workers
        int proc_id = getpid();  // the actual process id, or you can assign a virtual one, as long as it is distinct
        std::string master_host = "proj10";  // change to the node you are actually using
        std::string worker_host = "proj10";  // change to the node you are actually using
        
        // 3. One coordinator for one process
        Coordinator coordinator(proc_id, worker_host, &zmq_context, master_host, master_port);
        coordinator.serve();
        LOG(INFO) << "Coordinator begins serving";
        const std::string input = url;
        std::thread worker_thread([input, hdfs_namenode_port, hdfs_namenode, &coordinator, worker_host, parse, &datastore] {
          int num_threads = 1;
          int second_id = 0;
          LineInputFormat infmt(input, num_threads, second_id, &coordinator, worker_host, hdfs_namenode,
                                hdfs_namenode_port);
          LOG(INFO) << "Line input is well prepared";
          
          // Line counting demo
          // Deserialing logic in UDF/application library
          bool success = true;
          int count = 0;
          boost::string_ref record;
          while (true) {
            success = infmt.next(record);
            if (!success) {
              break;
            }
            auto temp_sample = parse(record, 10);
            // LOG(INFO) << "Sample:" << count << " " << temp_sample.toString();
            datastore->push_back(temp_sample);
            ++count;
            // if (count == 20) {
            //   break;
            // }
          }
          LOG(INFO) << "The number of lines in " << input << " is " << count;
          
          // Remember to notify master that the worker wants to exit
          BinStream finish_signal;
          finish_signal << worker_host << second_id;
          coordinator.notify_master(finish_signal, 300);
        });
        // Make sure zmq_context and coordinator live long enough
        master_thread.join();
        worker_thread.join();
      }
      
      // Developed by Andy
      template <typename Parse>  // e.g. std::function<Sample(boost::string_ref, int)>
      static void load(std::string url, std::string hdfs_namenode, std::string master_host, std::string worker_host,
                       int hdfs_namenode_port, int master_port, int n_features, Parse parse, DataStore* datastore) {
        // 1. Connect to the data source, e.g. HDFS, via the modules in io
        // 2. Extract and parse lines
        // 3. Put samples into datastore
        
        LOG(INFO) << "URL:" << url;
        // int hdfs_namenode_port = 9000;
        // int master_port = 45743;  // use a random port number to avoid collision with other users
        zmq::context_t zmq_context(1);
        
        // 1. Spawn the HDFS block assigner thread on the master
        std::thread master_thread([&zmq_context, master_port, hdfs_namenode_port, hdfs_namenode] {
          HDFSBlockAssigner hdfs_block_assigner(hdfs_namenode, hdfs_namenode_port, &zmq_context, master_port);
          hdfs_block_assigner.Serve();
        });
        
        // 2. Prepare meta info for the master and workers
        int proc_id = getpid();  // the actual process id, or you can assign a virtual one, as long as it is distinct
        // std::string master_host = "proj10";  // change to the node you are actually using
        // std::string worker_host = "proj10";  // change to the node you are actually using
        
        // 3. One coordinator for one process
        Coordinator coordinator(proc_id, worker_host, &zmq_context, master_host, master_port);
        coordinator.serve();
        LOG(INFO) << "Coordinator begins serving";
        // const std::string input = url;
        std::thread worker_thread([url, hdfs_namenode_port, hdfs_namenode, &coordinator, worker_host, parse, &datastore] {
          // std::thread worker_thread([input, hdfs_namenode_port, hdfs_namenode, &coordinator, worker_host, parse,
          // &datastore] {
          // [&input, hdfs_namenode_port, hdfs_namenode, &coordinator, worker_host, parse, &datastore] {
          // std::string input = "hdfs:///datasets/classification/a9";
          // std::string input = "hdfs:///datasets/classification/kdd12";
          // std::string input = url;
          // LOG(INFO) << "In the thread";
          int num_threads = 1;
          int second_id = 0;
          // LOG(INFO) << "Print url in thread";
          // LOG(INFO) << url;
          LineInputFormat infmt(url, num_threads, second_id, &coordinator, worker_host, hdfs_namenode, hdfs_namenode_port);
          LOG(INFO) << "Line input is well prepared";
          // LOG(INFO) << "After infmt";
          // Line counting demo
          // Deserialing logic in UDF/application library
          bool success = true;
          int count = 0;
          boost::string_ref record;
          while (true) {
            success = infmt.next(record);
            if (!success) {
              break;
            }
            auto temp_sample = parse(record, 10);
            // LOG(INFO) << "Sample:" << count << " " << temp_sample.toString();
            datastore->push_back(temp_sample);
            ++count;
            if (count == 20000) {
              break;
            }
          }
          LOG(INFO) << "The number of lines:" << count;
          
          // Remember to notify master that the worker wants to exit
          BinStream finish_signal;
          finish_signal << worker_host << second_id;
          coordinator.notify_master(finish_signal, 300);
        });
        // Make sure zmq_context and coordinator live long enough
        master_thread.join();
        worker_thread.join();
      }
      /* old function*/
      template <typename Parse>  // e.g. std::function<Sample(boost::string_ref, int)>
      static void load(std::string url, int n_features, Parse parse, DataStore* datastore) {
        // 1. Connect to the data source, e.g. HDFS, via the modules in io
        // 2. Extract and parse lines
        // 3. Put samples into datastore
        std::string hdfs_namenode = "proj10";
        LOG(INFO) << "URL:" << url;
        int hdfs_namenode_port = 9000;
        int master_port = 45743;  // use a random port number to avoid collision with other users
        zmq::context_t zmq_context(1);
        
        // 1. Spawn the HDFS block assigner thread on the master
        std::thread master_thread([&zmq_context, master_port, hdfs_namenode_port, hdfs_namenode] {
          HDFSBlockAssigner hdfs_block_assigner(hdfs_namenode, hdfs_namenode_port, &zmq_context, master_port);
          hdfs_block_assigner.Serve();
        });
        
        // 2. Prepare meta info for the master and workers
        int proc_id = getpid();  // the actual process id, or you can assign a virtual one, as long as it is distinct
        std::string master_host = "proj10";  // change to the node you are actually using
        std::string worker_host = "proj10";  // change to the node you are actually using
        
        // 3. One coordinator for one process
        Coordinator coordinator(proc_id, worker_host, &zmq_context, master_host, master_port);
        coordinator.serve();
        LOG(INFO) << "Coordinator begins serving";
        const std::string input = url;
        std::thread worker_thread([input, hdfs_namenode_port, hdfs_namenode, &coordinator, worker_host, parse, &datastore] {
          // [&input, hdfs_namenode_port, hdfs_namenode, &coordinator, worker_host, parse, &datastore] {
          // std::string input = "hdfs:///datasets/classification/a9";
          // std::string input = "hdfs:///datasets/classification/kdd12";
          // std::string input = url;
          // LOG(INFO) << "In the thread";
          int num_threads = 1;
          int second_id = 0;
          LOG(INFO) << "Print input";
          LOG(INFO) << input;
          LineInputFormat infmt(input, num_threads, second_id, &coordinator, worker_host, hdfs_namenode,
                                hdfs_namenode_port);
          LOG(INFO) << "Line input is well prepared";
          // LOG(INFO) << "After infmt";
          // Line counting demo
          // Deserialing logic in UDF/application library
          bool success = true;
          int count = 0;
          boost::string_ref record;
          while (true) {
            success = infmt.next(record);
            if (!success) {
              break;
            }
            auto temp_sample = parse(record, 10);
            // LOG(INFO) << "Sample:" << count << " " << temp_sample.toString();
            datastore->push_back(temp_sample);
            ++count;
            if (count == 20000) {
              break;
            }
          }
          LOG(INFO) << "The number of lines:" << count;
          
          // Remember to notify master that the worker wants to exit
          BinStream finish_signal;
          finish_signal << worker_host << second_id;
          coordinator.notify_master(finish_signal, 300);
        });
        // Make sure zmq_context and coordinator live long enough
        master_thread.join();
        worker_thread.join();
      }
      void test(){
        
      };
    };  // Class DataLoader
  }  // namespace lib
}  // namespace csci5570
