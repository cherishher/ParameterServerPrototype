#pragma once

#include <functional>
#include <string>

#include "boost/utility/string_ref.hpp"

#include "lib/parser.hpp"

namespace csci5570 {
namespace lib {

// template <typename Sample, template <typename> typename DataStore<Sample>>
template <typename Sample, typename DataStore>
class AbstractDataLoader {
 public:
  /**
   * Load samples from the url into datastore
   *
   * @param url          input file/directory
   * @param n_features   the number of features in the dataset
   * @param parse        a parsing function
   * @param datastore    a container for the samples / external in-memory storage abstraction
   */
  template <typename Parse>  // e.g. std::function<Sample(boost::string_ref, int)>
  static void load(std::string url, int n_features, Parse parse, DataStore* datastore) {
    // 1. Connect to the data source, e.g. HDFS, via the modules in io
    // 2. Extract and parse lines
    // 3. Put samples into datastore
    // LineInputFormat infmt(url, num_threads_, id_, &coordinator_, worker_hostname_, hdfs_namenode_, hdfs_namenode_port_);
    // boost::string_ref& ref
    // while(infmt.next(ref)){
    //   Sample samples = parse(ref, n_features);
    //   datastore.push_back(samples);
    // }


  }

  // Datastore datastore;
  // int num_threads_;
  // int id_
  // Coordinator* coordinator_;
  // std::string worker_hostname_;
  // std::string hdfs_namenode_;
  // int hdfs_namenode_port_;

};  // class AbstractDataLoader

}  // namespace lib
}  // namespace csci5570
