#pragma once

#include "boost/utility/string_ref.hpp"
#include "glog/logging.h"

namespace csci5570 {
namespace lib {

template <typename Sample, typename DataStore>
class Parser {
 public:
  /**
   * Parsing logic for one line in file
   *
   * @param line    a line read from the input file
   */
  static Sample parse_libsvm(boost::string_ref line, int n_features) {
    // check the LibSVM format and complete the parsing
    // hints: you may use boost::tokenizer, std::strtok_r, std::stringstream or any method you like
    // so far we tried all the tree and found std::strtok_r is fastest :)
    Sample sample;
    char* pos;

    std::unique_ptr<char> dataptr(new char[line.size() + 1]);
    strncpy(dataptr.get(), line.data(), line.size());
    dataptr.get()[line.size()] = '\0';
    char* token = strtok_r(dataptr.get(), " \t:", &pos);

    int i = -1;
    int key;
    float val;
    int feature_num = 0;
    while (true) {
      if (i == 0) {
        key = std::atoi(token) - 1;
        if (key > feature_num) {
          for (int j = feature_num; j < key; j++) {
            sample.x_.push_back(std::make_pair(j, 0));
          }
          feature_num = key;
        }
        i = 1;
      } else if (i == 1) {
        val = std::atof(token);
        sample.x_.push_back(std::make_pair(feature_num, val));
        feature_num++;
        i = 0;
      } else {
        sample.y_ = std::atof(token);
        i = 0;
      }
      // Next key/value pair
      token = strtok_r(NULL, " \t:", &pos);
      if(token == NULL){
        for(int j = feature_num; j < n_features ; j++){
          sample.x_.push_back(std::make_pair(j, 0));          
        }
        break;
      }
    }

    return sample;
  }

  static Sample parse_mnist(boost::string_ref line, int n_features) {
    // check the MNIST format and complete the parsing
  }

  // You may implement other parsing logic

};  // class Parser

}  // namespace lib
}  // namespace csci5570
