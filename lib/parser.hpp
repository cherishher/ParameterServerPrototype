#pragma once

#include "boost/utility/string_ref.hpp"

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
  static Sample parse_libsvm(boost::string_ref line) {
    // check the LibSVM format and complete the parsing
    // hints: you may use boost::tokenizer, std::strtok_r, std::stringstream or any method you like
    // so far we tried all the tree and found std::strtok_r is fastest :)
    Sample sample;

    char* dataptr(new char(line.size()));
    strncpy(dataptr, line.data(), line.size());
    char* token = strtok_r(dataptr, "\t:", &dataptr);

    int i = -1;
    int idx;
    float val;
    while (token != NULL) {
      if (i == 0) {
        idx = std::atoi(token) - 1;
        i = 1;
      } else if (i == 1) {
        val = std::atof(token);
        sample.first.push_back(std::make_pair(idx, val));
        i = 0;
      } else {
        sample.second = std::atof(token);
        i = 0;
      }
      // Next key/value pair
      token = strtok_r(NULL, "\t:", &dataptr);
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
