#include "lib/data_loader.hpp"
#include "lib/svm_sample.hpp"
#include <algorithm>

namespace csci5570{
  template<typename Sample>
  class BatchIterator {
  public:
    BatchIterator(const std::vector<Sample>& data_store){
      datastore=data_store;
    }
    std::pair<std::vector<uint32_t>,std::vector<Sample>> NextBatch(uint32_t size){
      std::vector<Sample> data;
      std::vector<uint32_t> keys;
      for(int i=0;i<size;i++){
        auto sample=datastore[rand()%datastore.size()];
        data.push_back(sample);
        auto& x=sample.x_;
        for (auto& field : x) {
          int key=field.first;
          std::vector<uint32_t>::iterator result = find(keys.begin(),keys.end(),key);
          if(result==keys.end()){
            keys.push_back(key);
          }
        }
        int n=keys.size();
        std::sort(keys.begin(),keys.end());
      }
      return make_pair(keys,data);
    }
    
  private:
    std::vector<Sample> datastore;
  };
}
