#include "server/util/progress_tracker.hpp"

#include <fstream>
#include "glog/logging.h"

namespace csci5570 {

void ProgressTracker::Init(const std::vector<uint32_t>& tids) {
  // TODO
  for (auto tid : tids) {
    progresses_[tid] = 0;
  }
  min_clock_ = 0;
}

int ProgressTracker::AdvanceAndGetChangedMinClock(int tid) {
  // TODO
  int rst;
  if (IsUniqueMin(tid)) {
    min_clock_++;
    rst = min_clock_;
  } else {
    rst = -1;
  }
  progresses_[tid]++;
  return rst;
}

int ProgressTracker::GetNumThreads() const {
  // TODO
  return progresses_.size();
}

int ProgressTracker::GetProgress(int tid) const {
  // TODO
  return progresses_.find(tid)->second;
}

int ProgressTracker::GetMinClock() const {
  // TODO
  return min_clock_;
}

bool ProgressTracker::IsUniqueMin(int tid) const {
  // TODO
  for (auto pair : progresses_) {
    if (pair.second == min_clock_ && pair.first != tid)
      return false;
  }
  return true;
}

bool ProgressTracker::CheckThreadValid(int tid) const {
  // TODO
  return progresses_.find(tid) != progresses_.end();
}

void ProgressTracker::Backup(int model_id) const {
  std::ofstream outfile("/data/tracker.dat");
  outfile << model_id << " " << min_clock_ << "\n";
  outfile.close();
}

// void ProgressTracker::Backup(int model_id) const {
//   std::ofstream outfile("/data/tracker.dat");
//   outfile << model_id << " " << min_clock_ << "\n";
//   outfile.close();
// }

}  // namespace csci5570
