#include "server/util/progress_tracker.hpp"

#include "glog/logging.h"

namespace csci5570 {

void ProgressTracker::Init(const std::vector<uint32_t>& tids) {
  // TODO
  for (auto tid : tids) {
    progresses_[tid] = 0;
  }
  min_clock_ = 0;
  printf("WWWWWWWWWWWWWWEEEEEEEEEEE IIIIIIINNNNNNNIIIIIIITTTTTT progress tracker, min_clock is %d, progress_tracker_.GetMinClock() is %d\n", min_clock_, GetMinClock());
  
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
    if (pair.second == min_clock_ && pair.first != tid) return false;
  }
  return true;
}

bool ProgressTracker::CheckThreadValid(int tid) const {
  // TODO
  return progresses_.find(tid) != progresses_.end();
}

}  // namespace csci5570
