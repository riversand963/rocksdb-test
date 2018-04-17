#include <atomic>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using std::cout;
using std::cerr;
using std::endl;
using std::flush;

static std::string FLAGS_db;
static bool FLAGS_destroy_db = true;
static int FLAGS_runtime_sec = 10;

namespace rocksdb {
class MultiWritersTest {
public:
  explicit MultiWritersTest() : stop_(false) {
    if (FLAGS_db.empty()) {
      std::string default_db_path;
      Env::Default()->GetTestDirectory(&default_db_path);
      default_db_path += "/multi_writers";
      FLAGS_db = default_db_path;
    }
    Options options;
    if (FLAGS_destroy_db) {
      DestroyDB(FLAGS_db, options);
    }

    options.create_if_missing = true;
    options.write_buffer_size = 256 * 1024;

    DB* db;
    Status s = DB::Open(options, FLAGS_db, &db);
    if (!s.ok()) {
      cerr << "Cannot open database: " << s.ToString() << endl << flush;
      std::abort();
    }
    db_.reset(db);
  }

  void WriteWithWALThreadFunc() {
    while (!stop_.load(std::memory_order_relaxed)) {
      writes_with_wal_++;
    }
  }

  void WriteWithoutWALThreadFunc() {
    while (!stop_.load(std::memory_order_relaxed)) {
      writes_without_wal_++;
    }
  }

  int Run() {
    writes_with_wal_.store(0, std::memory_order_relaxed);
    writes_without_wal_.store(0, std::memory_order_relaxed);
    threads_.emplace_back([&]() { WriteWithWALThreadFunc(); });
    threads_.emplace_back([&]() { WriteWithoutWALThreadFunc(); });

    Env::Default()->SleepForMicroseconds(FLAGS_runtime_sec * 1000 * 1000);

    stop_.store(true, std::memory_order_relaxed);
    for (auto& t : threads_) {
      t.join();
    }
    threads_.clear();
    cout << writes_with_wal_ << " " << writes_without_wal_ << endl << flush;
    return 0;
  }

private:
  std::atomic<bool> stop_;
  std::vector<std::thread> threads_;
  std::unique_ptr<DB> db_;
  std::atomic<long> writes_with_wal_;
  std::atomic<long> writes_without_wal_;
};
} /* namespace rocksdb */

int main(int argc, char* argv[]) {
  rocksdb::MultiWritersTest multi_writers;
  return multi_writers.Run();
}
