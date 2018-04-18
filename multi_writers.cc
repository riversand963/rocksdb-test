#include <atomic>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using std::cout;
using std::cerr;
using std::endl;
using std::flush;

using gflags::ParseCommandLineFlags;
using gflags::RegisterFlagValidator;
using gflags::SetUsageMessage;

DEFINE_int32(key_size, 10, "Key size");
DEFINE_int32(value_size, 100, "Value size");
DEFINE_string(db, "", "Use the db with the following name.");
DEFINE_bool(destroy_db, true, "Destroy existing DB before running the test");
DEFINE_int32(runtime_sec, 60, "How long are we running for, in seconds");

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

    DBOptions db_options;
    db_options.create_if_missing = true;
    //db_options.use_direct_io_for_flush_and_compaction = true;
    //db_options.bytes_per_sync = 1;
    db_options.use_fsync = false;

    ColumnFamilyOptions cf_options;
    Options options(db_options, cf_options);

    if (FLAGS_destroy_db) {
      DestroyDB(FLAGS_db, options);
    }

    DB* db;
    Status s = DB::Open(options, FLAGS_db, &db);
    if (!s.ok()) {
      cerr << "Cannot open database: " << s.ToString() << endl << endl;
      std::abort();
    }

    ColumnFamilyHandle *handle;
    s = db->CreateColumnFamily(cf_options, "cf1", &handle);
    if (!s.ok()) {
      cerr << "Cannot create column family: " << s.ToString() << endl << flush;
      std::abort();
    }
    delete db;
    db = nullptr;

    std::vector<ColumnFamilyDescriptor> cf_descs;
    cf_descs.push_back(ColumnFamilyDescriptor("default", cf_options));
    cf_descs.push_back(ColumnFamilyDescriptor("cf1", cf_options));

    std::vector<ColumnFamilyHandle*> handles;
    s = DB::Open(options, FLAGS_db, cf_descs, &handles, &db);
    if (!s.ok()) {
      cerr << "Cannot open database: " << s.ToString() << endl << flush;
      std::abort();
    }
    for (auto h : handles) {
      handles_.push_back(h);
    }
    db_.reset(db);
  }

  void WriteWithWALThreadFunc() {
    WriteOptions write_options;
    write_options.sync = false;
    write_options.disableWAL = false;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(1, 10000);
    FlushOptions flush_options;
    //flush_options.wait = true;
    while (!stop_.load(std::memory_order_relaxed)) {
      int r = dist(gen);
      std::string temp = std::to_string(r);
      Slice key(temp);
      Slice value(temp + "_value");
      Status s = db_->Put(write_options, handles_[0], key, value);
      if (s.ok()) {
        writes_with_wal_++;
      }
      s = db_->Flush(flush_options, handles_[0]);
      if (!s.ok()) {
        cerr << "Cannot flush memtable: " << s.ToString() << endl << flush;
        std::abort();
      }
    }
  }

  void WriteWithoutWALThreadFunc() {
    WriteOptions write_options;
    write_options.sync = false;
    write_options.disableWAL = true;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(1, 10000);
    while (!stop_.load(std::memory_order_relaxed)) {
      int r = dist(gen);
      std::string temp = std::to_string(r);
      Slice key(temp);
      Slice value(temp + "_vallue");
      Status s = db_->Put(write_options, handles_[1], key, value);
      if (s.ok()) {
        writes_without_wal_++;
      } else {
        cerr << "Cannot Put: " << s.ToString() << endl << flush;
        std::abort();
      }
    }
  }

  void MultiGetThreadFunc() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(1, 10000);
    ReadOptions read_options;
    while (!stop_.load(std::memory_order_relaxed)) {
      /*
      int r = dist(gen);
      std::string temp = std::to_string(r);
      std::vector<Slice> keys;
      keys.push_back(Slice(temp));
      std::vector<std::string> values;
      std::vector<ColumnFamilyHandle*> cfs;
      cfs.push_back(handles_[1]);
      std::vector<Status> status_vec = db_->MultiGet(read_options, cfs, keys, &values);
      for (auto s : status_vec) {
        if (!s.ok() && !s.IsNotFound()) {
          cerr << "MultiGet failed: " << s.ToString() << endl << flush;
          std::abort();
        }
      }
      */
      Options options = db_->GetOptions(handles_[1]);
      multigets_++;
    }
  }

  int Run() {
    writes_with_wal_.store(0, std::memory_order_relaxed);
    writes_without_wal_.store(0, std::memory_order_relaxed);
    multigets_.store(0, std::memory_order_relaxed);
    threads_.emplace_back([&]() { WriteWithWALThreadFunc(); });
    threads_.emplace_back([&]() { MultiGetThreadFunc(); });

    Env::Default()->SleepForMicroseconds(FLAGS_runtime_sec * 1000 * 1000);

    stop_.store(true, std::memory_order_relaxed);
    for (auto& t : threads_) {
      t.join();
    }
    threads_.clear();
    cout << writes_with_wal_ << " " << writes_without_wal_ << " " << multigets_ << " " << lock_acquired_ << endl << flush;
    return 0;
  }

private:
  std::atomic<bool> stop_;
  std::vector<std::thread> threads_;
  std::unique_ptr<DB> db_;
  std::vector<ColumnFamilyHandle*> handles_;
  std::atomic<long> writes_with_wal_;
  std::atomic<long> writes_without_wal_;
  std::atomic<long> multigets_;
  long lock_acquired_ = 0;
};
} /* namespace rocksdb */

int main(int argc, char* argv[]) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0])
      + " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);

  rocksdb::MultiWritersTest multi_writers;
  return multi_writers.Run();
}
