// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      tbl_file_cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete tbl_file_cache_; }

// returns OK and set the handle if successfully opened the file
// If successful, <key, FileAndTable> will be inserted into the cache
// private method
Status TableCache::FindAndOpenTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) 
{
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = tbl_file_cache_->Lookup(key);
  if (*handle == nullptr) // if not in the cache
  {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) 
    {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) 
      {
        s = Status::OK();
      }
    }

    if (s.ok()) 
    {
      s = Table::Open(options_, file, file_size, &table);
    }

    // if fail to open the file
    if (!s.ok()) 
    {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } 
    else // if succeed to open the file
    {
      TableAndFile* tbl_and_file = new TableAndFile;
      tbl_and_file->file = file;
      tbl_and_file->table = table;
      *handle = tbl_file_cache_->Insert(key, tbl_and_file, 1, &DeleteEntry); // handle is referenced twice, one in here, one in the hash table lying within tbl_file_cache_. DeleteEntry is the deleter function
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) 
{
  if (tableptr != nullptr) 
  {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindAndOpenTable(file_number, file_size, &handle);
  if (!s.ok())
  {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(tbl_file_cache_->Value(handle))->table;
  Iterator* result = table->NewTableIterator(options);
  result->RegisterCleanup(&UnrefEntry, tbl_file_cache_, handle); // if this iterator is deleted, its corresponding handle's ref will be unreferenced
  if (tableptr != nullptr) 
  {
    *tableptr = table;
  }
  return result;
}

// handle_result here saves the found value to a target
Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) 
{
  Cache::Handle* handle = nullptr;
  Status s = FindAndOpenTable(file_number, file_size, &handle);
  if (s.ok()) 
  {
    Table* t = reinterpret_cast<TableAndFile*>(tbl_file_cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result);
    tbl_file_cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) 
{
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  tbl_file_cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
