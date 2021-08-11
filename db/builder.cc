// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// Used to build table

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb 
{

// build SSTable from an iterator, an usual case is building a table from a MemTable iterator
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* file_meta_data) 
{
  Status s;
  file_meta_data->file_size = 0;
  iter->SeekToFirst();
  
  std::string fname = TableFileName(dbname, file_meta_data->file_num);
  if (iter->Valid()) 
  {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) 
    {
      return s;
    }
    TableBuilder* builder = new TableBuilder(options, file);
    file_meta_data->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) 
    {
      key = iter->key();
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      file_meta_data->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) 
    {
      file_meta_data->file_size = builder->FileSize();
      assert(file_meta_data->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) 
    {
      s = file->Sync();
    }
    if (s.ok()) 
    {
      s = file->Close(); 
    }
    delete file;
    file = nullptr;

    if (s.ok()) 
    {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), file_meta_data->file_num,
                                              file_meta_data->file_size);
      s = it->status();
      delete it;
    }
  } // iter->valid()

  // Check for input iterator errors
  if (!iter->status().ok()) 
  {
    s = iter->status();
  }

  if (s.ok() && file_meta_data->file_size > 0) 
  {
    // Keep it
  } else 
  {
    env->RemoveFile(fname);
  }
  return s;
}



}  // namespace leveldb
