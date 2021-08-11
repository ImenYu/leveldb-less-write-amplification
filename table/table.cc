// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/table_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep 
{
  ~Rep() 
  {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t file_size, Table** table) 
{
  *table = nullptr;
  if (file_size < Footer::kEncodedLength) 
  {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(file_size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  // attention, ReadBlock could return a BlockContents, the data space of which is already freed
  // however, in most cases, BlockContents points to an allocated area in heap
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) 
  {
    opt.verify_checksums = true;
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) 
  {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (tbl_rep_->options.filter_policy == nullptr) 
  {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (tbl_rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(tbl_rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* metaindex_block = new Block(contents);

  Iterator* iter = metaindex_block->NewBlockIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(tbl_rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete metaindex_block;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (tbl_rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(tbl_rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    tbl_rep_->filter_data = block.data.data();  // Will need to delete later
  }
  tbl_rep_->filter = new FilterBlockReader(tbl_rep_->options.filter_policy, block.data);
}

Table::~Table() { delete tbl_rep_; }

static void DeleteBlock(void* arg, void* ignored) 
{
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) 
{
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) 
{
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

Iterator* Table::GetIndexBlockIterator() const
{
  Options &options=tbl_rep_->options;
  return tbl_rep_->index_block->NewBlockIterator(options.comparator);
}

// Convert an index_iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// static method
Iterator* Table::BlockReader(void* arg, const ReadOptions& read_options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->tbl_rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle block_handle;
  Slice input = index_value;
  Status s = block_handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) 
  {
    BlockContents contents;
    if (block_cache != nullptr) 
    {
      // try to find this block in the block cache.
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->tbl_rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, block_handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) // found in block cache
      {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } 
      else 
      {
        s = ReadBlock(table->tbl_rep_->file, read_options, block_handle, &contents);
        if (s.ok()) 
        {
          block = new Block(contents);
          if (contents.cachable && read_options.fill_cache) 
          {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } 
    else 
    {
      s = ReadBlock(table->tbl_rep_->file, read_options, block_handle, &contents);
      if (s.ok()) 
      {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) 
  {
    iter = block->NewBlockIterator(table->tbl_rep_->options.comparator);
    if (cache_handle == nullptr) 
    {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } 
    else 
    {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } 
  else 
  {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewTableIterator(const ReadOptions& read_options) const 
{
  return new TableIterator(const_cast<Table*>(this),read_options);
}

Status Table::InternalGet(const ReadOptions& read_options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) 
{
  Status s;
  Iterator* index_block_iterator = tbl_rep_->index_block->NewBlockIterator(tbl_rep_->options.comparator);
  index_block_iterator->Seek(k);
    
  if (index_block_iterator->Valid()) 
  {
    Slice handle_value = index_block_iterator->value();
    FilterBlockReader* filter = tbl_rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) 
    {
      // Not found
    } 
    else // read the block and try to search it
    {
      Iterator* data_block_iter = BlockReader(this, read_options, index_block_iterator->value());
      data_block_iter->Seek(k);
      if (data_block_iter->Valid()) 
      {
        // arg is the saver, this function saves value to saver
        (*handle_result)(arg, data_block_iter->key(), data_block_iter->value());
      }
      s = data_block_iter->status();
      delete data_block_iter;
    }
  }
  if (s.ok()) 
  {
    s = index_block_iterator->status();
  }
  delete index_block_iterator;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      tbl_rep_->index_block->NewBlockIterator(tbl_rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = tbl_rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = tbl_rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
