// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <cstdio>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) 
  {
    for (size_t i = 0; i < files_[level].size(); i++) 
    {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) 
      {
        delete f;
      }
    }
  }
}

// find the file whose largest key >= key
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) 
  {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } 
    else 
    {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) 
  { 
    // files are not sorted so that each file has to be searched
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) 
    {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) 
      {
        // No overlap
      } 
      else 
      {
        return true;  // Overlap
      }
    }
    return false;
  }


  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) 
  {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override 
  {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override 
  {
    assert(Valid());
    index_++;
  }
  void Prev() override 
  {
    assert(Valid());
    if (index_ == 0) 
    {
      index_ = flist_->size();  // Marks as invalid
    } 
    else 
    {
      index_--;
    }
  }
  Slice key() const override 
  {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }

  Slice value() const override 
  {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->file_num);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) 
{
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) 
  {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  }
  else
  {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, files_[0][i]->file_num, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace 
{
  enum SaverState 
  {
    kNotFound,
    kFound,
    kDeleted,
    kCorrupt,
  };
  struct Saver 
  {
    SaverState saver_state;
    const Comparator* ucmp;
    Slice user_key;
    std::string* value;
  };
}  // namespace

static void SaveValue(void* arg, const Slice& ikey, const Slice& v) 
{
  Saver* saver = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) 
  {
    saver->saver_state = kCorrupt;
  } 
  else 
  {
    if (saver->ucmp->Compare(parsed_key.user_key, saver->user_key) == 0) 
    {
      saver->saver_state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (saver->saver_state == kFound) 
      {
        saver->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) 
{
  return a->file_num > b->file_num;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) 
{
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) 
  {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) 
    {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) 
  {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) 
    {
      if (!(*func)(arg, 0, tmp[i])) 
      {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) 
  {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) 
    {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) 
      {
        // All of "f" is past any data for user_key
      } 
      else 
      {
        if (!(*func)(arg, level, f)) 
        {
          return;
        }
      }
    }
  }
}

// if found, string* value will be filled with result
Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* get_stats) {
  
  get_stats->seeked_file = nullptr;
  get_stats->seek_file_level = -1;

  struct State 
  {
    Saver saver;
    GetStats* get_stats;
    const ReadOptions* options;
    Slice ikey;
    FileMetaData* last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status status;
    bool found;

    // void *arg is state, which contains saver, GetStats, ikey, etc.
    static bool Match(void* arg, int level, FileMetaData* f) 
    {
      State* state = reinterpret_cast<State*>(arg);

      if (state->get_stats->seeked_file == nullptr &&
          state->last_file_read != nullptr) 
      {
        // We have had more than one seek for this read.  Charge the 1st file.
        state->get_stats->seeked_file = state->last_file_read;
        state->get_stats->seek_file_level = state->last_file_read_level;
      }

      state->last_file_read = f;
      state->last_file_read_level = level;

      state->status = state->vset->table_cache_->Get(*state->options, f->file_num,
                                                f->file_size, state->ikey,
                                                &state->saver, SaveValue);
      if (!state->status.ok()) 
      {
        state->found = true;
        return false;
      }
      switch (state->saver.saver_state) 
      {
        case kNotFound:
          return true;  // returning true means keeping searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->status =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;
  state.get_stats = get_stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.saver_state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;

  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.status : Status::NotFound(Slice());
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seeked_file;
  if (f != nullptr) 
  {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_seekcompact_ == nullptr) 
    {
      file_to_seekcompact_ = f;
      seekcompact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seeked_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) 
    {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) 
      {
        break;
      }
      if (level + 2 < config::kNumLevels) 
      {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) 
        {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end].
// Here overlapping means in the terms of user keys which exclude sequence number
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) 
{
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_key_begin, user_key_end;
  if (begin != nullptr) 
  {
    user_key_begin = begin->user_key();
  }
  if (end != nullptr) 
  {
    user_key_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size();) 
  {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_key_begin) < 0) 
    {
      // "f" is completely before specified range; skip it
    } 
    else if (end != nullptr && user_cmp->Compare(file_start, user_key_end) > 0) 
    {
      // "f" is completely after specified range; skip it
    }
    else 
    {
      inputs->push_back(f);
      if (level == 0) 
      {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_key_begin) < 0) 
        {
          user_key_begin = file_start;
          inputs->clear();
          i = 0;
        } 
        else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_key_end) > 0) 
        {
          user_key_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->file_num);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder 
{
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey 
  {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const 
    {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) 
      {
        return (r < 0);
      } 
      else 
      {
        // Break ties by file number
        return (f1->file_num < f2->file_num);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState 
  {
    std::set<uint64_t> files_to_delete;
    FileSet* files_to_add;
  };

  VersionSet* vset_;
  Version* base_version_;
  LevelState level_states_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_version_(base) 
  {
    base_version_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) 
    {
      level_states_[level].files_to_add = new FileSet(cmp);
    }
  }

  ~Builder() 
  {
    for (int level = 0; level < config::kNumLevels; level++) 
    {
      const FileSet* added = level_states_[level].files_to_add;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) 
      {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) 
      {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) 
        {
          delete f;
        }
      }
    }
    base_version_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void GatherAlterInfo(VersionEdit* edit) 
  {
    // gather info about the files to delete
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) 
    {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      level_states_[level].files_to_delete.insert(number);
    }

    // gather info about the files to add
    for (size_t i = 0; i < edit->new_files_.size(); i++) 
    {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;
      
      level_states_[level].files_to_add->insert(f);
    }
  }

  void PrintAddDeleteFiles()
  {
    for(int level=0;level<config::kNumLevels;level++)
    {
      const std::set<uint64_t> &files_to_delete =level_states_[level].files_to_delete;
      const FileSet &files_to_add=*(level_states_[level].files_to_add);
      printf("----------------level %d----------------\n",level);
      printf("files to delete:");
      for(auto &file_num:files_to_delete)
      {
        printf(" %lu",file_num);
      }
      printf("\n");

      printf("files to add:");
      for(auto &f_meta:files_to_add)
      {
        printf(" %lu",f_meta->file_num);
      }
      printf("\n");
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) 
  {
    for(int level=0;level<config::kNumLevels;level++)
    {
      std::vector<FileMetaData*> &files=v->files_[level];
      const std::vector<FileMetaData*> &original_files=base_version_->files_[level];
      const FileSet &files_to_add=*(level_states_[level].files_to_add);
      const std::set<uint64_t> &files_to_delete =level_states_[level].files_to_delete;
      auto original_files_iter=original_files.begin();
      auto added_files_iter=files_to_add.begin();
      while (true)
      {
        while (original_files_iter!=original_files.end())
        {
          
          uint64_t file_num=(*original_files_iter)->file_num;
          if(files_to_delete.count(file_num)==0) // find the first file metadata from original files that is not in the delete list
            break;
          original_files_iter++;
        }

        if(original_files_iter==original_files.end() && added_files_iter==files_to_add.end())
        {
          break;
        }

        FileMetaData *fmeta_to_save=nullptr;
        if(original_files_iter!=original_files.end() && added_files_iter==files_to_add.end())
        {
          fmeta_to_save=*original_files_iter;
          original_files_iter++;
        }
        else if(original_files_iter==original_files.end() && added_files_iter!=files_to_add.end())
        {
          fmeta_to_save=*added_files_iter;
          added_files_iter++;
        }
        else // both iters not reach the end yet
        {
          if(vset_->icmp_.Compare((*original_files_iter)->largest,(*added_files_iter)->smallest)<0)
          {
            fmeta_to_save=*original_files_iter;
            original_files_iter++;
          }
          else
          {
            fmeta_to_save=*added_files_iter;
            added_files_iter++;
          }
        }

        assert(fmeta_to_save!=nullptr);
        if (level > 0 && !files.empty()) 
        {
          assert(vset_->icmp_.Compare(files[files.size() - 1]->largest,
                                      fmeta_to_save->smallest) < 0);
        }
        fmeta_to_save->refs++;
        v->files_[level].push_back(fmeta_to_save);
      }
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* icmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*icmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      manifest_file_(nullptr),
      manifest_writer_(nullptr),
      dummy_versions_(this),
      current_version_(nullptr) 
{
  AppendVersion(new Version(this)); // append an empty version to list
}

VersionSet::~VersionSet() {
  current_version_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete manifest_writer_;
  delete manifest_file_;
}

// insert the version, unref the previous current_version_, ref the inserted version
void VersionSet::AppendVersion(Version* v)
{
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_version_);
  if (current_version_ != nullptr) 
  {
    current_version_->Unref();
  }
  current_version_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::RenameAppendingFiles(VersionEdit *edit)
{
  Status s;
  for(auto &task:edit->rename_tasks_)
  {
    uint64_t &original_file_num=task.first;
    uint64_t &target_file_num=task.second;
    assert(original_file_num!=0);
    std::string original_fname=TableFileName(dbname_,original_file_num);
    std::string target_fname=TableFileName(dbname_,target_file_num);
    s=env_->RenameFile(original_fname,target_fname);
    if(!s.ok())
      return s;
  }
  return Status::OK();
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) 
{
  if (edit->has_log_number_) 
  {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } 
  else 
  {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) 
  {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_version_);
    builder.GatherAlterInfo(edit);
    builder.SaveTo(v);
  }
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file_fname;
  Status s;
  if (manifest_writer_ == nullptr) 
  {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(manifest_file_ == nullptr);
    new_manifest_file_fname = ManifestFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file_fname, &manifest_file_);
    if (s.ok()) 
    {
      manifest_writer_ = new log::Writer(manifest_file_);
      s = WriteSnapshot(manifest_writer_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write edit to MANIFEST log
    if (s.ok()) 
    {
      std::string record;
      edit->EncodeTo(&record);
      s = manifest_writer_->AddRecord(record);
      if (s.ok()) 
      {
        s = manifest_file_->Sync();
      }
      if (!s.ok()) 
      {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file_fname.empty()) 
    {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }
  
  if(s.ok())
  {
    s=RenameAppendingFiles(edit);
  }
  // Install the new version
  if (s.ok()) 
  {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } 
  else 
  {
    delete v;
    if (!new_manifest_file_fname.empty()) 
    {
      delete manifest_writer_;
      delete manifest_file_;
      manifest_writer_ = nullptr;
      manifest_file_ = nullptr;
      env_->RemoveFile(new_manifest_file_fname);
    }
  }
  return s;
}

Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_version_);
  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.GatherAlterInfo(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(manifest_file_ == nullptr);
  assert(manifest_writer_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &manifest_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(manifest_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  manifest_writer_ = new log::Writer(manifest_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

// set the compaction_level_ and the compaction_score_ for version v
void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels - 1; level++)
  {
    double score;
    if (level == 0) 
    {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      score = v->files_[level].size() /
              static_cast<double>(config::kL0_CompactionTrigger);
    } 
    else 
    {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->sizecompaction_level_ = best_level;
  v->sizecompaction_score_ = best_score;
}

// write fmetas from each level(stored by current_version_) to log
Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) 
  {
    const std::vector<FileMetaData*>& files = current_version_->files_[level];
    for (size_t i = 0; i < files.size(); i++) 
    {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->file_num, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_version_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files numbers each level[ %d %d %d %d %d %d %d ]",
      int(current_version_->files_[0].size()), int(current_version_->files_[1].size()),
      int(current_version_->files_[2].size()), int(current_version_->files_[3].size()),
      int(current_version_->files_[4].size()), int(current_version_->files_[5].size()),
      int(current_version_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->file_num, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) 
{
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) 
  {
    for (int level = 0; level < config::kNumLevels; level++) 
    {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) 
      {
        live->insert(files[i]->file_num);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_version_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_version_->files_[level].size(); i++) {
      const FileMetaData* f = current_version_->files_[level][i];
      current_version_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRangeFromTwoInputs(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}


Iterator* VersionSet::MakeInputIteratorFromInput0(Compaction *compaction)
{
  ReadOptions options;
  options.verify_checksums=options_->paranoid_checks;
  options.fill_cache=false;

  const int num=compaction->inputs_[0].size();
  Iterator **iterator_list=new Iterator*[num];
  
  if(!compaction->inputs_[0].empty())
  {
    const std::vector<FileMetaData*>& files=compaction->inputs_[0];
    for(size_t i=0;i<files.size();i++)
    {
      iterator_list[i]=table_cache_->NewIterator(options,files[i]->file_num,
        files[i]->file_size);
    }
  }
  Iterator *result=NewMergingIterator(&icmp_,iterator_list,num);
  delete[] iterator_list;
  return result;
}

Compaction* VersionSet::PickCompaction() 
{
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_version_->sizecompaction_score_ >= 1);
  const bool seek_compaction = (current_version_->file_to_seekcompact_ != nullptr);
  if (size_compaction) 
  {
    level = current_version_->sizecompaction_level_;
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // Pick the largest file from each level

    FileMetaData* f_to_compact=nullptr;
    uint64_t largest_file_size=0;
    for (size_t i = 0; i < current_version_->files_[level].size(); i++) 
    {
      FileMetaData* f = current_version_->files_[level][i];
      if(f->file_size>largest_file_size)
      {
        f_to_compact=f;
        largest_file_size=f->file_size;
      }
    }
    assert(f_to_compact!=nullptr);
    c->inputs_[0].push_back(f_to_compact);
  } 
  else if (seek_compaction) 
  {
    level = current_version_->seekcompact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_version_->file_to_seekcompact_);
  } 
  else 
  {
    return nullptr;
  }

  c->input_version_ = current_version_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) 
  {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest); // get the range of inputs_[0] and store to smallest and largest
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_version_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]); // Store in "*inputs" all files in "level" that overlap [begin,end]
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  return c;
}

// Finds the largest key in a vector of files. Returns false if files are empty
bool FindLargestInternalKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// find the file f whose smallest has the same user key as largest_internal_key and its smallest has a larger sequence number
// among those files, choose the one with the minimum f->smallest
// An internal key includes key, sequence number and value type while a user key only includes the key
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_internal_key) 
{
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) 
  {
    FileMetaData* f = level_files[i];
    if (icmp.Compare(f->smallest, largest_internal_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_internal_key.user_key()) == 0) 
        // if f->smallest and largest_internal_key have the same userkey but f->smallest has a larger sequence number
    {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) 
          // find the file with a smaller f->smallest
      {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) 
{
  InternalKey largest_internal_key;

  // Quick return if compaction_files is empty.
  if (!FindLargestInternalKey(icmp, *compaction_files, &largest_internal_key)) 
  {
    return;
  }

  // largest_key has been found in FindLargestKey
  bool continue_searching = true;
  while (continue_searching) 
  {
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_internal_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) 
    {
      compaction_files->push_back(smallest_boundary_file);
      largest_internal_key = smallest_boundary_file->largest;
    } 
    else 
    {
      continue_searching = false;
    }
  }
}

// set up c->inputs_[1] and possibly grow c->inputs_[0] without 
// changing the number of inputs in c->inputs_[1]
void VersionSet::SetupOtherInputs(Compaction* c) 
{
  const int level = c->level();
  InternalKey smallest, largest;

  AddBoundaryInputs(icmp_, current_version_->files_[level], &c->inputs_[0]);
  GetRange(c->inputs_[0], &smallest, &largest);

  current_version_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);

  // Get entire range covered by compaction from two inputs/levels
  InternalKey all_start, all_limit;
  GetRangeFromTwoInputs(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) 
  {
    std::vector<FileMetaData*> expanded0;
    current_version_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_version_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) 
    {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_version_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) 
      {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRangeFromTwoInputs(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }
}

Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) 
{
  std::vector<FileMetaData*> inputs;
  current_version_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) 
  {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) 
  {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) 
    {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) 
      {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  // set up the first level for compaction i.e. set up c->inputs_[0]
  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_version_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
  for(Section* &section:sections_)
  {
    delete section;
  }
}

void Compaction::print_compact_info()
{
  for(int i=0;i<2;i++)
  {
    for(int j=0;j<inputs_[i].size();j++)
    {
      FileMetaData *f_meta=inputs_[i][j];
      printf("level %d; file number %lu; file size %lu;\n",level_+i,
        f_meta->file_num,f_meta->file_size);
    }
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) 
{
  for (int which = 0; which < 2; which++) 
  {
    // file metadata in input_[1] also needs to be deleted
    // since we need to update them
    for (size_t i = 0; i < inputs_[which].size(); i++) 
    {
      edit->RemoveFile(level_ + which, inputs_[which][i]->file_num);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) 
{
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) 
  {
    if (seen_key_) 
    {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) 
  {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  }
  else 
  {
    return false;
  }
}


std::vector<Section*>* Compaction::GenerateSections(Env* const env,port::Mutex *mutex,const std::string &dbname, std::set<uint64_t> *pending_outputs,
        const Options &options, VersionSet *versions)
{
  std::vector<FileMetaData*> &files=inputs_[1];
  if(files.empty())
  {
    Section* section=new Section(env,mutex,dbname,pending_outputs,options,versions);
    sections_.push_back(section);
    return &sections_;
  }
  
  for(int i=0;i<files.size();i++)
  {
    Section* section=new Section(files[i],env,mutex,dbname,pending_outputs,options,versions);
    if((i+1)<files.size())
    {
      Slice user_key=files[i+1]->smallest.user_key();
      std::string upper_bound;
      upper_bound.assign(user_key.data(),user_key.size());
      AppendFixed64(&upper_bound,(kMaxSequenceNumber<<8)|kTypeValue);// set the sequence number to maximum
      section->section_upper_bound_.DecodeFrom(upper_bound);
      section->inf_upper_boundary_=false;
      assert(section->appending_upper_bound_.user_key().compare(section->section_upper_bound_.user_key())<0);
    }
    sections_.push_back(section);
  }
  return &sections_;
}

void Compaction::AbandonGoingTasks()
{
  for(auto &section:sections_)
  {
    section->ClearAppendingTask();
    section->ClearBuildingTask();
  }
}


void Compaction::ReleaseInputs() 
{
  if (input_version_ != nullptr) 
  {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
