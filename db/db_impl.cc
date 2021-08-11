#include "db/filename.h"
#include "db/db_impl.h"

#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"

#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "db/memtable.h"
#include "db/log_reader.h"
#include "db/builder.h"
#include "db/section.h"
#include "table/table_appender.h"
#include "util/mutexlock.h"


namespace leveldb
{
    const int kNumNonTableCacheFiles = 10;

    // Information kept for every waiting writer
    struct DBImpl::Writer 
    {
        explicit Writer(port::Mutex* mu)
        : batch(nullptr), sync(false), done(false), cv(mu) {}

    Status status;
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;
    }; // struct DBImpl::Writer 

    struct DBImpl::ManualCompaction
    {
        int level;
        bool done;
        const InternalKey* begin;  // null means beginning of key range
        const InternalKey* end;    // null means end of key range
        InternalKey tmp_storage;   // Used to keep track of compaction progress
    }; // struct DBImpl::ManualCompaction

    // Fix user-supplied options to be reasonable
    template <class T, class V>
    static void ClipToRange(T* ptr, V minvalue, V maxvalue) 
    {
        if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
        if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
    }

    Options SanitizeOptions(const std::string& dbname,
                            const InternalKeyComparator* icmp,
                            const InternalFilterPolicy* ipolicy,
                            const Options& src) 
    {
        Options result = src;
        result.comparator = icmp;
        result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
        ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
        ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
        ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
        ClipToRange(&result.block_size, 1 << 10, 4 << 20);
        if (result.info_log == nullptr) {
            // Open a log file in the same directory as the db
            src.env->CreateDir(dbname);  // In case it does not exist
            src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
            Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
            if (!s.ok()) {
            // No place suitable for logging
            result.info_log = nullptr;
            }
        }
        if (result.block_cache == nullptr) 
        {
            result.block_cache = NewLRUCache(8 << 20);
        }
        return result;
    }

    static int TableCacheSize(const Options& sanitized_options) 
    {
        // Reserve ten files or so for other uses and give the rest to TableCache.
        return sanitized_options.max_open_files - kNumNonTableCacheFiles;
    }

    DBImpl::DBImpl(const Options& raw_options, const std::string& dbname):
        env_(raw_options.env),
        internal_comparator_(raw_options.comparator),
        internal_filter_policy_(raw_options.filter_policy), // here, internal filter policy wraps filter pollicy.
        options_(SanitizeOptions(dbname, &internal_comparator_,
                                &internal_filter_policy_, raw_options)),
        owns_info_log_(options_.info_log != raw_options.info_log),
        owns_cache_(options_.block_cache != raw_options.block_cache),
        dbname_(dbname),
        table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
        db_lock_(nullptr),
        shutting_down_(false),
        background_work_finished_signal_(&mutex_),
        mem_(nullptr),
        imm_(nullptr),
        has_imm_(false),
        logfile_(nullptr),
        logfile_number_(0),
        log_(nullptr),
        seed_(0),
        tmp_batch_(new WriteBatch),
        background_compaction_scheduled_(false),
        manual_compaction_(nullptr),
        versions_(new VersionSet(dbname_, &options_, table_cache_,
                                &internal_comparator_))
        {

        }
    
    DBImpl::~DBImpl()
    {
        // Wait for background work to finish.
        mutex_.Lock();
        while (background_compaction_scheduled_) 
        {
            background_work_finished_signal_.Wait();
        }
        shutting_down_.store(true, std::memory_order_release);

        mutex_.Unlock();

        Log(options_.info_log,"For flushing ImmTable time needed: %.2f; total bytes read: %lu; total bytes written: %lu\n", 
                static_cast<double>(flush_stats_.micros)/1000000.0,flush_stats_.bytes_read,flush_stats_.bytes_written);
        CompactionStats result;
        for(int i=0;i<config::kNumLevels;i++)
        {
            result.Add(stats_[i]);
        }
        Log(options_.info_log,"total time spend: %.2f; total bytes read: %lu; total bytes written: %lu; write amplification: %.2f\n", 
                static_cast<double>(result.micros)/1000000.0, result.bytes_read,result.bytes_written,
                static_cast<double>(result.bytes_written)/static_cast<double>(flush_stats_.bytes_written));

        if (db_lock_ != nullptr) 
        {
            env_->UnlockFile(db_lock_);
        }

        delete versions_;
        if (mem_ != nullptr) mem_->Unref();
        if (imm_ != nullptr) imm_->Unref();
        delete tmp_batch_;
        delete log_;
        delete logfile_;
        delete table_cache_;

        if (owns_info_log_) 
        {
            delete options_.info_log;
        }
        if (owns_cache_) 
        {
            delete options_.block_cache;
        }
    }

    Status DBImpl::Put(const WriteOptions& opt, const Slice& key, const Slice& value) 
    {
        WriteBatch batch;
        batch.Put(key, value);
        return Write(opt, &batch);
    }

    Status DBImpl::Delete(const WriteOptions& opt, const Slice& key) 
    {
        WriteBatch batch;
        batch.Delete(key);
        return Write(opt, &batch);
    }

    // write updates into MemTable
    Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) 
    {
        Writer w(&mutex_);
        w.batch = updates;
        w.sync = options.sync;
        w.done = false;

        MutexLock l(&mutex_); //automatically locks mutex_ and will automatically unlock it when exiting
        writers_.push_back(&w); // add this writer to the back of dequeue consisting of writers
        while (!w.done && &w != writers_.front()) { // waiting for w to reach the front of dequeue
            w.cv.Wait();
        }
        if (w.done) 
        { // w might be written in another thread since writes are done in batch
            return w.status;
        }

        // May temporarily unlock and wait.
        Status status = MakeRoomForWrite(updates == nullptr);
        uint64_t last_sequence = versions_->LastSequence();
        Writer* last_writer = &w;
        if (status.ok() && updates != nullptr) 
        {  // nullptr batch is for compactions
            WriteBatch* write_batch = BuildBatchGroup(&last_writer); // build the batchgroup from writers_.front() and set the last writer
            WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
            last_sequence += WriteBatchInternal::Count(write_batch);

            // Add to log and apply to memtable.  We can release the lock
            // during this phase since &w is currently responsible for logging
            // and protects against concurrent loggers and concurrent writes
            // into mem_.
            {
                mutex_.Unlock();
                status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
                bool sync_error = false;
                if (status.ok() && options.sync) 
                {
                    status = logfile_->Sync();
                    if (!status.ok()) 
                    {
                    sync_error = true;
                    }
                }
                if (status.ok()) 
                {
                    status = WriteBatchInternal::InsertInto(write_batch, mem_);
                }
                mutex_.Lock();
                if (sync_error) 
                {
                    // The state of the log file is indeterminate: the log record we
                    // just added may or may not show up when the DB is re-opened.
                    // So we force the DB into a mode where all future writes fail.
                    RecordBackgroundError(status);
                }
            }
            if (write_batch == tmp_batch_) tmp_batch_->Clear();

            versions_->SetLastSequence(last_sequence);
        }

        while (true) { // database writers before w should be written already.
            Writer* ready = writers_.front();
            writers_.pop_front();
            if (ready != &w) {
            ready->status = status;
            ready->done = true;
            ready->cv.Signal();
            }
            if (ready == last_writer) break;
        }

        // Notify new head of write queue
        if (!writers_.empty()) {
            writers_.front()->cv.Signal();
        }
        return status;
    }

    const Snapshot* DBImpl::GetSnapshot() 
    {
        MutexLock l(&mutex_);
        return snapshots_.New(versions_->LastSequence());
    }

    Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) 
    {
        Status s;
        MutexLock l(&mutex_);
        SequenceNumber snapshot;
        if (options.snapshot != nullptr) 
        {
            snapshot =
                static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
        }
        else 
        {
            snapshot = versions_->LastSequence();
        }

        MemTable* mem = mem_;
        MemTable* imm = imm_;
        mem->Ref();
        if (imm != nullptr) imm->Ref();

        bool have_stat_update = false;
        Version::GetStats stats;

        bool found=false;
        // Unlock while reading from files and memtables
        {
            mutex_.Unlock();
            // First look in the memtable, then in the immutable memtable (if any).
            LookupKey lkey(key, snapshot);
            if (mem->Get(lkey, value, &s)) 
            {
                found=true;
            } 
            else if (imm != nullptr && imm->Get(lkey, value, &s)) 
            {
                found=true;
            } 
            
            if(!found)
            {
                mutex_.Lock();
                Version* current_version = versions_->current();
                current_version->Ref();
                s = current_version->Get(options, lkey, value, &stats);
                have_stat_update = true;
                if (have_stat_update && current_version->UpdateStats(stats)) 
                {
                    MaybeScheduleCompaction();
                }
                current_version->Unref();
                mutex_.Unlock();
            }
            mutex_.Lock();
        }


        mem->Unref();
        if (imm != nullptr) imm->Unref();
        return s;
    }

    void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) 
    {
        MutexLock l(&mutex_);
        snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
    }

    Status DBImpl::NewDB() 
    {
        VersionEdit new_db;
        new_db.SetComparatorName(user_comparator()->Name());
        new_db.SetLogNumber(0);
        new_db.SetNextFile(2);
        new_db.SetLastSequence(0);

        const std::string manifest_fname = ManifestFileName(dbname_, 1);
        WritableFile* file;
        Status s = env_->NewWritableFile(manifest_fname, &file);
        if (!s.ok()) 
        {
            return s;
        }
        {
            log::Writer log(file);
            std::string record;
            new_db.EncodeTo(&record);
            s = log.AddRecord(record);
            if (s.ok()) 
            {
                s = file->Sync();
            }
            if (s.ok()) 
            {
                s = file->Close();
            }
        }
        delete file;
        if (s.ok()) 
        {
            // Make "CURRENT" file that points to the new manifest file.
            s = SetCurrentFile(env_, dbname_, 1);
        }
        else 
        {
            env_->RemoveFile(manifest_fname);
        }
        return s;
    }

    Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) 
    {
        mutex_.AssertHeld();

        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        env_->CreateDir(dbname_);
        assert(db_lock_ == nullptr);
        Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
        if (!s.ok()) 
        {
            return s;
        }

        if (!env_->FileExists(CurrentFileName(dbname_))) 
        {
            if (options_.create_if_missing) 
            {
                Log(options_.info_log, "Creating DB %s since it was missing.",
                    dbname_.c_str());
                s = NewDB();
                if (!s.ok()) 
                {
                    return s;
                }
            } 
            else 
            {
                return Status::InvalidArgument(
                    dbname_, "does not exist (create_if_missing is false)");
            }
        } 
        else 
        {
            if (options_.error_if_exists) 
            {
            return Status::InvalidArgument(dbname_,
                                            "exists (error_if_exists is true)");
            }
        }

        s = versions_->Recover(save_manifest);
        if (!s.ok()) 
        {
            return s;
        }
        SequenceNumber max_sequence(0);

        // Recover from all newer log files than the ones named in the
        // descriptor (new log files may have been added by the previous
        // incarnation without registering them in the descriptor).
        //
        // Note that PrevLogNumber() is no longer used, but we pay
        // attention to it in case we are recovering a database
        // produced by an older version of leveldb.
        const uint64_t min_log = versions_->LogNumber();
        const uint64_t prev_log = versions_->PrevLogNumber();
        std::vector<std::string> filenames;
        s = env_->GetChildren(dbname_, &filenames);
        if (!s.ok()) 
        {
            return s;
        }
        std::set<uint64_t> expected;
        versions_->AddLiveFiles(&expected);
        uint64_t number;
        FileType type;
        std::vector<uint64_t> logs;
        for (size_t i = 0; i < filenames.size(); i++) 
        {
            if (ParseFileName(filenames[i], &number, &type)) 
            {
                expected.erase(number);
                if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
                    logs.push_back(number);
            }
        }
        if (!expected.empty()) {
            char buf[50];
            std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                        static_cast<int>(expected.size()));
            return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
        }

        // Recover in the order in which the logs were generated
        std::sort(logs.begin(), logs.end());
        for (size_t i = 0; i < logs.size(); i++) {
            s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                            &max_sequence);
            if (!s.ok()) {
            return s;
            }

            // The previous incarnation may not have written any MANIFEST
            // records after allocating this log number.  So we manually
            // update the file number allocation counter in VersionSet.
            versions_->MarkFileNumberUsed(logs[i]);
        }

        if (versions_->LastSequence() < max_sequence) 
        {
            versions_->SetLastSequence(max_sequence);
        }

        return Status::OK();
    }

    Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) 
    {
    struct LogReporter : public log::Reader::Reporter {
        Env* env;
        Logger* info_log;
        const char* fname;
        Status* status;  // null if options_.paranoid_checks==false
        void Corruption(size_t bytes, const Status& s) override {
        Log(info_log, "%s%s: dropping %d bytes; %s",
            (this->status == nullptr ? "(ignoring error) " : ""), fname,
            static_cast<int>(bytes), s.ToString().c_str());
        if (this->status != nullptr && this->status->ok()) *this->status = s;
        }
    };

    mutex_.AssertHeld();

    // Open the log file
    std::string fname = LogFileName(dbname_, log_number);
    SequentialFile* file;
    Status status = env_->NewSequentialFile(fname, &file);
    if (!status.ok()) {
        MaybeIgnoreError(&status);
        return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.fname = fname.c_str();
    reporter.status = (options_.paranoid_checks ? &status : nullptr);
    // We intentionally make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
    Log(options_.info_log, "Recovering log #%llu",
        (unsigned long long)log_number);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    int compactions = 0;
    MemTable* mem = nullptr;
    while (reader.ReadRecord(&record, &scratch) && status.ok()) {
        if (record.size() < 12) {
        reporter.Corruption(record.size(),
                            Status::Corruption("log record too small"));
        continue;
        }
        WriteBatchInternal::SetContents(&batch, record);

        if (mem == nullptr) {
        mem = new MemTable(internal_comparator_);
        mem->Ref();
        }
        status = WriteBatchInternal::InsertInto(&batch, mem);
        MaybeIgnoreError(&status);
        if (!status.ok()) {
        break;
        }
        const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                        WriteBatchInternal::Count(&batch) - 1;
        if (last_seq > *max_sequence) {
        *max_sequence = last_seq;
        }

        if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
        compactions++;
        *save_manifest = true;
        status = WriteLevel0Table(mem, edit, nullptr);
        mem->Unref();
        mem = nullptr;
        if (!status.ok()) {
            // Reflect errors immediately so that conditions like full
            // file-systems cause the DB::Open() to fail.
            break;
        }
        }
    }

    delete file;

    // See if we should keep reusing the last log file.
    if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
        assert(logfile_ == nullptr);
        assert(log_ == nullptr);
        assert(mem_ == nullptr);
        uint64_t lfile_size;
        if (env_->GetFileSize(fname, &lfile_size).ok() &&
            env_->NewAppendableFile(fname, &logfile_).ok()) {
        Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
        log_ = new log::Writer(logfile_, lfile_size);
        logfile_number_ = log_number;
        if (mem != nullptr) {
            mem_ = mem;
            mem = nullptr;
        } else {
            // mem can be nullptr if lognum exists but was empty.
            mem_ = new MemTable(internal_comparator_);
            mem_->Ref();
        }
        }
    }

    if (mem != nullptr) {
        // mem did not get reused; compact it.
        if (status.ok()) {
        *save_manifest = true;
        status = WriteLevel0Table(mem, edit, nullptr);
        }
        mem->Unref();
    }

    return status;
    }

    void DBImpl::RemoveObsoleteFiles() 
    {
        mutex_.AssertHeld();

        if (!bg_error_.ok()) 
        {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }

        // Make a set of all of the live files
        std::set<uint64_t> live = pending_outputs_;
        versions_->AddLiveFiles(&live);

        std::vector<std::string> filenames;
        env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
        uint64_t number;
        FileType type;
        std::vector<std::string> files_to_delete;
        for (std::string& filename : filenames) 
        {
            if (ParseFileName(filename, &number, &type)) 
            {
                bool keep = true;
                switch (type) {
                    case kLogFile:
                        keep = ((number >= versions_->LogNumber()) ||
                                (number == versions_->PrevLogNumber()));
                        break;
                    case kDescriptorFile:
                        // Keep my manifest file, and any newer incarnations'
                        // (in case there is a race that allows other incarnations)
                        keep = (number >= versions_->ManifestFileNumber());
                        break;
                    case kTableFile:
                        keep = (live.find(number) != live.end());
                        break;
                    case kTempFile:
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs_, which is inserted into "live"
                        keep = (live.find(number) != live.end());
                        break;
                    case kCurrentFile:
                    case kDBLockFile:
                    case kInfoLogFile:
                        keep = true;
                    break;
                }

                if (!keep) 
                {
                    files_to_delete.push_back(std::move(filename));
                    if (type == kTableFile) 
                    {
                        table_cache_->Evict(number);
                    }
                    Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
                        static_cast<unsigned long long>(number));
                }
            }
        }

        // While deleting all files unblock other threads. All files being deleted
        // have unique names which will not collide with newly created files and
        // are therefore safe to delete while allowing other threads to proceed.
        mutex_.Unlock();
        for (const std::string& filename : files_to_delete) 
        {
            env_->RemoveFile(dbname_ + "/" + filename);
        }
        mutex_.Lock();
    }


    // used to flush the Immutable Memtable
    // try to flush to level 2 if possible
    Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                    Version* base) 
    {
        mutex_.AssertHeld();
        const uint64_t start_micros = env_->NowMicros();
        FileMetaData f_meta;
        f_meta.file_num = versions_->NewFileNumber();
        pending_outputs_.insert(f_meta.file_num);
        Iterator* iter = mem->NewIterator();
        Log(options_.info_log, "Level-0 table #%llu: started",
            (unsigned long long)f_meta.file_num);

        Status s;
        {
            mutex_.Unlock();
            s = BuildTable(dbname_, env_, options_, table_cache_, iter, &f_meta);
            mutex_.Lock();
        }

        Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
            (unsigned long long)f_meta.file_num, (unsigned long long)f_meta.file_size,
            s.ToString().c_str());
        delete iter;
        pending_outputs_.erase(f_meta.file_num);

        // Note that if file_size is zero, the file has been deleted and
        // should not be added to the manifest.
        int level = 0;
        if (s.ok() && f_meta.file_size > 0) 
        {
            const Slice min_user_key = f_meta.smallest.user_key();
            const Slice max_user_key = f_meta.largest.user_key();
            if (base != nullptr) 
            {
                level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
            }
            edit->AddFile(level, f_meta.file_num, f_meta.file_size, f_meta.smallest,
                        f_meta.largest);
        }

        CompactionStats stats;
        stats.micros = env_->NowMicros() - start_micros;
        stats.bytes_written = f_meta.file_size;
        stats_[level].Add(stats);
        // for general statistics
        flush_stats_.Add(stats);
        return s;
    }

    void DBImpl::MaybeIgnoreError(Status* s) const 
    {
        if (s->ok() || options_.paranoid_checks) 
        {
            // No change needed
        }
        else 
        {
            Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
            *s = Status::OK();
        }
    }

    void DBImpl::RecordBackgroundError(const Status& s) 
    {
        mutex_.AssertHeld();
        if (bg_error_.ok()) 
        {
            bg_error_ = s;
            background_work_finished_signal_.SignalAll();
        }
    }

    // REQUIRES: Writer list must be non-empty
    // REQUIRES: First writer must have a non-null batch
    // build the batch group from writers_.front() and set last_writer
    WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) 
    {
        mutex_.AssertHeld();
        assert(!writers_.empty());
        Writer* first = writers_.front();
        WriteBatch* result = first->batch;
        assert(result != nullptr);

        size_t size = WriteBatchInternal::ByteSize(first->batch);

        // Allow the group to grow up to a maximum size, but if the
        // original write is small, limit the growth so we do not slow
        // down the small write too much.
        size_t max_size = 1 << 20;
        if (size <= (128 << 10)) {
            max_size = size + (128 << 10);
        }

        *last_writer = first;
        std::deque<Writer*>::iterator iter = writers_.begin();
        ++iter;  // Advance past "first"
        for (; iter != writers_.end(); ++iter) {
            Writer* w = *iter;
            if (w->sync && !first->sync) {
            // Do not include a sync write into a batch handled by a non-sync write.
            break;
            }

            if (w->batch != nullptr) {
            size += WriteBatchInternal::ByteSize(w->batch);
            if (size > max_size) {
                // Do not make batch too big
                break;
            }

            // Append to *result
            if (result == first->batch) {
                // Switch to temporary batch instead of disturbing caller's batch
                result = tmp_batch_;
                assert(WriteBatchInternal::Count(result) == 0);
                WriteBatchInternal::Append(result, first->batch);
            }
            WriteBatchInternal::Append(result, w->batch);
            }
            *last_writer = w;
        }
        return result;
    }

    // REQUIRES: mutex_ is held
    // REQUIRES: this thread is currently at the front of the writer queue
    Status DBImpl::MakeRoomForWrite(bool force) 
    {
        mutex_.AssertHeld();
        assert(!writers_.empty());
        bool allow_delay = !force;
        Status s;
        while (true) 
        {
            if (!bg_error_.ok()) 
            {
            // Yield previous error
            s = bg_error_;
            break;
            } 
            else if (allow_delay && versions_->NumLevelFiles(0) >=
                                        config::kL0_SlowdownWritesTrigger) 
            {
            // We are getting close to hitting a hard limit on the number of
            // L0 files.  Rather than delaying a single write by several
            // seconds when we hit the hard limit, start delaying each
            // individual write by 1ms to reduce latency variance.  Also,
            // this delay hands over some CPU to the compaction thread in
            // case it is sharing the same core as the writer.
            mutex_.Unlock();
            env_->SleepForMicroseconds(1000);
            allow_delay = false;  // Do not delay a single write more than once
            mutex_.Lock();
            }
            else if (!force &&
                    (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) 
            {
                // There is room in current memtable
                break;
            }
            else if (imm_ != nullptr) 
            {
                // We have filled up the current memtable, but the previous
                // one is still being compacted, so we wait.
                Log(options_.info_log, "Current memtable full; waiting...\n");
                background_work_finished_signal_.Wait();
            } 
            else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) 
            {
                // There are too many level-0 files.
                Log(options_.info_log, "Too many L0 files; waiting...\n");
                background_work_finished_signal_.Wait();
            } 
            else 
            {
                // Attempt to switch to a new memtable and trigger compaction of old
                assert(versions_->PrevLogNumber() == 0);
                uint64_t new_log_number = versions_->NewFileNumber();
                WritableFile* lfile = nullptr;
                s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
                if (!s.ok()) 
                {
                    // Avoid chewing through file number space in a tight loop.
                    versions_->ReuseFileNumber(new_log_number);
                    break;
                }
                delete log_;
                delete logfile_;
                logfile_ = lfile;
                logfile_number_ = new_log_number;
                log_ = new log::Writer(lfile);
                imm_ = mem_;
                has_imm_.store(true, std::memory_order_release);
                mem_ = new MemTable(internal_comparator_);
                mem_->Ref();
                force = false;  // Do not force another compaction if have room
                MaybeScheduleCompaction();
            }
        }
        return s;
    }

    void DBImpl::MaybeScheduleCompaction() 
    {
        mutex_.AssertHeld();
        if (background_compaction_scheduled_) 
        {
            // Already scheduled
        } 
        else if (shutting_down_.load(std::memory_order_acquire)) 
        {
            // DB is being deleted; no more background compactions
        } 
        else if (!bg_error_.ok()) 
        {
            // Already got an error; no more changes
        } 
        else if (imm_ == nullptr && manual_compaction_ == nullptr &&
                    !versions_->NeedsCompaction()) 
        {
            // No work to be done
        }
        else 
        {
            background_compaction_scheduled_ = true;
            env_->Schedule(&DBImpl::BGWork, this);
        }
    }

    void DBImpl::BGWork(void* db) 
    {
        reinterpret_cast<DBImpl*>(db)->BackgroundCall();
    }

    void DBImpl::BackgroundCall() 
    {
        MutexLock l(&mutex_);
        assert(background_compaction_scheduled_);
        if (shutting_down_.load(std::memory_order_acquire)) 
        {
            // No more background work when shutting down.
        } 
        else if (!bg_error_.ok()) 
        {
            // No more background work after a background error.
        } 
        else 
        {
            BackgroundCompaction();
        }

        background_compaction_scheduled_ = false;

        // Previous compaction may have produced too many files in a level,
        // so reschedule another compaction if needed.
        MaybeScheduleCompaction();
        background_work_finished_signal_.SignalAll();
    }

    // manual compaction: similar to normal compaction, but select SSTables based on user's intructions first
    // and then select the rest SSTables as normal compaction does
    // normal compaction: compact SSTables in a level with the SSTables in the next level
    void DBImpl::BackgroundCompaction() 
    {
        mutex_.AssertHeld();
        if (imm_ != nullptr) 
        {
            CompactMemTable();
            return;
        }
        
        Compaction *c;
        bool is_manual_compaction=(manual_compaction_!=nullptr);
        InternalKey manual_end;
        if(is_manual_compaction)
        {
            // TODO: in this case, this section is never triggered
        }
        else
        {
            c=versions_->PickCompaction(); // here set c's input_version_ to versions->current_version_ and ref it
        }

        Status status;
        if(c==nullptr)
        {
            // do nothing
        }
        else if (!is_manual_compaction && c->IsTrivialMove()) // trivial compaction: just move a file to a higher level
        { 
            // Move file to next level
            assert(c->num_input_files(0) == 1);
            FileMetaData* f = c->input(0, 0);
            c->edit()->RemoveFile(c->level(), f->file_num);
            c->edit()->AddFile(c->level() + 1, f->file_num, f->file_size, f->smallest,
                            f->largest);
            status = versions_->LogAndApply(c->edit(), &mutex_);
            if (!status.ok()) 
            {
                RecordBackgroundError(status);
            }
            VersionSet::LevelSummaryStorage tmp;
            Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
                static_cast<unsigned long long>(f->file_num), c->level() + 1,
                static_cast<unsigned long long>(f->file_size),
                status.ToString().c_str(), versions_->LevelSummary(&tmp));
        } 
        else // nonetrival compaction
        {
            CompactionState *compaction_state=new CompactionState(c);
            status=DoCompactionWork(compaction_state);
            if (!status.ok()) 
            {
                RecordBackgroundError(status);
            }
            CleanupCompaction(compaction_state);
            c->ReleaseInputs(); // here unref c's input_version_
            RemoveObsoleteFiles();
        }
        delete c;
        c=nullptr;

        if (status.ok()) 
        {
            // Done
        } 
        else if (shutting_down_.load(std::memory_order_acquire)) 
        {
            // Ignore compaction errors found during shutting down
        }
        else 
        {
            Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
        }
    }

    // remove finished files from pending_outputs_
    void DBImpl::CleanupCompaction(CompactionState* compact_state)
    {
        mutex_.AssertHeld();
        compact_state->compaction->AbandonGoingTasks();
        for (size_t i = 0; i < compact_state->outputs.size(); i++) 
        {
            const CompactionState::Output& out = compact_state->outputs[i];
            pending_outputs_.erase(out.target_file_num);
        }
        delete compact_state;
    }

    void DBImpl::CompactMemTable() 
    {
        mutex_.AssertHeld();
        assert(imm_ != nullptr);

        // Save the contents of the memtable as a new Table
        VersionEdit edit;
        Version* base = versions_->current();
        base->Ref();
        Status s = WriteLevel0Table(imm_, &edit, base);
        base->Unref();

        if (s.ok() && shutting_down_.load(std::memory_order_acquire)) 
        {
            s = Status::IOError("Deleting DB during memtable compaction");
        }

        // Replace immutable memtable with the generated Table
        if (s.ok()) 
        {
            edit.SetPrevLogNumber(0);
            edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
            s = versions_->LogAndApply(&edit, &mutex_);
        }

        if (s.ok()) 
        {
            // Commit to the new state
            imm_->Unref();
            imm_ = nullptr;
            has_imm_.store(false, std::memory_order_release);
            RemoveObsoleteFiles();
        }
        else 
        {
            RecordBackgroundError(s);
        }
    }

    Status DBImpl::DoCompactionWork(CompactionState* compact_state)
    {
        mutex_.AssertHeld();
        const uint64_t start_micros = env_->NowMicros();
        int64_t imm_micros = 0;  // Micros spent doing imm_ compactions
        Compaction *c=compact_state->compaction;

        Log(options_.info_log, "Compacting %d@%d + %d@%d files",
            compact_state->compaction->num_input_files(0), compact_state->compaction->level(),
            compact_state->compaction->num_input_files(1),
            compact_state->compaction->level() + 1);

        assert(versions_->NumLevelFiles(compact_state->compaction->level()) > 0);

        if (snapshots_.empty()) 
        {
            compact_state->smallest_snapshot = versions_->LastSequence();
        } 
        else 
        {
            compact_state->smallest_snapshot = snapshots_.oldest()->sequence_number();
        }

        std::vector<Section*> *sections=compact_state->compaction->GenerateSections(env_,&mutex_,dbname_,&pending_outputs_,
            options_,versions_);
        auto sec_iter=sections->begin();

        Status status=Status::OK();
        ParsedInternalKey parsed_current_internal_key;
        std::string shared_user_key;
        bool has_shared_user_key=false;
        SequenceNumber drop_controlling_sequence=kMaxSequenceNumber;

        mutex_.Unlock();
        Iterator *input_iterator=versions_->MakeInputIteratorFromInput0(c);
        input_iterator->SeekToFirst();

        while (input_iterator->Valid() && !shutting_down_.load(std::memory_order_acquire))
        {
            // Prioritize immutable compaction work
            // During compaction, it will check if the immutable memtable exists, if exists, 
            // it will try to flush immutable memtable first.
            if (has_imm_.load(std::memory_order_relaxed)) 
            {
                const uint64_t imm_start = env_->NowMicros(); // start time
                mutex_.Lock();
                if (imm_ != nullptr) 
                {
                    CompactMemTable();
                    // Wake up MakeRoomForWrite() if necessary.
                    background_work_finished_signal_.SignalAll();
                }
                mutex_.Unlock();
                imm_micros += (env_->NowMicros() - imm_start);
            }

            Slice current_internal_key=input_iterator->key();
            bool should_the_current_internal_key_be_dropped=false;                
            if(!ParseInternalKey(current_internal_key,&parsed_current_internal_key))
            {
                shared_user_key.clear();
                has_shared_user_key=false;
                drop_controlling_sequence=kMaxSequenceNumber;
            }
            else // current internal key is successfully parsed and stored in parsed_internal_key
            {
                if(!has_shared_user_key||
                    user_comparator()->Compare(parsed_current_internal_key.user_key,shared_user_key)!=0)
                {
                    shared_user_key.assign(parsed_current_internal_key.user_key.data(),
                        parsed_current_internal_key.user_key.size());
                    has_shared_user_key=true;
                    drop_controlling_sequence=kMaxSequenceNumber; // newly met user key should not be dropped. 
                }

                if(drop_controlling_sequence<=compact_state->smallest_snapshot)
                {
                    // the first met user key will not be dropped since its sequence is set to kMaxSequenceNumber
                    // Since the keys are ordered in increasing user key, decreasing sequence, decreasing type,
                    // the following keys with the same user key has sequence less than the first one.
                    // if the first one's sequence is less than smallest_snapshot, following keys with the same sequence will be dropped
                    should_the_current_internal_key_be_dropped=true; 
                }
                else if(parsed_current_internal_key.type==kTypeDeletion &&
                            parsed_current_internal_key.sequence<=compact_state->smallest_snapshot &&
                            compact_state->compaction->IsBaseLevelForKey(parsed_current_internal_key.user_key))
                {
                    // For this user key:
                    // (1) there is no data in higher levels
                    // (2) data in lower levels will have larger sequence numbers
                    // (3) data in layers that are being compacted here and have
                    //     smaller sequence numbers will be dropped in the next
                    //     few iterations of this loop (by rule (A) above).
                    // Therefore this deletion marker is obsolete and can be dropped.
                    should_the_current_internal_key_be_dropped=true;
                }
                drop_controlling_sequence=parsed_current_internal_key.sequence;
            }

            if(!should_the_current_internal_key_be_dropped)
            {
                while (sec_iter!=sections->end())
                {
                    // sec_iter->section_upper_bound's sequence number is set to maximum sequence number
                    if((*sec_iter)->HasInfUpperBoundary()||
                        internal_comparator_.Compare(input_iterator->key(),(*sec_iter)->SectionUpperBound().Encode())<0)
                    {
                        (*sec_iter)->Insert(input_iterator->key(),input_iterator->value());
                        break;
                    }
                    (*sec_iter)->Finish(compact_state,input_iterator);
                    sec_iter++;
                }
            }
            input_iterator->Next();
        }

        if(status.ok() && shutting_down_.load(std::memory_order_acquire))
        {
            status = Status::IOError("Deleting DB during compaction");
        }

        if(status.ok() && sec_iter!=sections->end())
        {
            status=(*sec_iter)->Finish(compact_state,input_iterator);
        }
          
        if (status.ok()) 
        {
            status = input_iterator->status();
        }
        delete input_iterator;
        input_iterator = nullptr;

        // do statistics
        CompactionStats stats;
        stats.micros=env_->NowMicros()-start_micros-imm_micros;
        for(int i=0;i<compact_state->compaction->num_input_files(0);i++)
        {
            stats.bytes_read+=compact_state->compaction->input(0,i)->file_size;
        }
        stats.bytes_read+=compact_state->total_bytes_read_by_appenders;
        stats.bytes_written += compact_state->total_bytes_written;

        mutex_.Lock();
        stats_[compact_state->compaction->level()+1].Add(stats);

        if(status.ok())
        {
            status=InstallCompactionResults(compact_state);
        }
        if (!status.ok()) 
        {
            RecordBackgroundError(status);
        }
        VersionSet::LevelSummaryStorage tmp;
        Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
        return status;
    }

    Status DBImpl::InstallCompactionResults(CompactionState* compact_state)
    {
        mutex_.AssertHeld();
        Log(options_.info_log, "Compacted %d files @ %d level + %d files @ %d level => %lld bytes",
            compact_state->compaction->num_input_files(0), compact_state->compaction->level(),
            compact_state->compaction->num_input_files(1), compact_state->compaction->level() + 1,
            static_cast<long long>(compact_state->total_bytes_written));
        
        compact_state->compaction->AddInputDeletions(compact_state->compaction->edit()); // add useless files to edit
        const int level=compact_state->compaction->level();
        for(int i=0;i<compact_state->outputs.size();i++)
        {
            const CompactionState::Output& out = compact_state->outputs[i];
            compact_state->compaction->edit()->AddFile(level + 1, out.target_file_num, out.final_file_size,
                                                        out.smallest, out.largest);
            if(out.origin_file_num!=0)
            {
                compact_state->compaction->edit()->AddRenameTasks(out.origin_file_num,out.target_file_num);
            }
        }
        return versions_->LogAndApply(compact_state->compaction->edit(), &mutex_);
    }

    Status DestroyDB(const std::string& dbname, const Options& options) 
    {
    Env* env = options.env;
    std::vector<std::string> filenames;
    Status result = env->GetChildren(dbname, &filenames);
    if (!result.ok()) {
        // Ignore error in case directory does not exist
        return Status::OK();
    }

    FileLock* lock;
    const std::string lockname = LockFileName(dbname);
    result = env->LockFile(lockname, &lock);
    if (result.ok()) {
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type) &&
            type != kDBLockFile) {  // Lock file will be deleted at end
            Status del = env->RemoveFile(dbname + "/" + filenames[i]);
            if (result.ok() && !del.ok()) {
            result = del;
            }
        }
        }
        env->UnlockFile(lock);  // Ignore error since state is already gone
        env->RemoveFile(lockname);
        env->RemoveDir(dbname);  // Ignore error in case dir contains other files
    }
    return result;
    }

} // namespace leveldb