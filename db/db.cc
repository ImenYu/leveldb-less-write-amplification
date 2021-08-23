#include "leveldb/db.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"
#include "db/db_impl.h"
#include "db/version_edit.h"
#include "db/memtable.h"
#include "db/filename.h"
#include "db/version_set.h"

// Default implementations of convenience methods that subclasses of DB
// can call if they wish

namespace leveldb
{
    DB::~DB() = default;

    Status DB::Open(const Options &options,const std::string& dbname,DB** dbptr)
    {
        *dbptr=nullptr;
        DBImpl *impl=new DBImpl(options,dbname);

        
        impl->mutex_.Lock();

        Status s=impl->env_->RemoveDir(dbname);
        if(s.ok())
        {
            printf("directory successfuly removed\n");
        }

        VersionEdit edit;

        bool save_manifest=false;
        s = impl->Recover(&edit, &save_manifest);
        if (s.ok() && impl->mem_ == nullptr) 
        {
            // Create new log and a corresponding memtable.
            uint64_t new_log_number = impl->versions_->NewFileNumber();
            WritableFile* lfile;
            s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                            &lfile);
            if (s.ok()) 
            {
            edit.SetLogNumber(new_log_number);
            impl->logfile_ = lfile;
            impl->logfile_number_ = new_log_number;
            impl->log_ = new log::Writer(lfile);
            impl->mem_ = new MemTable(impl->internal_comparator_);
            impl->mem_->Ref();
            }
        }

        if (s.ok() && save_manifest) 
        {
            edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
            edit.SetLogNumber(impl->logfile_number_);
            s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
        }

        if (s.ok()) 
        {
            impl->RemoveObsoleteFiles();
            impl->MaybeScheduleCompaction();
        }
        impl->mutex_.Unlock();
        if (s.ok()) 
        {
            assert(impl->mem_ != nullptr);
            *dbptr = impl;
        } 
        else 
        {
            delete impl;
        }
        return s;

        return Status().OK();
    }

} // namespace leveldb


