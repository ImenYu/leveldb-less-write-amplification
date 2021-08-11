#include "db/section.h"
#include "db/filename.h"

namespace leveldb
{
    Section::Section(Env *const env,port::Mutex *mutex,const std::string &dbname, std::set<uint64_t> *pending_outputs,
                        const Options &options,VersionSet *versions): // infinite range with no corresponding file for appending new data  
        appending_file_num_(0),
        appending_file_original_size_(0),
        inf_upper_boundary_(true),
        appending_task_(nullptr),
        building_task_(nullptr),
        env_(env),
        mutex_(mutex),
        dbname_(dbname),
        pending_outputs_(pending_outputs),
        options_(options),
        versions_(versions)
    {

    }

    Section::Section(const FileMetaData* f_meta,Env* const env,port::Mutex *mutex,const std::string &dbname,std::set<uint64_t> *pending_outputs,
                        const Options &options, VersionSet *versions):
        appending_file_num_(f_meta->file_num),
        appending_file_original_size_(f_meta->file_size),
        fmeta_smallest_(f_meta->smallest),
        inf_upper_boundary_(true),
        appending_task_(nullptr),
        building_task_(nullptr),
        env_(env),
        mutex_(mutex),
        dbname_(dbname),
        pending_outputs_(pending_outputs),
        options_(options),
        versions_(versions)
    {
        Slice user_key=f_meta->largest.user_key();
        std::string upper_bound;
        upper_bound.assign(user_key.data(),user_key.size());
        AppendFixed64(&upper_bound,(0<<8)|kTypeValue);
        appending_upper_bound_.DecodeFrom(upper_bound);
    }


    void Section::ClearAppendingTask()
    {
        if(appending_task_!=nullptr)
        {
            delete appending_task_;
            appending_task_=nullptr;
        }
        if(appending_file_num_!=0) // set to 0, so that no more appending task will be created
            appending_file_num_=0;
    }

    void Section::ClearBuildingTask()
    {
        if(building_task_!=nullptr)
        {
            delete building_task_;
            building_task_=nullptr;
        }
    }

    Status Section::StartAppendingTask(CompactionState *compact_state)
    {
        assert(appending_task_==nullptr);
        assert(appending_file_num_!=0);
        appending_task_=new AppendingTask(appending_file_num_);

        {
            mutex_->Lock();
            CompactionState::Output out;
            out.origin_file_num=appending_task_->file_num;
            out.target_file_num=versions_->NewFileNumber();
            out.smallest.Clear();
            out.largest.Clear();
            compact_state->outputs.push_back(out);
            pending_outputs_->insert(out.target_file_num);
            mutex_->Unlock();
        }

        std::string file_name=TableFileName(dbname_,appending_file_num_);
        Status s=env_->OpenFileAsRandomReWrFile(file_name,&appending_task_->outfile);

        if(s.ok())
        {
            s=TableAppender::Open(options_,
                appending_task_->outfile,
                appending_file_original_size_,
                &appending_task_->table_appender);
        }
        return s;
    }

    Status Section::StartBuildingTask(CompactionState *compact_state)
    {
        assert(compact_state!=nullptr);
        assert(building_task_==nullptr);

        uint64_t target_file_num;
        {
            mutex_->Lock();
            target_file_num=versions_->NewFileNumber();
            building_task_=new BuildingTask(target_file_num);

            CompactionState::Output out;
            out.origin_file_num=0;
            out.target_file_num=building_task_->file_num;
            out.smallest.Clear();
            out.largest.Clear();
            compact_state->outputs.push_back(out);
            pending_outputs_->insert(out.target_file_num);
            mutex_->Unlock();
        }

        std::string fname=TableFileName(dbname_,target_file_num);
        Status s=env_->NewWritableFile(fname,&building_task_->outfile);
        if(s.ok())
        {
            building_task_->table_builder=new TableBuilder(options_,building_task_->outfile);
        }
        return s;
    }

    Status Section::FinishAppendingTask(CompactionState *compact_state,Iterator *input_iterator)
    {
        assert(compact_state!=nullptr);
        assert(appending_task_!=nullptr);
        assert(appending_task_->table_appender!=nullptr);
        assert(appending_task_->outfile!=nullptr);
        assert(appending_task_->file_num==compact_state->current_output()->origin_file_num);

        const uint64_t num_appended_entries=appending_task_->table_appender->NumAppendedEntries();
        const uint64_t output_number=compact_state->current_output()->origin_file_num;
        assert(output_number!=0);

        Status s=input_iterator->status();
        if(s.ok())
        {
            s=appending_task_->table_appender->Finish();
        }
        else
        {
            appending_task_->table_appender->Abandon();
        }

        if(s.ok())
        {
            compact_state->current_output()->smallest=appending_task_->table_appender->GetSmallestKey();
            compact_state->current_output()->largest=appending_task_->table_appender->GetLargestKey();
        }

        const uint64_t current_file_size=appending_task_->table_appender->FileSize();
        compact_state->total_bytes_written+=appending_task_->table_appender->AppendedDataSize();
        compact_state->total_bytes_read_by_appenders+=appending_task_->table_appender->ReadBytes();
        compact_state->current_output()->final_file_size=current_file_size;

        if(s.ok())
        {
            s=appending_task_->outfile->Sync();
        }
        if(s.ok())
        {
            s=appending_task_->outfile->Close();
        }
        ClearAppendingTask();
        if(s.ok())
        {
            Log(options_.info_log, "Finished appending data to table #%llu@%d level(filename not changed yet): %lld keys appended, file size %lld bytes",
            (unsigned long long)output_number, compact_state->compaction->level(),
            (unsigned long long)num_appended_entries,
            (unsigned long long)current_file_size);
        }
        return s;
    }   

    Status Section::FinishBuildingTask(CompactionState *compact_state, Iterator *input_iterator)
    {
        assert(compact_state != nullptr);
        assert(building_task_!=nullptr);
        assert(building_task_->table_builder != nullptr);
        assert(building_task_->outfile!=nullptr);
        assert(building_task_->file_num==compact_state->current_output()->target_file_num);

        const uint64_t output_number = compact_state->current_output()->target_file_num;
        assert(output_number != 0);

        // check for iterator errors
        Status s=input_iterator->status();
        const uint64_t current_entries=building_task_->table_builder->NumEntries();
        if(s.ok())
        {
            s=building_task_->table_builder->Finish();
        }
        else
        {
            building_task_->table_builder->Abandon();
        }

        if(s.ok())
        {
            compact_state->current_output()->smallest=building_task_->table_builder->GetSmallestKey();
            compact_state->current_output()->largest=building_task_->table_builder->GetLargestKey();
        }

        const uint64_t current_bytes = building_task_->table_builder->FileSize();
        compact_state->current_output()->final_file_size = current_bytes;
        compact_state->total_bytes_written += current_bytes;

        // Finish and check for file errors
        if (s.ok()) 
        {
            s = building_task_->outfile->Sync();
        }
        if (s.ok()) 
        {
            s = building_task_->outfile->Close();
        }
        ClearBuildingTask();

        if (s.ok() && current_entries > 0) 
        {
            Log(options_.info_log, "Built table #%llu@%d: %lld keys, %lld bytes",
                (unsigned long long)output_number, compact_state->compaction->level(),
                (unsigned long long)current_entries,
                (unsigned long long)current_bytes);
        }
        return s;       
    } 



    void Section::Insert(const Slice &key,const Slice &value)
    {
        // assert(inf_upper_boundary_ || key.compare(section_upper_bound_.Encode())<0);
        buffer_.Insert(key,value);   
    }

    Status Section::Finish(CompactionState *compact_state,Iterator* input_iterator)
    {
        Status s=Status::OK();
        assert(appending_task_==nullptr && building_task_==nullptr);
        if(appending_file_num_!=0) // prioritize appending file
        {
            s=StartAppendingTask(compact_state);
            if(s.ok())
            {
                // insert until appending_upper_bound
                while (buffer_.items_num_written_<buffer_.NumBufferedItems())
                {
                    uint64_t &index=buffer_.items_num_written_;
                    std::string &key=*(buffer_.keys[index]);
                    Slice& value=buffer_.values[index];

                    if(options_.comparator->Compare(key,appending_upper_bound_.Encode())<0) // fill until appending_file_upper_bound_ 
                    {
                        appending_task_->table_appender->Append(key,value);
                        buffer_.items_num_written_++;
                        buffer_.invalid_bytes+=key.size();
                        buffer_.invalid_bytes+=value.size();
                    }
                    else
                    {
                        break;
                    }
                }
                // check the left key value pairs will not form a small file
                if(buffer_.BytesLeft()<compact_state->compaction->MaxOutputFileSize()/4)
                {
                    while (buffer_.items_num_written_<buffer_.NumBufferedItems())
                    {
                        uint64_t &index=buffer_.items_num_written_;
                        std::string &key=*(buffer_.keys[index]);
                        Slice& value=buffer_.values[index];

                        appending_task_->table_appender->Append(key,value);
                        buffer_.items_num_written_++;
                        buffer_.invalid_bytes+=key.size();
                        buffer_.invalid_bytes+=value.size();
                    }
                }
                s=FinishAppendingTask(compact_state,input_iterator);
            }
        }

        // start building tables
        while(s.ok() && buffer_.items_num_written_<buffer_.NumBufferedItems())
        {

            s=StartBuildingTask(compact_state);
            if(s.ok())
            {
                while (buffer_.items_num_written_<buffer_.NumBufferedItems())
                {
                    uint64_t &index=buffer_.items_num_written_;
                    std::string &key=*(buffer_.keys[index]);
                    Slice& value=buffer_.values[index];

                    building_task_->table_builder->Add(key,value);
                    buffer_.items_num_written_++;
                    buffer_.invalid_bytes+=key.size();
                    buffer_.invalid_bytes+=value.size();

                    if(building_task_->table_builder->FileSize() > compact_state->compaction->MaxOutputFileSize())
                        break;
                }
                s=FinishBuildingTask(compact_state,input_iterator);
            }
        }
        if(s.ok())
            assert(buffer_.BytesLeft()==0);
        return s;
    }
}