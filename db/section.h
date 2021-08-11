#ifndef STORAGE_LEVELDB_DB_SECTION_H_
#define STORAGE_LEVELDB_DB_SECTION_H_

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "db/version_set.h"

#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/table_appender.h"



namespace leveldb
{

    struct CompactionState;

    struct AppendingTask
    {
        AppendingTask(const uint64_t f_num):
            file_num(f_num),
            table_appender(nullptr),
            outfile(nullptr)
        {

        }

        ~AppendingTask()
        {
            if(table_appender!=nullptr)
            {
            delete table_appender;
            table_appender=nullptr;
            }

            if(outfile!=nullptr)
            {
            delete outfile;
            outfile=nullptr;
            }
        }

        uint64_t file_num;
        TableAppender *table_appender;
        RandomReWrFile *outfile;
    };

    struct BuildingTask
    {
        BuildingTask(const uint64_t f_num):
            file_num(f_num),
            table_builder(nullptr),
            outfile(nullptr)
        {

        }

        ~BuildingTask()
        {
            if(table_builder!=nullptr)
            {
            delete table_builder;
            table_builder=nullptr;
            }

            if(outfile!=nullptr)
            {
            delete outfile;
            outfile=nullptr;
            }
        }
        
        uint file_num;
        TableBuilder *table_builder;
        WritableFile *outfile;
    };

    class Section
    {
        public:
            Section(Env* const env,port::Mutex *mutex,const std::string &dbname,std::set<uint64_t> *pending_outputs,
                            const Options &options,VersionSet *versions);
            Section(const FileMetaData* f_meta,Env* const env,port::Mutex *mutex,const std::string &dbname,std::set<uint64_t> *pending_outputs,
                            const Options &options,VersionSet *versions);


            bool HasInfUpperBoundary()
            {
                return inf_upper_boundary_;
            }

            InternalKey& SectionUpperBound()
            {
                return section_upper_bound_;
            }

            void Insert(const Slice &key,const Slice &value);
            // finish writting buffered data into different files
            Status Finish(CompactionState *compact_state, Iterator* input_iterator);
        private:
            friend class Compaction;

            struct KeyValueBuffer
            {
                KeyValueBuffer():
                    total_bytes_buffered(0),
                    invalid_bytes(0),
                    items_num_written_(0)
                {

                }

                ~KeyValueBuffer()
                {
                    for(auto iter=keys.begin();iter!=keys.end();iter++)
                        delete *iter;
                }

                void Insert(const Slice &key_to_insert, const Slice &value_to_insert)
                {   
                    std::string *key=new std::string();
                    key->assign(key_to_insert.data(),key_to_insert.size());
                    keys.push_back(key);
                    
                    values.push_back(value_to_insert);

                    total_bytes_buffered+=key_to_insert.size();
                    total_bytes_buffered+=value_to_insert.size();
                }
                
                uint64_t NumBufferedItems()
                {
                    return keys.size();
                }

                uint64_t BytesLeft()
                {
                    return total_bytes_buffered-invalid_bytes;
                }

                std::vector<std::string *> keys;
                std::vector<Slice> values;
                uint64_t total_bytes_buffered;
                uint64_t items_num_written_;
                uint64_t invalid_bytes;
            };


            void ClearAppendingTask();
            void ClearBuildingTask();

            Status StartAppendingTask(CompactionState *compact_state);
            Status StartBuildingTask(CompactionState *compact_state);
            Status FinishAppendingTask(CompactionState *compact_state, Iterator *input_iterator);
            Status FinishBuildingTask(CompactionState *compact_state, Iterator *input_iterator);
            

            uint64_t appending_file_num_;
            uint64_t appending_file_original_size_;

            InternalKey fmeta_smallest_;
            InternalKey appending_upper_bound_;
            InternalKey section_upper_bound_;
            bool inf_upper_boundary_;

            AppendingTask *appending_task_;
            BuildingTask *building_task_;

            KeyValueBuffer buffer_;

            //Assistant variables
            Env* const env_;
            std::string dbname_;
            port::Mutex *mutex_; // mutex from DBimpl
            std::set<uint64_t> *pending_outputs_; // pending outputs from DBImpl
            VersionSet *versions_;
            const Options options_;

    };
}

#endif