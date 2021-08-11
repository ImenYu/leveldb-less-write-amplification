#ifndef STORAGE_LEVELDB_TABLE_TABLE_APPENDER_H_
#define STORAGE_LEVELDB_TABLE_TABLE_APPENDER_H_

#include "leveldb/export.h"
#include "leveldb/status.h"
#include "leveldb/options.h"
#include "leveldb/table.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "table/block_builder.h"
#include "db/dbformat.h"
#include <map>


// This is used to append data to an already existing table.


namespace leveldb
{ 
    class TableAppender
    {
    public:
        

        static Status Open(const Options& options,RandomReWrFile *file,uint64_t file_size,TableAppender **table_appender);


        TableAppender(const TableAppender&)=delete;
        TableAppender& operator=(const TableAppender&)=delete;
        ~TableAppender();

        void Append(const Slice& key,const Slice &value);
        // flush data block
        void Flush();
        Status Finish();
        void Abandon();

        
        Status status() const;

        void PrintOriginalIndexBlock();
        uint64_t OriginalFileSize();
        uint64_t NumAppendedEntries();
        uint64_t FileSize();
        uint64_t ReadBytes();
        uint64_t AppendedDataSize();
        InternalKey GetSmallestKey();
        InternalKey GetLargestKey();
        

    private:
        struct Rep;
        struct InternalKeyCompare;

        explicit TableAppender(Rep* rep);
        // get the original data block limit and set file write position
        Status Initialize();
        bool ok();
        void AppendBlockToFile(const Slice& raw, BlockHandle *block_handle);
        void AppendRawBlockToFile(const Slice& block_contents,
                            CompressionType compression_type, BlockHandle* handle);
        Rep* rep_;

    };

}
#endif
