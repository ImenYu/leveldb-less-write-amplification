#ifndef STORAGE_LEVELDB_INCLUDE_HYBRID_TABLE_BUILDER_H_
#define STORAGE_LEVELDB_INCLUDE_HYBRID_TABLE_BUILDER_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb
{
    class BlockBuilder;
    class BlockHandle;
    class WritableFile;

    class InternalKey;

    class LEVELDB_EXPORT TableBuilder
    {
    public:
        TableBuilder(const Options &options, WritableFile *file);
        TableBuilder(const TableBuilder&)=delete;
        TableBuilder& operator=(const TableBuilder&)=delete;
        ~TableBuilder();

        Status ChangeOptions(const Options& options);
        void Add(const Slice& key, const Slice& value);
        InternalKey GetSmallestKey();
        InternalKey GetLargestKey();
        void Flush();
        Status status() const;
        Status Finish();
        void Abandon();
        uint64_t NumEntries() const;
        uint64_t FileSize() const;
    private:
        bool ok();
        void AppendBlockToFile(const Slice& raw, BlockHandle *block_handle);
        void AppendRawBlockToFile(const Slice &data, CompressionType, BlockHandle *block_handle);

        struct Rep;
        Rep* rep_;
    };
} // leveldb

#endif