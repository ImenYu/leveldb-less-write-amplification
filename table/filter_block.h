#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_NEW_H
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_NEW_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"
namespace leveldb
{
    class FilterPolicy;

    // This filter block builder creates a filter for a fixed number of keys instead for each block
    class FilterBlockBuilder
    {
    public:
        explicit FilterBlockBuilder(const FilterPolicy*);

        FilterBlockBuilder(const FilterBlockBuilder&)=delete;
        FilterBlockBuilder& operator=(const FilterBlockBuilder&)=delete;

        void AddKey(const Slice& key);
        Slice Finish();

    private:
        void GenerateFilter();

        const FilterPolicy *policy_;
        std::string flattened_keys_;
        std::vector<size_t> flattened_keys_offsets_;
        std::string result_;
        std::vector<Slice> tmp_keys_;
        std::vector<uint32_t> filter_offsets_;
    };

    class FilterBlockReader
    {
    public:
        FilterBlockReader(const FilterPolicy *policy,const Slice& contents);
        bool KeyMayMatch(uint64_t key_index_num, const Slice& key);
    private:
        const FilterPolicy *policy_;
        const char* data_;
        const char* filter_array_limit_;
        size_t filter_num_;
        size_t key_num_threshold_;

    };
}

#endif