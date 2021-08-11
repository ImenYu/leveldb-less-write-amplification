#ifndef STORAGE_LEVELDB_UTIL_BLOOM_H
#define STORAGE_LEVELDB_UTIL_BLOOM_H

#include "leveldb/filter_policy.h"

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb
{
    class BloomFilterPolicy : public FilterPolicy 
    {
    public:
        explicit BloomFilterPolicy(int bits_per_key);
        const char* Name() const override;
        void CreateFilter(const Slice* keys, int n, std::string* dst) const override;
        bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override;
    private:
        size_t bits_per_key_;
        size_t k_;
    };

    const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);
    static uint32_t BloomHash(const Slice& key);
}

#endif