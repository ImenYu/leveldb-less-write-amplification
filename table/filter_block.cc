#include "table/filter_block.h"
#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb
{
    static const size_t kKeyNumThreshold=20;

    FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy):policy_(policy){}

    // For a filter block, the key to add should get rid of sequence number and value type
    void FilterBlockBuilder::AddKey(const Slice& key)
    {
        flattened_keys_offsets_.push_back(flattened_keys_.size());
        flattened_keys_.append(key.data(),key.size());
        if(flattened_keys_offsets_.size()>=kKeyNumThreshold)
        {
            GenerateFilter();
        }
    }

    Slice FilterBlockBuilder::Finish()
    {
        if(!flattened_keys_offsets_.empty())
        {
            GenerateFilter();
        }
        const uint32_t array_offset=result_.size();
        for(size_t i=0;i<filter_offsets_.size();i++)
        {
            AppendFixed32(&result_,filter_offsets_[i]);
        }
        AppendFixed32(&result_,array_offset);
        result_.push_back(kKeyNumThreshold);
        return Slice(result_);
    }

    void FilterBlockBuilder::GenerateFilter()
    {
        const size_t num_keys=flattened_keys_offsets_.size();
        if(num_keys==0)
        {
            filter_offsets_.push_back(result_.size());
            return;
        }

        flattened_keys_offsets_.push_back(flattened_keys_.size());// to simplify length computation
        tmp_keys_.resize(num_keys);
        for(size_t i=0;i<num_keys;i++)
        {
            const char * base=flattened_keys_.data()+flattened_keys_offsets_[i];
            size_t length=flattened_keys_offsets_[i+1]-flattened_keys_offsets_[i];
            tmp_keys_[i]=Slice(base,length);
        }

        filter_offsets_.push_back(result_.size());
        policy_->CreateFilter(&tmp_keys_[0],static_cast<int>(num_keys),&result_);

        tmp_keys_.clear();
        flattened_keys_.clear();
        flattened_keys_offsets_.clear();
    }

    FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,const Slice& contents):
        policy_(policy),data_(nullptr),filter_array_limit_(nullptr),filter_num_(0),key_num_threshold_(0)
    {
        size_t n=contents.size();
        if(n<5)
            return;
        key_num_threshold_=contents[n-1];
        uint32_t filter_array_len=DecodeFixed32(contents.data()+n-5);
        if(filter_array_len>n-5)
            return;
        data_=contents.data();
        filter_array_limit_=data_+filter_array_len;
        filter_num_=(n-5-filter_array_len)/4;
    }

    bool FilterBlockReader::KeyMayMatch(uint64_t key_index_num, const Slice& key)
    {
        uint64_t filter_index=key_index_num/key_num_threshold_;
        if(filter_index<filter_num_)
        {
            uint32_t start=DecodeFixed32(filter_array_limit_+filter_index*4);
            uint32_t limit=DecodeFixed32(filter_array_limit_+filter_index*4+4);
            if(start<limit && limit<=static_cast<size_t>(filter_array_limit_-data_))
            {
                Slice filter=Slice(data_+start,limit-start);
                return policy_->KeyMayMatch(key,filter);
            }
            else if(start==limit)
            {
                // Empty filters do not match any keys
                return false;
            }

        }
        return true;
    }
}