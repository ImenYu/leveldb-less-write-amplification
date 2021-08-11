#include "table/table_iterator.h"

namespace leveldb
{
    TableIterator::TableIterator(Table *table,ReadOptions read_options):
        table_(table),
        index_block_iter_(table->GetIndexBlockIterator()),
        current_data_block_iter_(nullptr),
        read_options_(read_options),
        status_(Status::OK())
    {

    }

    TableIterator::~TableIterator()
    {
        delete index_block_iter_;
        index_block_iter_=nullptr;
        if(current_data_block_iter_!=nullptr)
        {
            delete current_data_block_iter_;
            current_data_block_iter_=nullptr;
        }
        for(auto iter=past_data_block_iters_.begin();iter!=past_data_block_iters_.end();iter++) // delete all the past data_block_iter_
            delete *iter;
    }

    void TableIterator::Seek(const Slice& target)
    {
        index_block_iter_->Seek(target);
        InitDataBlock();
        SeekTheFoundKeyInDataBlock();
    }

    void TableIterator::SeekToFirst()
    {
        index_block_iter_->SeekToFirst();
        InitDataBlock();
        SeekTheFoundKeyInDataBlock();
    }

    void TableIterator::SeekToLast()
    {
        index_block_iter_->SeekToLast();
        InitDataBlock();
        SeekTheFoundKeyInDataBlock();
    }

    void TableIterator::Next()
    {
        index_block_iter_->Next();
        InitDataBlock();
        SeekTheFoundKeyInDataBlock();
    }

    void TableIterator::Prev()
    {
        index_block_iter_->Prev();
        InitDataBlock();
        SeekTheFoundKeyInDataBlock();
    }

    bool TableIterator::Valid() const
    {
        return current_data_block_iter_==nullptr? false: current_data_block_iter_->Valid();
    }

    Slice TableIterator::key() const
    {
        assert(Valid());
        return current_data_block_iter_->key();
    }

    Slice TableIterator::value() const
    {
        assert(Valid());
        return current_data_block_iter_->value();
    }

    Status TableIterator::status() const
    {
        // It'd be nice if status() returned a const Status& instead of a Status
        if (!index_block_iter_->status().ok()) 
        {
            return index_block_iter_->status();
        } 
        else if (current_data_block_iter_ != nullptr && !current_data_block_iter_->status().ok()) 
        {
            return current_data_block_iter_->status();
        } 
        else 
        {
            return status_;
        }
    }

    void TableIterator::InitDataBlock()
    {
        if(!index_block_iter_->Valid())
        {
            delete current_data_block_iter_;
            current_data_block_iter_=nullptr;
            data_block_handle_.clear();
        }
        else
        {
            Slice handle=index_block_iter_->value();
            if(current_data_block_iter_!=nullptr && handle.compare(data_block_handle_)==0)
            {
                // since the data block is already read, nothing needs to be done
            }   
            else
            {
                if(current_data_block_iter_!=nullptr)
                    past_data_block_iters_.push_back(current_data_block_iter_);
                current_data_block_iter_=Table::BlockReader(table_,read_options_,handle);
                data_block_handle_.assign(handle.data(),handle.size());
            }
        }
    }

    void TableIterator::SeekTheFoundKeyInDataBlock()
    {
        if(!index_block_iter_->Valid())
        {
            // do nothing, since it has been done in InitDataBlock()
            return;
        }
        Slice key=index_block_iter_->key();
        if(current_data_block_iter_!=nullptr)
        {
            current_data_block_iter_->Seek(key);
            if(current_data_block_iter_->Valid())
            {
                int result=key.compare(current_data_block_iter_->key());
                if(result==0)
                {
                    // Do nothing
                }
                else
                {
                    // mark as in valid if the key found in data block is not the same as the key found in index block
                    delete current_data_block_iter_;
                    current_data_block_iter_=nullptr;
                    data_block_handle_.clear();
                    status_=Status::Corruption("The key found in index block is not present in data block\n");
                }
            }
        }
    }

}