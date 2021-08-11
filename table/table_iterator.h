#ifndef STORAGE_LEVELDB_TABLE_TABLE_ITERATOR_H_
#define STORAGE_LEVELDB_TABLE_TABLE_ITERATOR_H_

#include "leveldb/iterator.h"
#include "leveldb/table.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include <vector>

namespace leveldb
{
    class Table;
    class TableIterator:public Iterator
    {
    public:
        TableIterator(Table* table,ReadOptions read_options);
        ~TableIterator();

        virtual void Seek(const Slice& target) override;
        virtual void SeekToFirst() override;
        virtual void SeekToLast() override;
        virtual void Next() override;
        virtual void Prev() override;
        virtual bool Valid() const override;
        virtual Slice key() const override;
        virtual Slice value() const override;
        virtual Status status() const override;

    private:
        void InitDataBlock();
        // assure the key found by index_block_iter_ is the same as
        // the key found by data_block_iter_
        // if not the same, mark the iterator as invalid and set curruption status
        void SeekTheFoundKeyInDataBlock();

        Table *table_;

        Iterator *index_block_iter_;
        Iterator *current_data_block_iter_;
        std::vector<Iterator*> past_data_block_iters_; // save the old data_block_iters_

        const ReadOptions& read_options_;
        Status status_;
        // it is current data block's block handle(i.e. offset and size in the file)
        std::string data_block_handle_;
    };
}



#endif