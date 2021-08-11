#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/cache.h"
#include "table/format.h"
#include "db/table_cache.h"
#include "db/filename.h"
#include <string>

using namespace leveldb;
using std::string;


int main(int argc, char const *argv[])
{
    Status s;
    Options raw_options;    
    if(raw_options.block_cache==nullptr)
    {
        printf("no block cache\n");
    }

    InternalKeyComparator icmp(raw_options.comparator);
    InternalFilterPolicy internal_filter_policy(raw_options.filter_policy);
    Options options;
    options.comparator=&icmp;
    options.filter_policy=raw_options.filter_policy==nullptr ? nullptr:&internal_filter_policy;

    string dbname="./dbfiles";
    uint64_t file_num=5;
    string file_path=TableFileName(dbname,file_num);


    RandomAccessFile *file=nullptr;
    s=options.env->NewRandomAccessFile(file_path,&file);
    if(!s.ok())
    {
        printf("failed to open file: %s\n",file_path.c_str());
        return 1;
    }

    uint64_t file_size;
    s=options.env->GetFileSize(file_path,&file_size);
    if(!s.ok())
    {
        printf("failed to get file size\n");
        return 1;
    }
    else
    {
        printf("file size is %lu\n",file_size);
    }
    
    
    Table *table=nullptr;
    s=Table::Open(options,file,file_size,&table);
    if(!s.ok())
    {
        printf("failed to open table\n");
        return 1;
    }
    

    ReadOptions read_options;
    read_options.verify_checksums=options.paranoid_checks;
    read_options.fill_cache=false;
    Iterator *iter=table->NewTableIterator(read_options);
    
    iter->SeekToFirst();
    string key;
    bool assigned=false;
    while (iter->Valid())
    {
        key.assign(iter->key().data(),iter->key().size()-8);
        if(!assigned)
        {
            printf("smallest user key: %s\n",key.c_str());
            assigned=true;
        }

        ParsedInternalKey internal_key;
        bool success=ParseInternalKey(iter->key(),&internal_key);
        if(success)
        {
            printf("user key: %.*s; sequence number %lu\n",static_cast<int>(internal_key.user_key.size()),internal_key.user_key.data(),internal_key.sequence);
        }
        iter->Next();
    }
    printf("largest user key: %s\n",key.c_str());

    delete table;
    delete iter; 

    return 0;
}
