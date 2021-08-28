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

const int kNumNonTableCacheFiles=10;


int main(int argc, char const *argv[])
{
    Status s;
    Options options;
    string dbname="/tmp/dbfiles";
    uint64_t file_num=5;
    string file_path=TableFileName(dbname,file_num);
    
    if(options.block_cache==nullptr)
    {
        printf("no block cache\n");
    }


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
    while (iter->Valid())
    {
        ParsedInternalKey internal_key;
        bool success=ParseInternalKey(iter->key(),&internal_key);
        if(success)
        {
            printf("user key: %.*s; sequence number %lu\n",static_cast<int>(internal_key.user_key.size()),internal_key.user_key.data(),internal_key.sequence);
        }
        iter->Next();
    }

    delete iter;    

    
    return 0;
}
