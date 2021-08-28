#include "table/table_appender.h"
#include "leveldb/status.h"
#include "db/filename.h"
#include "db/dbformat.h"
#include "dbtests/common.h"
#include <string>
#include <vector>

using namespace leveldb;
using std::string;
using std::to_string;
using std::vector;
using std::pair;

int main(int argc, char const *argv[])
{
    Status status;
    Options raw_options;
    string dbname="/tmp/dbfiles";
    uint64_t file_num=5;
    string file_path=TableFileName(dbname,file_num);

    InternalKeyComparator icmp(raw_options.comparator);
    InternalFilterPolicy internal_filter_policy(raw_options.filter_policy);
    Options options;
    options.comparator=&icmp;
    options.filter_policy=raw_options.filter_policy==nullptr ? nullptr:&internal_filter_policy;

    RandomReWrFile *file=nullptr;
    status=options.env->OpenFileAsRandomReWrFile(file_path,&file);
    if(!status.ok())
    {
        printf("failed to open the file %s\n",file_path.c_str());
        return 1;
    }
    
    uint64_t file_size;
    status=options.env->GetFileSize(file_path,&file_size);
    if(!status.ok())
    {
        printf("failed to get the file size\n");
        return 1;
    }

    TableAppender *table_appender=nullptr;
    status=TableAppender::Open(options,file,file_size,&table_appender);
    if(!status.ok())
    {
        printf("failed to open the table\n");
        return 1;
    }

    // table_appender->PrintOriginalIndexBlock();
    // table_appender->PrintDataBlockSize();
    vector<pair<string*,string*>> newly_appended_pairs;
    for(int i=0;i<5000;i++)
    {
        string *key=new_internal_key(i,i+1,kTypeValue);// sequence number is 1 bigger than original corresponding user kers
        string *value=new string();
        (*value)=generate_rand_string();
        newly_appended_pairs.push_back(pair<string*,string*>(key,value));
        table_appender->Append(*key,*value);
    }
    printf("num of appended pairs: %d\n",table_appender->NumAppendedEntries());
    table_appender->Finish();

    InternalKey smallest=table_appender->GetSmallestKey();
    InternalKey largest=table_appender->GetLargestKey();
    printf("smallest user key: %.*s\n",smallest.user_key().size(),smallest.user_key().data());
    printf("largest user key: %.*s\n",largest.user_key().size(),largest.user_key().data());
    printf("File size: %lu; Appended Data size: %lu; Original file size: %lu\n",
        table_appender->FileSize(),
        table_appender->AppendedDataSize(),
        table_appender->OriginalFileSize());

    Status s;
    s = file->Sync();
    if(!s.ok())
    {
        printf("failed to sync file\n");
    }
    s = file->Close();
    if(!s.ok())
    {
        printf("failed to close the file\n");
    }
    delete file;
    delete table_appender;
    for(auto iter=newly_appended_pairs.begin();iter!=newly_appended_pairs.end();iter++)
    {
        delete (*iter).first;
        delete (*iter).second;
    }


    // Iterator *iter=table_appender->GetIndexBlockIterator();
    // iter->SeekToFirst();
    // while (iter->Valid())
    // {
    //     printf("%.*s\n",iter->key().size()-8,iter->key().data());
    //     iter->Next();
    // }

    return 0;
}
