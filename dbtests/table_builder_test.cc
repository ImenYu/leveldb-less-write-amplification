#include "leveldb/options.h"
#include "leveldb/table_builder.h"
#include "leveldb/env.h"
#include "db/filename.h"
#include "db/dbformat.h"

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

#include <sys/types.h> 
#include <sys/stat.h>

#include <assert.h>
#include <iostream>
#include <vector>
#include <stack>

using namespace leveldb;
using std::string;
using std::to_string;
using std::vector;
using std::pair;
using std::stack;

static void RemoveNonEmptyDir(const string &dirname)
{
    DIR *d=opendir(dirname.c_str());
    if(d)
    {
        struct dirent *ent;
        while (ent=readdir(d))
        {
            if(!strcmp(ent->d_name,".")||!strcmp(ent->d_name,".."))
                continue;

            string entry_path(dirname);
            entry_path.append("/");
            entry_path.append(ent->d_name,strlen(ent->d_name));

            struct stat st;
            if(stat(entry_path.c_str(),&st)==0)
            {
                if(S_ISDIR(st.st_mode))
                {
                    RemoveNonEmptyDir(entry_path);
                }
                else
                {
                    unlink(entry_path.c_str());
                }
            }
        }
        closedir(d);
    }
    rmdir(dirname.c_str());
}

static void generate_rand_string(string *s)
{
    int len=rand()%500+500;
    for(int i=0;i<len;i++)
    {
        char c;
        switch ((rand() % 3))
        {
        case 1:
            c='A'+rand()%26;
            break;
        case 2:
            c='a'+rand()%26;
            break;
        default:
            c='0'+rand()%10;
            break;
        }
        s->push_back(c);
    }
}

static string * new_internal_key(int i, SequenceNumber sequence,ValueType value_type)
{
    char buf[100];
    string *key=new string();
    snprintf(buf,100,"%010d",i);
    key->append(buf,10);

    EncodeFixed64(buf,(sequence<<8)|value_type);
    key->append(buf,8);
    return key;
}

int main(int argc, char const *argv[])
{
    Options raw_options; 
    std::string dbname="./dbfiles";
    if(raw_options.filter_policy==nullptr)
    {
        printf("no filter policy\n");
    }
    RemoveNonEmptyDir(dbname);
    raw_options.env->CreateDir(dbname);

    InternalKeyComparator icmp(raw_options.comparator);
    InternalFilterPolicy internal_filter_policy(raw_options.filter_policy);
    Options options;
    options.comparator=&icmp;
    options.filter_policy=raw_options.filter_policy==nullptr ? nullptr:&internal_filter_policy;

    int file_num=5;
    std::string filename=TableFileName(dbname,file_num);

    Status s;
    WritableFile *file=nullptr;
    s=raw_options.env->NewWritableFile(filename,&file);
    if(!s.ok())
    {
        printf("failed to open the file\n");
        return 1;
    }

    string *key;
    string *value;
    vector<pair<string *,string *>> inserted_key_value_pairs;
    TableBuilder table_builder(options,file);

    for(int i=0;i<5000;i++)
    {
        key=new_internal_key(i,i,kTypeValue);// this key is of type internal key
        value=new string();
        generate_rand_string(value);
        table_builder.Add(*key,*value);
        inserted_key_value_pairs.push_back(pair<string*,string*>(key,value));
    }

    table_builder.Finish();

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
    file = nullptr;

    InternalKey smallest=table_builder.GetSmallestKey();
    InternalKey largest=table_builder.GetLargestKey();
    printf("samllest user key: %.*s\n",smallest.user_key().size(),smallest.user_key().data());
    printf("largest user key: %.*s\n",largest.user_key().size(),largest.user_key().data());
    printf("file size: %d\n",table_builder.FileSize());

    for(auto iter=inserted_key_value_pairs.begin();iter!=inserted_key_value_pairs.end();iter++)
    {
        delete iter->first;
        delete iter->second;
    }



    return 0;
}
