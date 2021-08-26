#include "table/filter_block.h"
#include "leveldb/options.h"
#include "db/dbformat.h"
#include "dbtests/common.h"
#include <string>
#include <vector>

using std::string;
using std::vector;
using std::to_string;
using namespace leveldb;


int main(int argc, char const *argv[])
{
    Options options;
    InternalFilterPolicy internal_filter_policy(options.filter_policy);
    FilterBlockBuilder filter_block_builder(&internal_filter_policy);
    vector<string *> generated_internal_keys;

    for(int i=0;i<100;i++)
    {
        string *key=new_internal_key(i,i,kTypeValue);
        generated_internal_keys.push_back(key);
        filter_block_builder.AddKey(*key);
    }
    Slice result=filter_block_builder.Finish();

    FilterBlockReader filter_reader(&internal_filter_policy,result);
    for(int i=0;i<generated_internal_keys.size();i++)
    {
        string &key=*generated_internal_keys[i];

        if(filter_reader.KeyMayMatch(i,key))
        {
            printf("key may match for key index %d\n",i);
        }
        else
        {
            printf("No match for key index %d\n",i);
        }
    }

    for(auto iter=generated_internal_keys.begin();iter!=generated_internal_keys.end();iter++)
    {
        delete *iter;
    }

    return 0;
}
