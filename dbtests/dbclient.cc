#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "dbtests/common.h"

int main(int argc, char const *argv[])
{
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing=true;
    
    string dbname="./dbfiles";
    RemoveNonEmptyDir(dbname);

    leveldb::Status status = leveldb::DB::Open(options,dbname, &db);
    assert(status.ok());

    map<string,string> inserted_key_value_pairs;
    int num_keys_to_insert=1000000;
    for(int i=0;i<num_keys_to_insert;i++)
    {
        string key=to_string(random()%200000);
        string value=generate_rand_string();
        status = db->Put(leveldb::WriteOptions(), key,value);
        if(i%10000==0)
        {
            printf("inserting %dth key\n",i);
        }
        assert(status.ok());
        // if(inserted_key_value_pairs.find(key)!=inserted_key_value_pairs.end())
        // {
        //     inserted_key_value_pairs.erase(key);
        // }
        // inserted_key_value_pairs.insert(pair<string,string>(key,value));
    }

    // for(auto iter=inserted_key_value_pairs.begin();iter!=inserted_key_value_pairs.end();iter++)
    // {
    //     leveldb::Slice origin_key(iter->first);
    //     leveldb::Slice origin_value(iter->second);
    //     string found_value;

    //     status = db->Get(leveldb::ReadOptions(), origin_key, &found_value);
    //     if(!status.ok())
    //     {
    //         printf("key %s; %s\n",iter->first.c_str(),status.ToString().c_str());
    //         continue;
    //     }
    //     if(origin_value.compare(found_value)!=0)
    //     {
    //         printf("value not matched for key %s\n",iter->first.c_str());
    //         printf("-----------------------------------------------------------\n");

    //     }
    // }
    delete db;
    return 0;
}
