#include "include/leveldb/db.h"
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

#include <sys/types.h> 
#include <sys/stat.h>

#include <assert.h>
#include <iostream>
#include <map>
#include <stack>

#include <leveldb/slice.h>

using std::string;
using std::to_string;
using std::pair;
using std::stack;
using std::map;

static void RemoveNonEmptyDir(const std::string &dirname)
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

static string generate_rand_string()
{
    string s;
    int len=4096;
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
        s.push_back(c);
    }
    return s;
}


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