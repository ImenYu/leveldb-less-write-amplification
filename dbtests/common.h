#ifndef DBTEST_COMMON_H_
#define DBTEST_COMMON_H_

#include "db/dbformat.h"
#include "util/coding.h"

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

using std::string;
using std::to_string;
using std::pair;
using std::stack;
using std::map;

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

static string * new_internal_key(int i, leveldb::SequenceNumber sequence,leveldb::ValueType value_type)
{
    char buf[100];
    string *key=new string();
    snprintf(buf,100,"%010d",i);
    key->append(buf,10);

    leveldb::EncodeFixed64(buf,(sequence<<8)|value_type);
    key->append(buf,8);
    return key;
}

#endif