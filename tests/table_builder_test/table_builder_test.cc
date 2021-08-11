#include "leveldb/table_builder.h"
#include <string.h>
#include <assert.h>
#include <iostream>
#include <vector>
#include <leveldb/env.h>

using std::string;
using std::vector;
using std::pair;

void generate_rand_string(string *s)
{
    int len=rand()%1000;
    if(len<500) len+=500;
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


int main(int argc, char const *argv[])
{

    string *key;
    string *value;
    vector<pair<string *,string *>> inserted_key_value_pairs;
    for(int i=0;i<10000;i++)
    {
        key=new string();
        value=new string();
        generate_rand_string(key);
        generate_rand_string(value);
        
        printf("puting %dth key-value pair\n",i);


        inserted_key_value_pairs.push_back(pair<string*,string*>(key,value));
    }

    for(auto iter=inserted_key_value_pairs.begin();iter!=inserted_key_value_pairs.end();iter++)
    {
        delete iter->first;
        delete iter->second;
    }
    return 0;
}