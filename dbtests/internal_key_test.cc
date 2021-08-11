#include "db/dbformat.h"

using namespace leveldb;

int main(int argc, char const *argv[])
{
    std::string user_key="helloworld";
    SequenceNumber sequence=128;
    ValueType type=kTypeValue;
    InternalKey key(user_key,sequence,kTypeValue);
    InternalKey other_key;
    other_key=key;
    return 0;
}
