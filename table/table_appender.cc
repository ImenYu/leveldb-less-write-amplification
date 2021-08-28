#include "table/table_appender.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/block.h"
#include "table/merger.h"
#include "leveldb/options.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "leveldb/filter_policy.h"
#include "db/dbformat.h"
#include "util/crc32c.h"
#include <set>

namespace leveldb
{
    struct TableAppender::Rep
    {
        Rep(const Options& opt,RandomReWrFile *f,const Footer &footer, const BlockContents &index_block_contents,uint64_t file_size,uint64_t r_bytes):
            options(opt),
            index_block_options(opt),
            file(f),
            data_block_builder(&options),
            index_block_builder(&index_block_options),
            newly_added_indices(&options),
            filter_block_builder(opt.filter_policy==nullptr?
                        nullptr:new FilterBlockBuilder(opt.filter_policy)),
            closed(false),
            file_offset(file_size),
            original_file_size(file_size),
            read_bytes(r_bytes),
            num_appended_entries(0)        
        {
            original_index_block=new Block(index_block_contents);
            original_metaindex_block_handle=footer.metaindex_handle();
            index_block_options.block_restart_interval=1;
            smallest_key.Clear();
            largest_key.Clear();
        }

        ~Rep()
        {
            delete original_index_block;
            delete filter_block_builder;
        }
        Options options;
        Options index_block_options;
        Status status;
        RandomReWrFile *file;
        bool closed;
        uint64_t read_bytes;
        int64_t num_appended_entries;
        std::string last_appended_key;

        Block* original_index_block;
        BlockHandle original_metaindex_block_handle;

        uint64_t original_file_size;
        uint64_t file_offset; // initially set to the original file size

        InternalKey smallest_key;
        InternalKey largest_key;

        // used for appending new data
        BlockBuilder data_block_builder;
        BlockBuilder index_block_builder;
        BlockBuilder newly_added_indices; // container for newly appended indices
        FilterBlockBuilder *filter_block_builder;
        std::string compressed_output;
        
        BlockHandle pending_handle;
        std::vector<std::string*> pending_keys;
    };

    Status TableAppender::Open(const Options& options,RandomReWrFile *file,uint64_t file_size,TableAppender **table_appender)
    {
        uint64_t read_bytes=0;
        *table_appender=nullptr;
        if(file_size<Footer::kEncodedLength)
        {
            return Status::Corruption("file is too short to be an sstable");
        }

        char footer_space[Footer::kEncodedLength];
        Slice footer_input;
        Status s=file->Read(file_size-Footer::kEncodedLength,Footer::kEncodedLength,&footer_input,footer_space);
        read_bytes+=Footer::kEncodedLength;
        if(!s.ok()) return s;

        Footer footer;
        s=footer.DecodeFrom(&footer_input);
        if(!s.ok()) return s;

        BlockHandle metaindex_block_handle=footer.metaindex_handle();
        BlockHandle index_block_handle=footer.index_handle();

        BlockContents index_block_contents;
        ReadOptions read_options;
        if(options.paranoid_checks)
        {
            read_options.verify_checksums=true;
        }
        s=ReadBlockFromRamdomReWrFile(file,read_options,index_block_handle,&index_block_contents);
        read_bytes+=index_block_handle.size();

        if(s.ok())
        {
            Rep* rep=new TableAppender::Rep(options,file,footer,index_block_contents,file_size,read_bytes);
            *table_appender=new TableAppender(rep);
            s=(*table_appender)->Initialize();
        }
        return s;
    }

    TableAppender::TableAppender(Rep* rep):rep_(rep){}

    TableAppender::~TableAppender()
    {
        assert(rep_->closed);
        delete rep_;
    }

    void TableAppender::PrintOriginalIndexBlock()
    {
        Iterator *iter=rep_->original_index_block->NewBlockIterator(rep_->options.comparator);
        iter->SeekToFirst();
        while(iter->Valid())
        {
            
            ParsedInternalKey parsed_internal_key;
            bool success=ParseInternalKey(iter->key(),&parsed_internal_key);
            if(success)
            {
                printf("user key is %.*s; sequence number is %lu\n",static_cast<int>(parsed_internal_key.user_key.size()),
                    parsed_internal_key.user_key.data(),
                    parsed_internal_key.sequence);
            }
            else
            {
                printf("failed to parse key as parsed internal key\n");
                break;
            }
            
            Status s;
            BlockHandle block_handle;
            Slice input(iter->value());
            s=block_handle.DecodeFrom(&input);
            if(s.ok())
            {
                printf("block offset is %lu;block size is %lu\n",block_handle.offset(),block_handle.size());
            }
            else
            {
                printf("failed to decode block handle\n");
                break;
            }
            iter->Next();
        }
        delete iter;
    }

    uint64_t TableAppender::OriginalFileSize()
    {
        return rep_->original_file_size;
    }
    
    uint64_t TableAppender::NumAppendedEntries()
    {
        return rep_->num_appended_entries;
    }

    uint64_t TableAppender::FileSize()
    {
        return rep_->file_offset;
    }
    
    uint64_t TableAppender::ReadBytes()
    {
        return rep_->read_bytes;
    }

    uint64_t TableAppender::AppendedDataSize()
    {
        return rep_->file_offset-rep_->original_file_size;
    }

    InternalKey TableAppender::GetSmallestKey()
    {
        assert(rep_->closed && rep_->status.ok());
        return rep_->smallest_key;
    }

    InternalKey TableAppender::GetLargestKey()
    {
        assert(rep_->closed && rep_->status.ok());
        return rep_->largest_key;
    }


    void TableAppender::Append(const Slice& key, const Slice& value)
    {
        assert(!rep_->closed);
        if(!ok()) return;
        if(rep_->num_appended_entries>0)
        {
            assert(rep_->options.comparator->Compare(key,Slice(rep_->last_appended_key))>0);
            // there will not be inserting two same keys consecutively.
        }

        std::string *pending_key=new std::string();
        pending_key->assign(key.data(),key.size());
        rep_->pending_keys.push_back(pending_key);

        rep_->last_appended_key.assign(key.data(),key.size());
        rep_->num_appended_entries++;
        rep_->data_block_builder.Add(key,value);
        
        const size_t estimated_block_size=rep_->data_block_builder.CurrentSizeEstimate();
        if(estimated_block_size>=rep_->options.block_size)
        {
            Flush();
        }

    }

    void TableAppender::Flush()
    {
        assert(!rep_->closed);
        if(!ok()) return;

        const Slice& data_block_data=rep_->data_block_builder.GenerateRawBlock();
        AppendBlockToFile(data_block_data,&rep_->pending_handle);
        rep_->data_block_builder.Reset();

        if(ok())
        {
            std::string pending_handle_encoding;
            rep_->pending_handle.EncodeTo(&pending_handle_encoding);
            for(auto iter=rep_->pending_keys.begin();iter!=rep_->pending_keys.end();iter++)
            {
                std::string *pending_key=*iter;
                rep_->newly_added_indices.Add(Slice(pending_key->data(),pending_key->size()),Slice(pending_handle_encoding));
                delete *iter; // recycle the space
            }
            rep_->pending_keys.clear();
        }
    }

    Status TableAppender::Finish()
    {
        Flush(); // flush the the last data block and insert corresponding keys into new_indices_container
        assert(!rep_->closed);
        rep_->closed=true;

        // construct the merging iterator
        Slice newly_added_indices_data=rep_->newly_added_indices.GenerateRawBlock();        
        Block newly_added_indices_block(newly_added_indices_data,false);
        Iterator *original_index_block_iter=rep_->original_index_block->NewBlockIterator(rep_->options.comparator);
        Iterator *newly_added_indices_iter=newly_added_indices_block.NewBlockIterator(rep_->options.comparator);
        Iterator **iterators=new Iterator*[2];
        iterators[0]=original_index_block_iter;
        iterators[1]=newly_added_indices_iter;
        Iterator *merging_iter=NewMergingIterator(rep_->options.comparator,iterators,2);
        // merging iter organize keys in increasing user key order, decreasing sequence number order, decreasing type order.

        // generate the new index block, add only the newest keys to index block        
        std::string last_internal_key;
        bool has_last_internal_key=false;
        std::string last_value;
        merging_iter->SeekToFirst();
        while (ok() && merging_iter->Valid())
        {
            assert(merging_iter->key().size()>=8);
            if(!has_last_internal_key)
            {
                last_internal_key.assign(merging_iter->key().data(),merging_iter->key().size());
                last_value.assign(merging_iter->value().data(),merging_iter->value().size());
                rep_->smallest_key.DecodeFrom(last_internal_key);
                has_last_internal_key=true;
            }
            else
            {
                Slice last_user_key(last_internal_key.data(),last_internal_key.size()-8);
                Slice current_user_key(merging_iter->key().data(),merging_iter->key().size()-8);
                int r=current_user_key.compare(last_user_key);
                assert(r>=0);
                if(r>0)
                {
                    rep_->index_block_builder.Add(last_internal_key,last_value);
                    last_internal_key.assign(merging_iter->key().data(),merging_iter->key().size());
                    last_value.assign(merging_iter->value().data(),merging_iter->value().size());
                }                
                // if r==0 do nothing, since it is in decreasing sequence order
            }
            merging_iter->Next();
        }
        rep_->index_block_builder.Add(last_internal_key,last_value);
        rep_->largest_key.DecodeFrom(last_internal_key);

        delete[] iterators;
        delete merging_iter;// original_index_block_iter and newly_added_indices_iter are deleted within delete merging_iter;

        BlockHandle filter_block_handle,metaindex_block_handle,index_block_handle;

        // generate the filter block and write it to file
        const Slice index_block_data=rep_->index_block_builder.GenerateRawBlock();
        if(ok()&&rep_->filter_block_builder!=nullptr)
        {
            Block index_block(index_block_data,false);
            Iterator *iter=index_block.NewBlockIterator(rep_->options.comparator);
            iter->SeekToFirst();
            while (iter->Valid())
            {
                rep_->filter_block_builder->AddKey(iter->key());
                iter->Next();
            }
            delete iter;
            AppendRawBlockToFile(rep_->filter_block_builder->Finish(),kNoCompression,&filter_block_handle);
        }

        // generate and append the meta index block
        if(ok())
        {
            BlockBuilder meta_index_block(&rep_->options);
            if (rep_->filter_block_builder != nullptr) 
            {
                // Add mapping from "filter.Name" to location of filter data
                std::string key = "filter.";
                key.append(rep_->options.filter_policy->Name());
                std::string handle_encoding;
                filter_block_handle.EncodeTo(&handle_encoding);
                meta_index_block.Add(key, handle_encoding);
            }

            // TODO(postrelease): Add stats and other meta blocks
            AppendBlockToFile(meta_index_block.GenerateRawBlock(), &metaindex_block_handle);   
        }

        // append the index block
        if(ok())
        {
            AppendBlockToFile(index_block_data,&index_block_handle);
        }

        if (ok()) 
        {
            Footer footer;
            footer.set_metaindex_handle(metaindex_block_handle);
            footer.set_index_handle(index_block_handle);
            std::string footer_encoding;
            footer.EncodeTo(&footer_encoding);
            rep_->status = rep_->file->Append(footer_encoding);
            if (rep_->status.ok()) 
            {
                rep_->file_offset += footer_encoding.size();
            }
        }
        return rep_->status;
    }

    void TableAppender::Abandon()
    {
        assert(!rep_->closed);
        rep_->closed=true;
    }

    void TableAppender::AppendBlockToFile(const Slice& raw, BlockHandle* handle)
    {
        assert(ok());
        Rep* r = rep_;

        Slice block_contents;
        CompressionType type = r->options.compression;
        // TODO(postrelease): Support more compression options: zlib?
        switch (type) 
        {
            case kNoCompression:
            block_contents = raw;
            break;

            case kSnappyCompression: 
            {
            std::string* compressed = &r->compressed_output;
            if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                compressed->size() < raw.size() - (raw.size() / 8u)) 
            {
                block_contents = *compressed;
            }
            else 
            {
                // Snappy not supported, or compressed less than 12.5%, so just
                // store uncompressed form
                block_contents = raw;
                type = kNoCompression;
            }
            break;
            }
        }
        AppendRawBlockToFile(block_contents, type, handle);
        r->compressed_output.clear();
    }

    void TableAppender::AppendRawBlockToFile(const Slice& block_contents,
                        CompressionType compression_type, BlockHandle* handle)
    {
        Rep* r = rep_;
        handle->set_offset(r->file_offset);
        handle->set_size(block_contents.size());
        r->status = r->file->Append(block_contents);
        if (r->status.ok()) 
        {
            char trailer[kBlockTrailerSize];
            trailer[0] = compression_type;
            uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
            crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
            EncodeFixed32(trailer + 1, crc32c::Mask(crc));
            r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
            if (r->status.ok()) 
            {
                r->file_offset += block_contents.size() + kBlockTrailerSize;
            }
        }
    }


    // Status TableAppender::Initialize() // previous version of initialize
    // {
    //     Status &rep_status=rep_->status;
    //     if(rep_->options.filter_policy==nullptr)
    //     {
    //         rep_->file_offset=rep_->original_metaindex_block_handle.offset();
    //         rep_->original_file_size=rep_->file_offset;
    //         rep_status=rep_->file->Lseek(rep_->file_offset);
    //         return rep_status;
    //     }

    //     ReadOptions read_options;
    //     if (rep_->options.paranoid_checks) 
    //     {
    //         read_options.verify_checksums = true;
    //     }

    //     BlockContents contents;
    //     if (!ReadBlockFromRamdomReWrFile(rep_->file, read_options, rep_->original_metaindex_block_handle, &contents).ok()) 
    //     {
    //         // Do not propagate errors since meta info is not needed for operation
    //         rep_status=Status::Corruption("failed to get data block limit\n");
    //         return rep_status;
    //     }

    //     Block* metaindex_block = new Block(contents);
    //     Iterator* iter = metaindex_block->NewBlockIterator(rep_->options.comparator);
    //     std::string key = "filter.";
    //     key.append(rep_->options.filter_policy->Name());
    //     iter->Seek(key);
    //     if (iter->Valid() && iter->key() == Slice(key)) 
    //     {
    //         Slice value=iter->value();
    //         BlockHandle filter_block_handle;
    //         rep_status=filter_block_handle.DecodeFrom(&value);
    //         if(rep_status.ok())
    //         {
    //             rep_->file_offset=filter_block_handle.offset();
    //             rep_->original_file_size=rep_->file_offset;
    //         }
    //     }
    //     delete metaindex_block;
    //     delete iter;

    //     if(rep_status.ok())
    //     {
    //         rep_status=rep_->file->Lseek(rep_->file_offset);
    //     }
    //     return rep_status;
    // }

    Status TableAppender::Initialize()
    {
        rep_->status=rep_->file->Lseek(rep_->original_file_size);
        return rep_->status;
    }

    Status TableAppender::status() const
    {
        return rep_->status;
    }

    bool TableAppender::ok()
    {
        return status().ok();
    }

} // namespace leveldb