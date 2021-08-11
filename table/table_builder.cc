#include <cassert>
#include <vector>

#include "leveldb/table_builder.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "db/dbformat.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb
{
    struct TableBuilder::Rep
    {
        Rep(const Options& opt, WritableFile *f):
            options(opt),
            index_block_options(opt),
            file(f),
            file_offset(0),
            data_block_builder(&options),
            index_block_builder(&index_block_options),
            num_entries(0),
            closed(false),
            filter_block_builder(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy))
        {
            index_block_options.block_restart_interval=1;
            smallest_key.Clear();
            largest_key.Clear();
        }

        Options options;
        Options index_block_options;
        WritableFile *file;
        uint64_t file_offset;
        Status status;
        InternalKey smallest_key;
        InternalKey largest_key;
        BlockBuilder data_block_builder;
        BlockBuilder index_block_builder;
        std::string last_key;
        int64_t num_entries;
        bool closed;
        FilterBlockBuilder *filter_block_builder;
        
        // this flag indicates whether the keys saved within keys_buffer shall be saved to index block
        // Keys within keys buffer shall be saved to index block 
        // if the corresponding data block is flushed to file
        BlockHandle pending_handle;
        std::string compressed_output;
        std::vector<std::string*> pending_keys; // keys that are inserted into the data_block
    };

    TableBuilder::TableBuilder(const Options &options,WritableFile *file)
        :rep_(new Rep(options,file))
    {

    }

    TableBuilder::~TableBuilder()
    {
        assert(rep_->closed);
        delete rep_->filter_block_builder;
        delete rep_;
    }

    Status TableBuilder::ChangeOptions(const Options& options) 
    {
        // Note: if more fields are added to Options, update
        // this function to catch changes that should not be allowed to
        // change in the middle of building a Table.
        if (options.comparator != rep_->options.comparator) 
        {
            return Status::InvalidArgument("changing comparator while building table");
        }

        // Note that any live BlockBuilders point to rep_->options and therefore
        // will automatically pick up the updated options.
        rep_->options = options;
        rep_->index_block_options = options;
        rep_->index_block_options.block_restart_interval = 1;
        return Status::OK();
    }

    void TableBuilder::Add(const Slice& key,const Slice &value)
    {
        Rep *r=rep_;
        assert(!r->closed);
        if(!ok()) return;
        if(r->num_entries==0)
        {
            r->smallest_key.DecodeFrom(key);
        }
        r->largest_key.DecodeFrom(key);
        if(r->num_entries>0)
        {
            assert(r->options.comparator->Compare(key,Slice(r->last_key))>0);
        }
        
        std::string *pending_key=new std::string();
        pending_key->assign(key.data(),key.size());
        r->pending_keys.push_back(pending_key);

        if(r->filter_block_builder!=nullptr)
        {
            r->filter_block_builder->AddKey(key);
        }

        r->last_key.assign(key.data(),key.size());
        r->num_entries++;
        r->data_block_builder.Add(key,value);

        // TODO: change the trigger of flush
        const size_t estimated_block_size=r->data_block_builder.CurrentSizeEstimate();
        if(estimated_block_size>=r->options.block_size)
        {
            Flush();
        }
    }

    InternalKey TableBuilder::GetSmallestKey()
    {
        assert(rep_->closed && rep_->status.ok());
        return rep_->smallest_key;
    }
    InternalKey TableBuilder::GetLargestKey()
    {
        assert(rep_->closed && rep_->status.ok());
        return rep_->largest_key;
    }
    // r->pending_handle is set during appending blocks to file
    void TableBuilder::Flush()
    {
        Rep *r=rep_;
        assert(!r->closed);
        if(!ok()) return;
        AppendBlockToFile(&r->data_block_builder,&r->pending_handle);
        if(ok())
        {
            r->status=r->file->Flush();
        }
        if(ok())
        {
            std::string pending_handle_encoding;
            r->pending_handle.EncodeTo(&pending_handle_encoding);
            for(auto iter=r->pending_keys.begin();iter!=r->pending_keys.end();iter++)
            {
                // save every key written in the last data block to index block
                std::string *pending_key=*iter;
                r->index_block_builder.Add(Slice(pending_key->data(),pending_key->size()),Slice(pending_handle_encoding));
                delete *iter; // recycle the space each key occupies

            }
            r->pending_keys.clear();
        }
    }
    
    Status TableBuilder::Finish()
    {
        Rep *r=rep_;
        Flush(); // flush the final data_block to file
        assert(!r->closed);
        r->closed=true;

        BlockHandle filter_block_handle,metaindex_block_handle,index_block_handle;

        if(ok() && r->filter_block_builder!=nullptr)
        {
            AppendRawBlockToFile(r->filter_block_builder->Finish(),kNoCompression,&filter_block_handle);
        }
        
        // Write metaindex block, this block contains the file of the filter and 
        // the block handle of the filter block
        if (ok()) 
        {
            BlockBuilder meta_index_block(&r->options);
            if (r->filter_block_builder != nullptr) 
            {
                // Add mapping from "filter.Name" to location of filter data
                std::string key = "filter.";
                key.append(r->options.filter_policy->Name());
                std::string handle_encoding;
                filter_block_handle.EncodeTo(&handle_encoding);
                meta_index_block.Add(key, handle_encoding);
            }

            // TODO(postrelease): Add stats and other meta blocks
            AppendBlockToFile(&meta_index_block, &metaindex_block_handle);
        }

        if(ok())
        {
            AppendBlockToFile(&r->index_block_builder,&index_block_handle);
        }

        if (ok()) 
        {
            Footer footer;
            footer.set_metaindex_handle(metaindex_block_handle);
            footer.set_index_handle(index_block_handle);
            std::string footer_encoding;
            footer.EncodeTo(&footer_encoding);
            r->status = r->file->Append(footer_encoding);
            if (r->status.ok()) 
            {
                r->file_offset += footer_encoding.size();
            }
            // printf("totoal file size is %lu\n",index_block_handle.offset()+index_block_handle.size()+5+footer_encoding.size());
   
        }
        return r->status;
    }

    // File format contains a sequence of blocks where each block has:
    // block_data: uint8[n](maybe compressed)
    // type: uint8
    // crc: uint32
    void TableBuilder::AppendBlockToFile(BlockBuilder* block_builder, BlockHandle* handle)
    {
        assert(ok());
        Rep* r = rep_;
        Slice raw = block_builder->GenerateRawBlock();

        Slice block_contents;
        CompressionType type = r->options.compression;
        // TODO(postrelease): Support more compression options: zlib?
        switch (type) {
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
        block_builder->Reset();
    }

    void TableBuilder::AppendRawBlockToFile(const Slice& block_contents,
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
    
    void TableBuilder::Abandon()
    {
        Rep *r=rep_;
        assert(!r->closed);
        r->closed=true;
    }

    Status TableBuilder::status() const
    {
        return rep_->status;
    }

    bool TableBuilder::ok()
    {
        return status().ok();
    }

    uint64_t TableBuilder::NumEntries() const
    {
        return rep_->num_entries;
    }
    uint64_t TableBuilder::FileSize() const
    {
        return rep_->file_offset;
    }
} // namespace leveldb