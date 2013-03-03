#ifndef __BLOCKMGR_TRANSACT_H__
#define __BLOCKMGR_TRANSACT_H__
#include "morphdb.h"
#include "blockmgrint.h"

#define BLOCKMGR_TRANSACT_MAGIC 0xffff5555

typedef struct BlockMgrTransactHdr{
   uint32_t magic;
   uint64_t lastChunkOffset;
   uint64_t lastChunkSize;
   uint64_t lastChunkChecksum;
   uint32_t dataStart;
}BlockMgrTransactHdr;

typedef struct BlockMgrTransactChunk {
   uint64_t targetOffset;
   uint32_t len;
}BlockMgrTransactChunk;

typedef struct BlockMgrTransactHandle {
   BlockMgrHandle* handle;
   BlockMgrTransactHdr hdr;
   BlockMgrFileHandle journalFile;
   MorphDB* blockList;   
   uint64_t curFileLen;
   uint64_t abortFileLen;
   uint32_t status;
} BlockMgrTransactHandle;

uint8_t BlockMgrJournalRead(BlockMgrHandle* handle,void* buf,uint32_t len,uint64_t offset);

uint8_t BlockMgrJournalWrite(BlockMgrHandle* handle,void* buf,uint32_t len,uint64_t offset);

#endif
