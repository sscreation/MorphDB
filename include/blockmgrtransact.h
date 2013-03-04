/*
    Copyright (C) 2013 Sridhar Valaguru <sridharnitt@gmail.com>

    This file is part of MorphDB.

    MorphDB is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    MorphDB is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with MorphDB.  If not, see <http://www.gnu.org/licenses/>.
*/

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
