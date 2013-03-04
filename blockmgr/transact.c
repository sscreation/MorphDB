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

#include "blockmgrtransact.h"
#include "fileops.h"
#include "blockmgrint.h"

typedef enum JournalStatus {
   JOURNAL_IN_PROGRESS,
   JOURNAL_TO_REPLAY,
}JournalStatus;

uint8_t BlockMgrTransactStartWithLock(BlockMgrHandle* handle);
uint8_t BlockMgrTransactAbortWithLock(BlockMgrHandle* handle);
uint8_t BlockMgrTransactCommitWithLock(BlockMgrHandle* handle);
uint8_t BlockMgrTransactReplayWithLock(BlockMgrHandle* handle);
uint8_t BlockMgrTransactStartWithLock(BlockMgrHandle* handle) {
   BTreeKeyOps rangeOps;
   BlockMgrTransactHandle* jHandle;
   char journalName[4096];

   rangeOps.cmp = BTreeKeyRangeCmp;
   //oidOps.cmp = BTreeKeyOidCmp;
   rangeOps.free = BTreeKeyBStrFree;
   rangeOps.copy = BTreeKeyBStrCopy;
   rangeOps.print = BTreeKeyBStrPrint;
   if (handle->journalHandle) {
      SetBMgrError(handle,BLOCKMGR_TRANSAC_EXISTS);
      return False;
   }
   jHandle = malloc(sizeof(BlockMgrTransactHandle));
   if (!jHandle) {
      SetBMgrError(handle,BLOCKMGR_NO_MEMORY);
      return False;
   }
   snprintf(journalName,sizeof(journalName),"%s.journal",_BlockMgrFileName(handle->fileHandle));

   if (SkipListMDBInit(NULL,32,&jHandle->blockList)) {
      SetBMgrError(handle,BLOCKMGR_NO_MEMORY);
      free(jHandle);
      return False;
   }
   MorphDBSetKeyOps(jHandle->blockList,rangeOps);
   if(!_BlockMgrFileOpen(&jHandle->journalFile,journalName)) {
      SetBMgrError(handle,jHandle->journalFile.error);
      free(jHandle);
      return False;
   }
   _BlockMgrGetFileSize(handle->fileHandle,&jHandle->abortFileLen);
   jHandle->status = JOURNAL_IN_PROGRESS;
   jHandle->hdr.magic = BLOCKMGR_TRANSACT_MAGIC;
   jHandle->hdr.lastChunkOffset = 0;
   jHandle->hdr.lastChunkSize = 0;
   jHandle->hdr.lastChunkChecksum = 0;
   jHandle->hdr.dataStart = 512;

   jHandle->handle = handle;
   jHandle->curFileLen = 512;
   handle->journalHandle = jHandle;
   
   return True;
}

uint8_t BlockMgrTransactStart(BlockMgrHandle* handle) {
   uint8_t ret = False;
   PlatformRWGetWLock(handle->rwLock);
   ret = BlockMgrTransactStartWithLock(handle);
   PlatformRWUnlock(handle);
   return ret;
}

uint8_t BlockMgrTransactAbortWithLock(BlockMgrHandle* handle) {
   BlockMgrTransactHandle* jHandle = handle->journalHandle;
   if (!jHandle) {
      SetBMgrError(handle,BLOCKMGR_NO_TRANSAC);
      return False;
   }
   if (jHandle->status == JOURNAL_TO_REPLAY) {
      // already 
      return True;
   }
   _BlockMgrFileTruncate(handle->fileHandle,jHandle->abortFileLen);
   MorphDBRemove(jHandle->blockList);
   _BlockMgrFileRemove(&jHandle->journalFile);
   free(jHandle);
   handle->journalHandle = NULL;
   return BlockMgrDropCaches(handle);
}

uint8_t BlockMgrTransactAbort(BlockMgrHandle* handle) {
   uint8_t ret = False;
   PlatformRWGetWLock(handle->rwLock);
   ret = BlockMgrTransactAbortWithLock(handle);
   PlatformRWUnlock(handle->rwLock);
   return ret;
}
typedef struct ChunkDesc {
   uint8_t present;
   uint64_t startOffset;
   uint64_t len;
   uint64_t targetOffset;
}ChunkDesc;

static uint8_t BlockMgrJournalGetChunkList(BlockMgrTransactHandle* jHandle,
      uint64_t offset, uint32_t len,ListNode* head) {
   LongRange toFind;
   BTreeKey key,value;
   MorphDBCursor* cursor;
   uint64_t curOff = offset;
   ListNode* node = NULL;
   ChunkDesc* desc = NULL;

   toFind.start = offset;
   toFind.end   = offset+len;
   key.data =  (char*) &toFind;
   key.len  =  sizeof(toFind);
   if (!MorphDBCursorInit(jHandle->blockList,&cursor)) {
      return False;
   }
   if (!MorphDBCursorSetStartKey(cursor,key)) {
      //MorphDBCursorFree(cursor);
      //return False;
   }
   
   do {
      LongRange *r;

      if (!MorphDBCursorValue(cursor,&key,&value)) {
         // cursor end
         //MorphDBCursorFree(cursor);
         break;
      }
      r = (LongRange*) key.data; 
      if (r->start > curOff) {
         node = malloc(sizeof(ListNode));
         if (!node) {
            MorphDBCursorFree(cursor);
            return False;
         }
         desc = malloc(sizeof(ChunkDesc));
         if (!desc) {
            free(node);
            MorphDBCursorFree(cursor);
            return False;
         }
         node->data = desc;
         desc->present = False;
         desc->startOffset = curOff;
         desc->len = (r->start >= toFind.end) ? toFind.end-curOff : r->start - curOff;
         node->data = desc;
         LIST_ADD_AFTER(node,LIST_LAST(head));
         curOff += desc->len;
      }
      if (curOff >= toFind.end ) {
         // All done break
         BinaryStrFree(&key);
         BinaryStrFree(&value);
         break;
      }
      node = malloc(sizeof(ListNode));
      if (!node) {
         MorphDBCursorFree(cursor);
         return False;
      }
      desc = malloc(sizeof(ChunkDesc));
      if (!desc) {
         free(node);
         MorphDBCursorFree(cursor);
         return False;
      }
      node->data = desc;
      desc->present = True;
      desc->startOffset = r->start;
      desc->len  = (r->end >= toFind.end) ? toFind.end-curOff : r->end-curOff;
      desc->targetOffset = *((uint64_t*)value.data);
      curOff += desc->len;
      LIST_ADD_AFTER(node,LIST_LAST(head));
      BinaryStrFree(&key);
      BinaryStrFree(&value);
      if (curOff >= toFind.end ) {
         break;
      }
      //printf("CurOff %lu\n",curOff);
   } while(MorphDBCursorNext(cursor));

   if (curOff < toFind.end) {
      node = malloc(sizeof(ListNode));
      if (!node) {
         //MorphDBCursorFree(cursor);
         return False;
      }
      desc = malloc(sizeof(ChunkDesc));
      if (!desc) {
         free(node);
         MorphDBCursorFree(cursor);
         return False;
      }
      node->data = desc;
      desc->present = False;
      desc->startOffset = curOff;
      desc->len = toFind.end-curOff;
      curOff += desc->len;
      LIST_ADD_AFTER(node,LIST_LAST(head));
   }
   MorphDBCursorFree(cursor);
   return True;
}


void BlockMgrJournalFreeChunkList(ListNode* head) {
   ListNode* node,*next;
   LIST_FOR_ALL_SAFE(head,node,next){
      ChunkDesc* desc = node->data;
      LIST_REMOVE(&head,node);
      free(node);
      free(desc);
   }

}

uint8_t BlockMgrJournalRead(BlockMgrHandle* handle,void* buf,uint32_t len,uint64_t offset) {
   BlockMgrTransactHandle* jHandle = handle->journalHandle;
   ListNode head;
   ListNode* node;
   uint8_t rv = True;
   uint64_t curOff=0;

   LIST_INIT(&head);
   if (!BlockMgrJournalGetChunkList(jHandle,offset,len,&head)) {
      SetBMgrError(handle,BLOCKMGR_NO_MEMORY);
      rv = False;
      goto cleanup;
   }
   LIST_FOR_ALL(&head,node) {
      ChunkDesc* desc = node->data;
      if (desc->present) {
         if (!_BlockMgrFileRead(&jHandle->journalFile,buf+curOff,desc->len,desc->targetOffset)){
            SetBMgrError(handle,BLOCKMGR_IO_ERR);
            rv = False;
            goto cleanup;
         }
      } else {
         if (!BlockMgrFileReadDirect(jHandle->handle,buf+curOff,desc->len,offset+curOff)) {
            rv = False;
            goto cleanup;
         }
      }   
      curOff += desc->len;
   }
cleanup:
   BlockMgrJournalFreeChunkList(&head);
   return rv;
}

uint8_t BlockMgrJournalWrite(BlockMgrHandle* handle,void* buf,uint32_t len,uint64_t offset) {
   BlockMgrTransactHandle* jHandle = handle->journalHandle;
   ListNode head;
   ListNode* node;
   uint8_t rv=True;
   uint64_t curOff=0;
   LongRange range;
   uint64_t valOffset=0;
   BTreeKey key,val;

   if (jHandle->status == JOURNAL_TO_REPLAY) {
      if (!BlockMgrTransactReplay(handle)) {
         return False;
      }
      return BlockMgrFileWriteDirect(handle,buf,len,offset);
   }

   key.data = (char*)&range;
   key.len = sizeof(range);
   val.data = (char*) &valOffset;
   val.len  = sizeof(valOffset);

   LIST_INIT(&head);
   if (!BlockMgrJournalGetChunkList(jHandle,offset,len,&head)) {
      SetBMgrError(handle,BLOCKMGR_NO_MEMORY);
      rv = False;
      goto cleanup;
   }
   LIST_FOR_ALL(&head,node) {
      ChunkDesc* desc = node->data;
      if (desc->present) {
         if (!_BlockMgrFileWrite(&jHandle->journalFile,buf+curOff,desc->len,desc->targetOffset)){
            SetBMgrError(handle,BLOCKMGR_IO_ERR);
            rv = False;
            goto cleanup;
         }
      } else {
         BlockMgrTransactChunk chunk;
         chunk.targetOffset = offset+curOff;
         chunk.len = desc->len;
         if (!_BlockMgrFileWrite(&jHandle->journalFile,&chunk,sizeof(chunk),jHandle->curFileLen)){
            SetBMgrError(handle,BLOCKMGR_IO_ERR);
            rv = False;
            goto cleanup;
         }
         jHandle->curFileLen += sizeof(BlockMgrTransactChunk);
         jHandle->hdr.lastChunkOffset = jHandle->curFileLen;
         jHandle->hdr.lastChunkSize = desc->len;
         if (!_BlockMgrFileWrite(&jHandle->journalFile,buf+curOff,desc->len,jHandle->curFileLen)){
            SetBMgrError(handle,BLOCKMGR_IO_ERR);
            rv = False;
            goto cleanup;
         }
         range.start = desc->startOffset;
         range.end = desc->startOffset+desc->len;
         valOffset = jHandle->curFileLen;
         if (!MorphDBInsert(jHandle->blockList,key,val)) {
            SetBMgrError(handle,BLOCKMGR_NO_MEMORY);
            rv = False;
            goto cleanup;
         }
         jHandle->curFileLen += desc->len;
      }   
      curOff += desc->len;
   }
cleanup:
   BlockMgrJournalFreeChunkList(&head);
   return rv;
}

uint8_t BlockMgrTransactCommitWithLock(BlockMgrHandle* handle) {
   BlockMgrTransactHandle* jHandle = handle->journalHandle;
   if (!jHandle) {
      SetBMgrError(handle,BLOCKMGR_NO_TRANSAC);
      return False;
   }
   if (!_BlockMgrFileWrite(&jHandle->journalFile,&jHandle->hdr,sizeof(jHandle->hdr),0)) {
      SetBMgrError(handle,BLOCKMGR_IO_ERR);
      return False;
   }
   if (!_BlockMgrFileSync(&jHandle->journalFile)) {
      SetBMgrError(handle,BLOCKMGR_IO_ERR);
      return False;
   }
   jHandle->status = JOURNAL_TO_REPLAY;
   return True;
}


uint8_t BlockMgrTransactCommit(BlockMgrHandle* handle) {
   uint8_t ret;
   PlatformRWGetWLock(handle->rwLock);
   ret = BlockMgrTransactCommitWithLock(handle->rwLock);
   PlatformRWUnlock(handle->rwLock);
   return ret;
}
static uint8_t BlockMgrJournalVerify(BlockMgrTransactHandle* jHandle) {
   uint64_t fileSize;

   if (!_BlockMgrGetFileSize(&jHandle->journalFile,&fileSize)) {
      SetBMgrError(jHandle->handle,BLOCKMGR_IO_ERR);
      return False;
   }
   if (fileSize != jHandle->hdr.lastChunkOffset+jHandle->hdr.lastChunkSize) {
      return False;
   }
   // Verify checksum
   return True;
}

uint8_t BlockMgrTransactReplayWithLock(BlockMgrHandle* handle) {
   BlockMgrTransactHandle* jHandle = handle->journalHandle;      
   uint64_t curOff ;

   if (!jHandle) {
      SetBMgrError(handle,BLOCKMGR_NO_TRANSAC);
      return False;
   }
   curOff = jHandle->hdr.dataStart;
   if (jHandle->status != JOURNAL_TO_REPLAY) {
      SetBMgrError(handle,BLOCKMGR_TRANSAC_NOT_COMMITTED); 
      return False;
   }
   // journal already verified 
   while (curOff < jHandle->hdr.lastChunkOffset + jHandle->hdr.lastChunkSize) {
      BlockMgrTransactChunk chunk;
      char* data;
      if(!_BlockMgrFileRead(&jHandle->journalFile,&chunk,sizeof(chunk),curOff)) {
         SetBMgrError(handle,BLOCKMGR_IO_ERR); 
         return False;
      }
      data = malloc(chunk.len);
      if (!data) {
         SetBMgrError(handle,BLOCKMGR_NO_MEMORY); 
         return False;
      }
      curOff += sizeof(chunk);
      if(!_BlockMgrFileRead(&jHandle->journalFile,data,chunk.len,curOff)) {
         SetBMgrError(handle,BLOCKMGR_IO_ERR); 
         free(data);
         return False;
      }
      if(!BlockMgrFileWriteDirect(jHandle->handle,data,chunk.len,chunk.targetOffset)) {
         SetBMgrError(handle,BLOCKMGR_IO_ERR); 
         free(data);
         return False;
      }
      free(data);
      curOff += chunk.len;
   }
   _BlockMgrFileRemove(&jHandle->journalFile);
   if (jHandle->blockList) {
      MorphDBRemove(jHandle->blockList);
   }
   free(jHandle);
   handle->journalHandle = NULL;
   return True;
}


uint8_t BlockMgrTransactReplay(BlockMgrHandle* handle) {
   uint8_t ret = False;
   PlatformRWGetWLock(handle->rwLock);
   ret = BlockMgrTransactReplayWithLock(handle->rwLock);
   PlatformRWUnlock(handle->rwLock);
   return ret;
}
uint8_t BlockMgrJournalCheck(BlockMgrHandle* handle) {
   BlockMgrTransactHandle *jHandle;
   char journalName[4096];

   jHandle = malloc(sizeof(*jHandle));
   if (!jHandle) {
      SetBMgrError(handle,BLOCKMGR_NO_MEMORY);
      free(jHandle);
      return False;
   }
   snprintf(journalName,sizeof(journalName),"%s.journal",_BlockMgrFileName(handle->fileHandle));
   if(!_BlockMgrFileOpenRW(&jHandle->journalFile,journalName)) {
      // file does not exists
      free(jHandle);
      return True;
   }
   if (!_BlockMgrFileRead(&jHandle->journalFile,&jHandle->hdr,sizeof(jHandle->hdr),0)) {
      _BlockMgrFileRemove(&jHandle->journalFile);
      free(jHandle);
      return True;
   }
   jHandle->handle = handle;
   
   if (!BlockMgrJournalVerify(jHandle)) {
      _BlockMgrFileRemove(&jHandle->journalFile);
      free(jHandle);
      return True;
   }
   jHandle->status = JOURNAL_TO_REPLAY;
   jHandle->blockList = NULL;
   handle->journalHandle = jHandle;
   if (!BlockMgrTransactReplay(handle)) {
      _BlockMgrFileClose(&jHandle->journalFile);
      handle->journalHandle = NULL;
      free(jHandle);
      return False;
   }

   _BlockMgrFileRemove(&jHandle->journalFile);
   handle->journalHandle = NULL;
   return True;
}
