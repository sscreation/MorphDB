#include "blockmgrtransact.h"

uint8_t BlockMgrTransactStart(BlockMgrHandle* handle) {
   BlockMgrTransactHandle* jHandle;
   char journalName[4096];

   if (handle->journalHandle) {
      SetBMgrError(handle,BLOCKMGR_TRANSAC_EXISTS);
      return False;
   }
   jHandle = malloc(sizeof(BlockMgrTransactHandle));
   if (!jHandle) {
      SetBMgrError(handle,BLOCKMGR_NO_MEMORY);
      return False;
   }
   snprintf(journalName,"%s.journal",BlockMgrFileName(handle));

   if(!_BlockMgrFileOpen(jHandle->journalFile,journalName)) {
      SetBMgrError(handle,jHandle->journalFile.error);
      free(jHandle);
      return False;
   }
   jHandle->hdr.magic = BLOCKMGR_TRANSACT_MAGIC;
   jHandle->lastChunkOffset = 0;
   jHandle->lastChunkSize = 0;
   jHandle->lastChunkCheckSum = 0;
   jHandle->dataStart = 512;
   jHandle->handle = handle;
   handle->journalHandle = jHandle;
   return True;
}

uint8_t BlockMgrTransactAbort(BlockMgrHandle* handle) {
   BlockMgrDropCaches(handle);
}
