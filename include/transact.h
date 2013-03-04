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
