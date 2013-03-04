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

#ifndef __FILE_OP_H__
#define __FILE_OP_H__
#include "blockmgrint.h"
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
uint8_t _BlockMgrGetFileSize(BlockMgrFileHandle* handle,uint64_t *size);
uint8_t _BlockMgrFileTruncate(BlockMgrFileHandle* handle, uint64_t size);
uint8_t _BlockMgrFileOpen(BlockMgrFileHandle* handle,char* fileName);
uint8_t _BlockMgrFileOpenRW(BlockMgrFileHandle* handle,char* fileName);
uint8_t _BlockMgrFileRead(BlockMgrFileHandle* handle,void *buffer,size_t size,size_t offset);
uint8_t _BlockMgrFileWrite(BlockMgrFileHandle* handle,void *buffer,size_t size,uint64_t offset);
uint8_t _BlockMgrFileClose(BlockMgrFileHandle* handle);
uint8_t _BlockMgrFileMap(BlockMgrFileHandle* fileHandle,
                uint64_t offset,
                uint32_t len,
                void **mapAddr,
                void **mapHandle);
uint8_t _BlockMgrFileUnmap(BlockMgrFileHandle* fileHandle,
                 void* addr,
                 void* map,uint32_t length);
uint8_t _BlockMgrFileSync(BlockMgrFileHandle* handle);
const char* _BlockMgrFileName(BlockMgrFileHandle* handle);
uint8_t _BlockMgrFileRemove(BlockMgrFileHandle* handle);
#endif
