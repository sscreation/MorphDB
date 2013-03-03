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
