#include "blockmgrint.h"

#ifdef WIN32
uint8_t _BlockMgrGetFileSize(BlockMgrFileHandle* handle,uint64_t *size) {
   LARGE_INTEGER dataTemp;
   if(GetFileSizeEx(handle->fd,&dataTemp)) {
      *size = dataTemp.QuadPart;
      return True;
   } else {
      handle->error = BLOCKMGR_IO_ERR;
      handle->osError = GetLastError();
      return False;
   }
}


uint8_t _BlockMgrFileTruncate(BlockMgrFileHandle* handle, uint64_t size) {
   DWORD dwPtr = SetFilePointer( handle->fd, 
                                size, 
                                NULL, 
                                FILE_BEGIN ); 
   if(SetEndOfFile(handle->fd)) {
      return True; 
   } else {
      handle->error = BLOCKMGR_IO_ERR;
      handle->osError = GetLastError();
      return False;
   }
}
//fopen and sets endof file to 0.

//uint8_t _BlockMgrFileOpen1(

uint8_t _BlockMgrFileOpen(BlockMgrFileHandle* handle,char* fileName) {
  //INVALID_HANDLE_VALUE
  if( (handle->fd = CreateFile((TCHAR*)fileName,                  // open filename
              GENERIC_READ|GENERIC_WRITE|FILE_APPEND_DATA,        // open for writing/reading
              FILE_SHARE_READ|FILE_SHARE_WRITE,                   // allow multiple readers
              NULL,                     // no security
              OPEN_ALWAYS,              // open or create
              FILE_ATTRIBUTE_NORMAL,    // normal file
              NULL)) != INVALID_HANDLE_VALUE) {                   // no attr. template
     return True;
  } else {
     handle->error = BLOCKMGR_IO_ERR;
     handle->osError = GetLastError(); 
     return False;    
  }  
}
uint8_t _BlockMgrFileOpenRW(BlockMgrFileHandle* handle,char* fileName) {
     if( (handle->fd = CreateFile((TCHAR*)fileName, // open Two.txt
              GENERIC_READ|GENERIC_WRITE|FILE_APPEND_DATA,         // open for writing/reading
              FILE_SHARE_READ|FILE_SHARE_WRITE,                   // allow multiple readers
              NULL,                     // no security
              OPEN_EXISTING,              // open or create
              FILE_ATTRIBUTE_NORMAL,    // normal file
              NULL)) != INVALID_HANDLE_VALUE) {                   // no attr. template
        return True;
     } else {
        handle->error = BLOCKMGR_IO_ERR;
        handle->osError = GetLastError(); 
        return False;    
     }  
}
///XXX Need to be changed to accommodate 64 bit value.
uint8_t _BlockMgrFileRead(BlockMgrFileHandle* handle,void *buffer,size_t size,size_t offset ) {
   DWORD  dwBytesRead = 0;
   if(SetFilePointer( handle->fd, 
                                offset, 
                                NULL, 
                                FILE_BEGIN ) == INVALID_SET_FILE_POINTER) {
      return False;
   }
   if(ReadFile(handle->fd, buffer,size, &dwBytesRead, NULL)) {
      return True;
   }
   else {
      handle->error = BLOCKMGR_IO_ERR;
      handle->osError = GetLastError(); 
      return False;
   }
}

///XXX Need to be changed to accommodate 64 bit value.
uint8_t _BlockMgrFileWrite(BlockMgrFileHandle* handle,void *buffer,size_t size,uint64_t offset ) {
   DWORD  dwBytesWriten = 0;
   if(SetFilePointer( handle->fd, 
                                offset, 
                                NULL, 
                                FILE_BEGIN ) == INVALID_SET_FILE_POINTER) {
      return False;
   } 
   if( WriteFile(   handle->fd,           // open file handle
                    buffer,      // start of data to write
                    size,  // number of bytes to write
                    &dwBytesWriten, // number of bytes that were written
                    NULL)) { 
        if (dwBytesWriten != size)
        {
            // This is an error because a synchronous write that results in
            // success (WriteFile returns TRUE) should write all data as
            // requested. This would not necessarily be the case for
            // asynchronous writes.
            printf("Error: dwBytesWriten != dwBytesToWrite\n");
            return False;
        }
        else
        {
          return True;
        }
    } else {
        handle->error = BLOCKMGR_IO_ERR;
        handle->osError = GetLastError();
        return False;
    }
  
}

uint8_t _BlockMgrFileClose(BlockMgrFileHandle* handle) {
   if(CloseHandle(handle->fd)) {
      return True;
   } else { 
        handle->error = BLOCKMGR_IO_ERR;
        handle->osError = GetLastError();
        return False;
   }     
}
//XXX correct the function and implement gettimeofday
double UTime(){
   struct timeval tim;
   double ret=0;
  // gettimeofday(&tim, NULL);
   //ret = tim.tv_sec+(tim.tv_usec/1000000.0);
   return ret;
}

#else // For Linux

uint8_t _BlockMgrGetFileSize(BlockMgrFileHandle* handle,uint64_t *size) {
   struct stat st;
   if (fstat(handle->fd,&st) <0) {
      handle->error = BLOCKMGR_IO_ERR;
      handle->osError = errno;
      return False;
   } else {
      *size = st.st_size;
      return True;
   }
}

//rew- size is enough in size_t
uint8_t _BlockMgrFileTruncate(BlockMgrFileHandle* handle, uint64_t size) {
   if(ftruncate(handle->fd,size) < 0) {
     printf("File truncation failed\n");
     handle->error = BLOCKMGR_IO_ERR;
     handle->osError = errno;
   } else {
      return True;
   }
}

uint8_t _BlockMgrFileOpen(BlockMgrFileHandle* handle,char* fileName) {
   if((handle->fd = open(fileName,O_RDWR|O_CREAT,0600)) < 0 ) {

     handle->error = BLOCKMGR_IO_ERR;
     handle->osError = errno;
     return False;
   } else {
      strncpy(handle->name,fileName,NAME_MAX);
      return True;
   }
}
uint8_t _BlockMgrFileOpenRW(BlockMgrFileHandle* handle,char* fileName) {
   if((handle->fd = open(fileName,O_RDWR)) < 0) {
     handle->error = BLOCKMGR_IO_ERR;
     handle->osError = errno;
     return False;
   } else {
      strncpy(handle->name,fileName,NAME_MAX);
      return True;
   }
}

///XXX Need to be changed to accommodate 64 bit value.
uint8_t _BlockMgrFileRead(BlockMgrFileHandle* handle,void *buffer,size_t size,size_t offset ) {

   if ((pread(handle->fd,buffer,size,offset)) != size) {
      return False;
   }
   return True;
   if ((lseek(handle->fd,offset,SEEK_SET))<0) {
      handle->error = BLOCKMGR_IO_ERR;
      handle->osError = errno; 
      perror("LSEEK failed");
      assert(0);
      return False;
   }
   if(read(handle->fd,buffer,size) < 0) {
      handle->error = BLOCKMGR_IO_ERR;
      handle->osError = errno; 
      perror("read failed");
   } else {
      return True;
   }

}

///XXX Need to be changed to accommodate 64 bit value.
uint8_t _BlockMgrFileWrite(BlockMgrFileHandle* handle,void *buffer,size_t size,uint64_t offset ) {
   /*if ((lseek(handle->fd,offset,SEEK_SET))<0) {
      handle->error = BLOCKMGR_IO_ERR;
      handle->osError = errno; 
      perror("LSEEK failed");
      return False;
   }*/
   size_t rv = pwrite(handle->fd,buffer,size,offset);
   if ( ((int)rv)<0 || rv <size) {
      handle->error = BLOCKMGR_IO_ERR;
      handle->osError = errno;
      perror("WRITE failed");
      return False;
   } else {
      return True;
   }
}

uint8_t _BlockMgrFileSync(BlockMgrFileHandle* handle) {
   if (fsync(handle->fd)) {
      handle->error = BLOCKMGR_IO_ERR;
      return False;
   }
   return True;
}

uint8_t _BlockMgrFileClose(BlockMgrFileHandle* handle) {
   //fsync(handle->fd);
   if(close(handle->fd) == 0) {
      return True;
   } else { 
        handle->error = BLOCKMGR_IO_ERR;
        handle->osError = errno;
        return False;
   }     
}

double UTime(){
   struct timeval tim;
   double ret=0;
   gettimeofday(&tim, NULL);
   ret = tim.tv_sec+(tim.tv_usec/1000000.0);
   return ret;
}
#endif

typedef struct BlockMgrFileMMapDesc {
   void* ptr;
} BlockMgrFileMMapDesc;

uint8_t
_BlockMgrFileMap(BlockMgrFileHandle* fileHandle,
                uint64_t offset,
                uint32_t len,
                void **mapAddr,
                void **mapHandle) {
#ifndef WIN32
   void *addr = NULL;
   *mapAddr = NULL;
   *mapHandle = NULL;
   addr = mmap(NULL,len,
               PROT_READ | PROT_WRITE,  
               MAP_SHARED,fileHandle->fd,offset); 
   if (!addr) {
      return False;
   }
   *mapAddr = addr;
   return True;
#else
   return False;
#endif
}

uint8_t
_BlockMgrFileUnmap(BlockMgrFileHandle* fileHandle,
                 void* addr,
                 void* map,uint32_t length){
#ifndef WIN32
   if (munmap(addr,length)) {
      return False;
   }
   return True;
#else
   return False; 
#endif
}


uint8_t
_BlockMgrFileRemove(BlockMgrFileHandle* handle){

#ifndef WIN32
   _BlockMgrFileClose(handle);
   if (unlink(handle->name)) {
      return False;
   }
   return True;
#else
   return False; 
#endif
}

const char* _BlockMgrFileName(BlockMgrFileHandle* handle) {
   return handle->name;
}
