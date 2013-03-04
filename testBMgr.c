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

#include "blockmgr.h"
#include <assert.h>

void GenerateStr(char* str, int len) {
   int i;
   for (i=0;i<len;i++) {
      str[i] = 'a'+(i%26);
   }
   str[i] = '\0';
   //printf("%s\n",str);
}

uint8_t TestTransactions(BlockMgrHandle* handle) {
   BinaryStr val;
   BlockMgrDiskPtr ptr1,ptr2;
   uint8_t newPtr;
   int i=100;
   int *k;
   char arr[1000];

   val.data = (char*) &i;
   val.len  = sizeof(i);
   assert(BlockMgrTransactStart(handle));
   _SO(&ptr1,0); 
   assert(BlockMgrDataWrite(handle,&val,&ptr1,&newPtr));
   assert(newPtr);
   printf("Allocated %lu .\n" ,_O(&ptr1));

   assert(BlockMgrDataRead(handle,ptr1,&val));
   k = (int*) val.data;

   assert(*k == 100);
   BinaryStrFree(&val);
   assert(BlockMgrDataFree(handle,ptr1));
   _SO(&ptr2,0); 
   i = 200;
   val.data = (char*) &i;
   val.len  = sizeof(i);
   assert(BlockMgrDataWrite(handle,&val,&ptr2,&newPtr));
   assert(newPtr);
   printf("Allocated %lu .\n" ,_O(&ptr2));


   assert(BlockMgrDataRead(handle,ptr2,&val));
   k = (int*) val.data;
   assert(*k == 200);
   BinaryStrFree(&val);
   assert(BlockMgrTransactCommit(handle)); 
   assert(BlockMgrTransactAbort(handle));
   val.data = (char*) &i;
   val.len  = sizeof(i);
   _SO(&ptr1,0); 
   assert(BlockMgrDataWrite(handle,&val,&ptr1,&newPtr));
   assert(newPtr);
   printf("Allocated %lu .\n" ,_O(&ptr1));

   _SO(&ptr2,0); 
   i = 200;
   assert(BlockMgrDataWrite(handle,&val,&ptr2,&newPtr));
   assert(newPtr);
   printf("Allocated %lu .\n" ,_O(&ptr2));
   val.data = arr;
   val.len  = 200;
   assert(BlockMgrDataWrite(handle,&val,&ptr2,&newPtr));
   assert(BlockMgrDataRead(handle,ptr2,&val));
}

int main(int argc, char** argv) {
   int i=1;   
   int j=i*10;
   int rv;
   int len = 0;
   int count = 1000*1000;
   BinaryStr key;
   BinaryStr val;
   BinaryStr val2;
   BlockMgrHandle *handle;
   char *data = NULL;

   if (argc < 3) {
      printf("Usage: %s <dbName> <strlen> [count]\n",argv[0]);
      return -1;
   }
   len = atoi(argv[2]);
   if (argc > 3) {
      count = atoi(argv[3]);
   }
   data = malloc(len+1);
   assert(data);

   key.data = (char*) &i;
   key.len  = sizeof(int);
   //uint8_t newPtr;
   //val.len = 10;
   //assert(BlockMgrDataWrite(handle,&val,&ptr,&newPtr));
   //val.data = malloc(13*1024*1024);
   //val.len = 13*1024*1024;
   //assert(BlockMgrDataWrite(handle,&val,&ptr,&newPtr));
   //assert(newPtr);
   GenerateStr(data,len);
   val.data = data;
   val.len  = len;
   assert(!BlockMgrFormatFile(argv[1],0));
   assert(!BlockMgrInitHandle(argv[1],0,&handle));
   BlockMgrDiskPtr ptr;
   _SO(&ptr,0); 
//   return TestTransactions(handle);
   double t1=UTime(),t2;; 
   printf("Without transactions \n");
   for (i=0;i< count;i++) {
      BlockMgrDiskPtr ptr;
      uint8_t newPtr;
      _SO(&ptr,0); 
      assert(BlockMgrDataWrite(handle,&val,&ptr,&newPtr));
      assert(newPtr);
     // printf("Allocated address %lu\n",_O(&ptr));
      if (i%10000 == 0) {
         printf("Prog %d\n",i);
      }
   }
   t2=UTime();
   printf("Time took %lf \n",t2-t1);
   BlockMgrFreeHandle(handle);
   assert(!BlockMgrFormatFile(argv[1],0));
   assert(!BlockMgrInitHandle(argv[1],0,&handle));
   assert(BlockMgrTransactStart(handle));
   t1=UTime();
   for (i=0;i< count;i++) {
      BlockMgrDiskPtr ptr;
      uint8_t newPtr;
      _SO(&ptr,0); 
      assert(BlockMgrDataWrite(handle,&val,&ptr,&newPtr));
      assert(newPtr);
     // printf("Allocated address %lu\n",_O(&ptr));
      if (i%10000 == 0) {
         printf("Prog %d\n",i);
      }
   }
   assert(BlockMgrTransactCommit(handle)); 
   assert(BlockMgrTransactReplay(handle)); 
   t2=UTime();
   printf("Time took %lf \n",t2-t1);
   BlockMgrFreeHandle(handle);
   free(data);
   return 0;
}

