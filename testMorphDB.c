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

#include "morphdb.h"
#include "bson.h"
#include <sys/time.h>
#include <sys/resource.h>


void 
DBTreeCursorTest(char* fname) {
   int rv;
   MorphDBDiskTree *tree;
   MorphDBDiskPtr ptr;
   BTreeKeyOps intOps;
   int i;
   int count=12;
   BTreeKey key,value,k,v;

   MorphDBDiskPtr *p = malloc(sizeof(MorphDBDiskPtr)*100000);

   rv = MorphDBInitTreeHandle(fname,&tree);
   if (rv) {
      printf("Initializing tree failed\n");
      return;
   }
   intOps.cmp = BTreeKeyIntCmp;
   intOps.free = BTreeKeyBStrFree;
   intOps.copy = BTreeKeyBStrCopy;
   intOps.print = BTreeKeyIntPrint;
   tree->keyOps = intOps;

   k.data = (char*)&i;
   k.len = sizeof(i);
   v.data = (char*)&i;
   v.len = sizeof(i);
   // 5, 8, 1, 7, 3, 12, 9, 6
   i=5;
   assert(DBTreeInsert(tree,&k,&v));
   i=8;
   assert(DBTreeInsert(tree,&k,&v));
   i=1;
   assert(DBTreeInsert(tree,&k,&v));
   i=7;
   assert(DBTreeInsert(tree,&k,&v));
   i=3;
   assert(DBTreeInsert(tree,&k,&v));
   i=12;
   assert(DBTreeInsert(tree,&k,&v));
   i=9;
   assert(DBTreeInsert(tree,&k,&v));
   i=6;
   assert(DBTreeInsert(tree,&k,&v));
   DBTreePrint(tree);
   i = 9;
   MorphDBCursor * cursor;
   if (!MorphDBCursorInit(tree,&cursor)){
      assert(0);
      printf("Cursor initialization failed.\n");
   }
   do {
      assert(MorphDBCursorValue(cursor,&k,&v));
      printf("Key : ");
      BTreeKeyPrint(tree,&k);
      printf("\nValue:");
      BTreeKeyPrint(tree,&v);
      printf("\n");
   } while (MorphDBCursorNext(cursor));

   k.data = (char*)&i;
   k.len = sizeof(i);
   v.data = (char*)&i;
   v.len = sizeof(i);
   i = 2;
   if (!MorphDBCursorSetStartKey(cursor,k)){
      printf("Setting start key failed.\n");
      assert(0);
   }
   i=10;
   if (!MorphDBCursorSetEndKey(cursor,k)){
      printf("Setting start key failed.\n");

   }
   printf("Range cursor\n");
   do {
      assert(MorphDBCursorValue(cursor,&k,&v));
      printf("Key : ");
      BTreeKeyPrint(tree,&k);
      printf("\nValue:");
      BTreeKeyPrint(tree,&v);
      printf("\n");
   } while (MorphDBCursorNext(cursor));
   k.data = (char*)&i;
   k.len = sizeof(i);
   v.data = (char*)&i;
   v.len = sizeof(i);
   i = 25;
   if (!MorphDBCursorSetStartKey(cursor,k)){
      printf("Setting start key failed.\n");
      assert(0);
   }
   assert(!MorphDBCursorValue(cursor,&k,&v) && cursor->error == MORPHDB_CURSOR_END);
}

void GenerateStr(char* str, int len) {
   int i;
   for (i=0;i<len;i++) {
      str[i] = 'a'+(i%26);
   }
   str[i] = '\0';
   printf("%s\n",str);
}

void
DBTreeImplTest(char* fname, int dataSize) {
   int rv;
   MorphDBDiskTree *tree;
   MorphDBDiskPtr ptr;
   BTreeKeyOps intOps;
   int i;
   int count=1000*1000;
   BTreeKey key,value,k,v;
   MorphDBIntNode* node = NULL;
   double t1,t2;
   uint8_t newPtr;
   int j=0;
   int l=0;
   char smd[1024];
   char buf[1024];
   char *v1 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

   rv = MorphDBInitTreeHandle(fname,&tree);
   if (rv) {
      printf("Initializing tree failed\n");
      return;
   }
   intOps.cmp = BTreeKeyBStrCmp;
   intOps.free = BTreeKeyBStrFree;
   intOps.copy = BTreeKeyBStrCopy;
   intOps.print = BTreeKeyIntPrint;
   tree->keyOps = intOps;

   k.data = (char*)&i;
   k.len = sizeof(i);
   GenerateStr(smd,dataSize);
   v.data = smd;
   v.len = dataSize;
//   StrToBStr(v1,&v);
   t1 = UTime();
   for (i=0;i<count;i++) {
      //printf("Inserting %d\n",i);
      /*v.data = buf;
      v.len = snprintf(buf,1024,"%s%d",smd,i);*/
      assert(DBTreeInsert(tree,&k,&v));
      //DBTreePrint(tree);
      //DBTreeCheck(tree);
   }
   t2 = UTime();
   printf("Time took for %d %lf\n",count,t2-t1);
   printf("Data writeTime %lf seconds\n",tree->handle->dataWrite);
   printf("Num data writes %lu seconds\n",tree->handle->numDataWrite);
   return ;
   for (i=0;i<count;i++) {
      BTreeValue tmp;
      //printf("Inserting %d\n",i);
      assert(DBTreeFind(tree,&k,&tmp));
      if (memcmp(v.data,tmp.data,v.len)){
         printf("Data comparison failed.\n");
         assert(0);
      }
      free(tmp.data);
      //DBTreePrint(tree);
      //DBTreeCheck(tree);
   }

   assert(0);
   return;
   t1 = UTime(); 
   
   MorphDBIntNodeInit(tree,NULL,&node);
   for (i=0;i<count;i++) {
      if (NKEYS(node) >= TORDER(tree)-1) {
         MorphDBIntNodeInit(tree,NULL,&node);
         j = 0;
         l++;
      }
      MorphDBIntNodeInsertKey(tree,node,k,j,ptr);

      MorphDBIntNodeWrite(tree,node,&newPtr);
   }
   t2 = UTime();
   printf("Time took for insertion and intnode write %lf seconds \n",t2-t1);

   struct rusage usage;
   getrusage(RUSAGE_SELF,&usage);

   assert(0);
}

void DBTreeDeleteTest(char* fname){
   int rv;
   MorphDBDiskTree *tree;
   MorphDBDiskPtr ptr;
   BTreeKeyOps intOps;
   int i;
   int count=12;
   BTreeKey key,value,k,v;

   MorphDBDiskPtr *p = malloc(sizeof(MorphDBDiskPtr)*100000);

   rv = MorphDBInitTreeHandle(fname,&tree);
   if (rv) {
      printf("Initializing tree failed\n");
      return;
   }
   intOps.cmp = BTreeKeyIntCmp;
   intOps.free = BTreeKeyBStrFree;
   intOps.copy = BTreeKeyBStrCopy;
   intOps.print = BTreeKeyIntPrint;
   tree->keyOps = intOps;

   k.data = (char*)&i;
   k.len = sizeof(i);
   v.data = (char*)&i;
   v.len = sizeof(i);
   // 5, 8, 1, 7, 3, 12, 9, 6
   i=5;
   assert(DBTreeInsert(tree,&k,&v));
   i=8;
   assert(DBTreeInsert(tree,&k,&v));
   i=1;
   DBTreePrint(tree);
   assert(DBTreeInsert(tree,&k,&v));
   i=7;
   assert(DBTreeInsert(tree,&k,&v));
   i=3;
   assert(DBTreeInsert(tree,&k,&v));
   i=12;
   assert(DBTreeInsert(tree,&k,&v));
   i=9;
   assert(DBTreeInsert(tree,&k,&v));
   i=6;
   assert(DBTreeInsert(tree,&k,&v));
   DBTreePrint(tree);
   i = 9;
   printf("Deleting %d\n",i);
   assert(DBTreeDelete(tree,&k));
   DBTreePrint(tree);
   i = 8;
   printf("Deleting %d\n",i);
   assert(DBTreeDelete(tree,&k));
   DBTreePrint(tree);
   i = 12;
   printf("Deleting %d\n",i);
   assert(DBTreeDelete(tree,&k));
   DBTreePrint(tree);
   i = 5;
   printf("Deleting %d\n",i);
   assert(DBTreeDelete(tree,&k));
   DBTreePrint(tree);
   i = 1;
   printf("Deleting %d\n",i);
   assert(DBTreeDelete(tree,&k));
   DBTreePrint(tree);
   i = 7;
   printf("Deleting %d\n",i);
   assert(DBTreeDelete(tree,&k));
   DBTreePrint(tree);
   i = 3;
   printf("Deleting %d\n",i);
   assert(DBTreeDelete(tree,&k));
   DBTreePrint(tree);
   i = 6;
   printf("Deleting %d\n",i);
   assert(DBTreeDelete(tree,&k));
   DBTreePrint(tree);
}


void MorphDBTest(char* fname) {
   int rv;
   MorphDBDiskTree *tree;
   MorphDBDiskPtr ptr;
   int i;
   int count=12;
   MorphDBDiskPtr *p = malloc(sizeof(MorphDBDiskPtr)*100000);

   rv = MorphDBInitTreeHandle(fname,&tree);
   if (rv) {
      printf("Initializing tree failed\n");
      return;
   }
   for (i=0;i<count;i++) {

      if (!MorphDBAllocateDataBytes(tree->handle,16*1024,&p[i]) ){
         printf("Alllocating data bytes failed.\n");
         return;
      }
      //printf("Allocated offset %llu\n",_O(&p[i]));
   }

   if (!MorphDBFreeBlock(tree->handle,p[0])) {
      printf("freeing block failed .\n");
      return;
   }
   //printf("Freeing block passed %llu.\n",_O(&p[0]));
   if (!MorphDBAllocateDataBytes(tree->handle,16*1024,&ptr) ){
      printf("Alllocating data bytes failed.\n");
      return;
   }
   assert(_O(&ptr) == _O(&p[0]));
   //printf("Allocated offset %llu\n",_O(&ptr));
   if (!MorphDBAllocateDataBytes(tree->handle,16*1024,&ptr) ){
      printf("Alllocating data bytes failed.\n");
      return;
   }
   //printf("Allocated offset %llu\n",_O(&ptr));
   assert(_O(&ptr) > _O(&p[count-1]));
   printf("Allocate and free test passed\n");
}

int 
main(int argc , char** argv) {
    char* testStr = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
   MorphDBDiskTree *tree;
   double t1,t2;
   int i; 
   int count = 1000000;
   bson_oid_t *oids = malloc(sizeof(*oids)*count);
   BTreeKey key,value;
   int rv;
   double allocTime =0,writeTime=0,intNodeWriteTime =0;
   int dataSize = 100;
   //MorphDBTest(argv[1]);
   //DBIntNodeTest(argv[1]);
   if (argc < 3) {
      printf("Filename and value len is required.\n");
      return -1;
   }
   dataSize = atoi(argv[2]);
   DBTreeImplTest(argv[1],dataSize);
   //DBTreeDeleteTest(argv[1]);
   //DBTreeCursorTest(argv[1]);
   return 0;
   BTreeStrTest();
   if (argc < 2) {
      printf("DB name required.\n");
      return -1;
   }
   t1=UTime();
   rv = MorphDBInitTreeHandle(argv[1],&tree);
   if (rv) {
      printf("Initializing tree failed\n");
      return 0;
   }
   t2=UTime();
   printf("Tree initialization time  %lf\n",t2-t1);
   DBTreeIntNodePrint(tree,tree->root,True);

   value.data = testStr;
   value.len = strlen(testStr);
   t1=UTime();
   for (i=0;i<count;i++) {
      //printf("%d\n",i);
      bson_oid_gen(&oids[i]);
      key.data = oids[i].bytes;
      key.len = sizeof(oids[i].bytes);
      value = key;
      //t1 = UTime();
      assert(DBTreeInsert(tree,&key,&value));
      /*
      if (i%10 == 0) {
         //MorphDBIntNodeInit(tree,NULL,&node);
      }
      //MorphDBIntNodeWrite(tree,node);
      t2=UTime();
      intNodeWriteTime+= (t2-t1);
      t1=UTime();
      if (!MorphDBAllocateDataBytes(tree->handle,1000,&ptr) ){
         return False;
      }
      t2=UTime();
      allocTime += (t2-t1);
      t1=UTime();
      MorphDBDataWrite(tree,&value,&ptr);
      t2=UTime();
      writeTime +=(t2-t1);
      //DBTreeCheckLRUList(tree);
      //printf("\n");
      //DBTreeIntNodePrint(tree,tree->root,True);   
      //DBTreePrint(tree);*/
   }
   t2=UTime();
   printf("\ngetfreeblock %lf\n",tree->handle->getFreeBlock);
   printf("Alloctime %lf writeTime %lf intNodeWriteTime %lf\n",allocTime,writeTime,intNodeWriteTime);
   printf("numInt Nodes %u %lf, %lf\n",tree->numIntNodes,tree->dataWriteTime,tree->intNodeWriteTime);
   printf("Insertion took %lf seconds\n",t2-t1);
   printf("Data writeTime %lf seconds\n",tree->handle->dataWrite);
   //assert(0); 
   //return 0;
   t1=UTime();
   for (i=0;i<count;i++) {
      //printf("%d\n",i);
      //bson_oid_gen(&oids[i]);
      key.data = oids[i].bytes;
      key.len = sizeof(oids[i].bytes);
      assert(DBTreeFind(tree,&key,&value));

      if (memcmp(key.data,value.data,key.len)){
         assert(0);
      }
      free(value.data);
      //value = key;
      //printf("\n");
      //DBTreeIntNodePrint(tree,tree->root,True);   
      //DBTreePrint(tree);
   }
   t2=UTime();
   printf("Reading took %lf seconds\n",t2-t1);
}

