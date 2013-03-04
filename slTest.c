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


#include "skiplist.h"
#include "btreekeyops.h"
#include "list.h"
#include "morphdb.h"
#include <sys/time.h>

double UTime(){
   struct timeval tim;
   double ret=0;
   gettimeofday(&tim, NULL);
   ret = tim.tv_sec+(tim.tv_usec/1000000.0);
   return ret;
}

#define StrToBStr2(str,bStr) (bStr)->data = (char*)str; (bStr)->len = strlen(str)+1;
typedef struct ChunkDesc {
   uint8_t present;
   uint64_t startOffset;
   uint64_t len;
   uint64_t targetOffset;
}ChunkDesc;

int main() {
   MorphDB* db;
   MorphDBCursor* cursor;
   int i;
   BTreeKey key;
   

   assert(SkipListMDBInit("",32,&db) == MORPHDB_OK);
   assert(MorphDBCursorInit(db,&cursor));
   key.data = (char*)&i;
   key.len = sizeof(i);
   assert(MorphDBCursorSetStartKey(cursor,key)); 
   return 0;
}
#if 0
void BTreeKeyRangePrint(BTreeKey* key) {
   LongRange *r1 = (LongRange*)key->data;
   if (!r1) {
      return;
   }
   printf(" [ %lu %lu ]",r1->start,r1->end);
}

void SkipListCursorTest(SkipList* sl) {
   SkipListCursor *cursor;
   BTreeKey key,value;
   LongRange range;
   assert(SkipListCursorInit(sl,&cursor));
   key.data = (char*) &range;
   key.len = sizeof(range);
   range.start = 100;
   range.end   = 500; 
   assert(SkipListCursorSetStartKey(cursor,key));
   do {
      assert(SkipListCursorValue(cursor,&key,&value));
      BTreeKeyRangePrint(&key);
      BinaryStrFree(&key);
      BinaryStrFree(&value);
   }while(SkipListCursorNext(cursor));
   printf("\n");
   SkipListCursorFree(cursor);
}

void
FindOffsetRange(SkipList* list,LongRange* toFind) {
   ListNode head;
   BTreeKey key;
   BTreeKey value;
   uint64_t curOff = toFind->start;
   LIST_INIT(&head);
   SkipListCursor *cursor;

   key.data = (char*)toFind;
   key.len  = sizeof(*toFind);
   assert(SkipListCursorInit(list,&cursor));
   assert(SkipListCursorSetStartKey(cursor,key));
   do {
      LongRange *r;
      ListNode* node;
      ChunkDesc* desc;
      assert(SkipListCursorValue(cursor,&key,&value));
      r = (LongRange*) key.data; 

      if (r->start > curOff) {
         ListNode* node = malloc(sizeof(ListNode));
         ChunkDesc* desc = malloc(sizeof(ChunkDesc));
         // starting position is not there.
         desc->present = False;
         desc->startOffset = curOff;
         desc->len = (r->start >= toFind->end) ? toFind->end-curOff : r->start - curOff;
         node->data = desc;
         LIST_ADD_AFTER(node,LIST_LAST(&head));
         curOff += desc->len;
      printf("ADD\n");
      }
      if (curOff >= toFind->end ) {
         // All done break
         BinaryStrFree(&key);
         BinaryStrFree(&value);
         break;
      }

      node = malloc(sizeof(ListNode));
      desc = malloc(sizeof(ChunkDesc));
      node->data = desc;
      desc->present = True;
      desc->startOffset = r->start;
      desc->len  = (r->end >= toFind->end) ? toFind->end-curOff : r->end-curOff;
      desc->targetOffset = *((uint64_t*)value.data);
      curOff += desc->len;
      LIST_ADD_AFTER(node,LIST_LAST(&head));
      BinaryStrFree(&key);
      BinaryStrFree(&value);
      printf("ADDXXX\n");
      if (curOff >= toFind->end ) {
         break;
      }
   } while(SkipListCursorNext(cursor));

   if (curOff < toFind->end) {
      printf("ADDad\n");
      ListNode* node = malloc(sizeof(ListNode));
      ChunkDesc* desc = malloc(sizeof(ChunkDesc));
      node->data = desc;
      desc->present = False;
      desc->startOffset = curOff;
      desc->len = toFind->end-curOff;
      curOff += desc->len;
      LIST_ADD_AFTER(node,LIST_LAST(&head));
   }
   assert(curOff == toFind->end);
   ListNode* node,*next;
   printf("For range %lu,%lu\n",toFind->start,toFind->end);

   LIST_FOR_ALL_SAFE(&head,node,next){
      
      ChunkDesc* desc = node->data;
      LIST_REMOVE(&head,node);
      printf("Present %d , startOffset %lu len %lu targetOffset %lu\n",desc->present,desc->startOffset,desc->len,desc->targetOffset);
      free(node);
      free(desc);
   }
}

void SLRangeTest() {
   BTreeKey key,value;
   uint64_t off =0;
   LongRange range,toFind;
   BTreeKeyOps rangeOps;
   rangeOps.cmp = BTreeKeyRangeCmp;
   //oidOps.cmp = BTreeKeyOidCmp;
   rangeOps.free = BTreeKeyBStrFree;
   rangeOps.copy = BTreeKeyBStrCopy;
   rangeOps.print = BTreeKeyRangePrint;
   SkipList* list = SkipListInit(32,&rangeOps);
   if (!list) {
      printf("Failed to initialize list.\n");
      return;
   }
   range.start = 100;
   range.end = 500;
   key.data = (char*) &range; 
   key.len = sizeof(range);
   off = 100;
   value.data = (char*) &off;
   value.len = sizeof(off);
   SkipListInsert(list,&key,&value);
   SkipListPrint(list);
   range.start = 100;
   range.end = 1000;
   SkipListInsert(list,&key,&value);
   SkipListPrint(list);
   range.start = 10;
   range.end = 50;
   off = 10;
   SkipListInsert(list,&key,&value);
   SkipListPrint(list);
   range.start = 750;
   range.end = 1000;
   off = 750;
   SkipListInsert(list,&key,&value);
   SkipListPrint(list);
   range.start = 550;
   range.end = 700;
   off = 550;
   SkipListInsert(list,&key,&value);
   SkipListPrint(list);
   SkipListCursorTest(list);
   toFind.start = 10;
   toFind.end = 150;
   FindOffsetRange(list,&toFind);
}

int 
main2(int argc, char** argv) {
   int v = atoi(argv[1]);

   BinaryStr key,value;
   double t1,t2;
   int count = 2000000;
   int i;
   BTreeKeyOps oidOps;
   SLRangeTest();
   return; 
   oidOps.cmp = BTreeKeyBStrCmp;
   //oidOps.cmp = BTreeKeyOidCmp;
   oidOps.free = BTreeKeyBStrFree;
   oidOps.copy = BTreeKeyBStrCopy;
   oidOps.print = BTreeKeyBStrPrint;
   SkipList* list = SkipListInit(v,&oidOps);
   if (!list) {
      printf("Failed to initialize list.\n");
      return -1;
   }
/*   bson_oid_t *oids;
  oids = malloc(count*sizeof(*oids));
   t1 = UTime();
   
   for (i=0;i<count;i++) {
      bson_oid_gen(&oids[i]);
      key.data = oids[i].bytes;
      key.len  = 12;
      value = key;
      SkipListInsert(list,&key,&value);
   }
   t2 = UTime();
   printf("Insertion time for %d items %lf\n",t2-t1);
   t1 = UTime();
   for (i=0;i<count;i++) {
      key.data = oids[i].bytes;
      key.len  = 12;
      SkipListSearch(list,&key,&value);
      //assert(!memcmp(key.data,value.data,key.len));
   }
   t2 = UTime();
   printf("Search time for %d items %lf\n",t2-t1);
   return 0;*/
   StrToBStr2("A",&key);
   value = key;
   SkipListInsert(list,&key,&value);
   SkipListPrint(list);
   StrToBStr2("B",&key);
   value = key;
   SkipListInsert(list,&key,&value);
   SkipListPrint(list);
   StrToBStr2("C",&key);
   value = key;
   SkipListInsert(list,&key,&value);
   SkipListPrint(list);
   StrToBStr2("D",&key);
   value = key;
   SkipListInsert(list,&key,&value);
   SkipListPrint(list);
   StrToBStr2("E",&key);
   value = key;
   SkipListInsert(list,&key,&value);
   assert(!SkipListSearch(list,&key,&value));
   SkipListPrint(list);
   SkipListDelete(list,&key);
   printf("After deleteing %s \n",key.data);
   SkipListPrint(list);
   StrToBStr2("D",&key);
   SkipListDelete(list,&key);
   printf("After deleteing %s \n",key.data);
   SkipListPrint(list);
   StrToBStr2("A",&key);
   SkipListDelete(list,&key);
   printf("After deleteing %s \n",key.data);
   SkipListPrint(list);
   StrToBStr2("C",&key);
   SkipListDelete(list,&key);
   printf("After deleteing %s \n",key.data);
   SkipListPrint(list);
   StrToBStr2("B",&key);
   SkipListDelete(list,&key);
   printf("After deleteing %s \n",key.data);
   SkipListPrint(list);
   return 0;
}
#endif
