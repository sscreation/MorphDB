#include "skiplist.h"
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "btreekeyops.h"
#include <assert.h>
#include "list.h"

#define INTCMP(i1,i2) ((i1) > (i2) ? 1 : ((i1) == (i2) ? 0:-1))


static __thread MorphDBError tSkipListError;

#define SetSkipListError(list,err)\
   tSkipListError = err;

#define SetCursorError(list,err)\
   tSkipListError = err;

void Free(void* ptr){
   free(ptr);
}
#define free Free

void SkipListNodeFree(SkipListNode* node);
int
BinaryStrCmp(BinaryStr* key1,BinaryStr* key2) {
   int len = key1->len < key2->len ? key1->len : key2->len;
   char* k1 = key1->data,*k2 = key2->data;
   int i=0;
   while ((k1[i] == k2[i] ) && (i < len )){
      i++;
   }
   if (i>=len) {
      return INTCMP(key1->len , key2->len);
   }
   return INTCMP(k1[i],k2[i]);
}

SkipListNode*  SkipListNodeInit(uint8_t level,BinaryStr* key , BinaryStr* value) {
   SkipListNode* node;
   node = malloc(sizeof(SkipListNode));
   node->numLevels = level;
   node->forward = malloc(sizeof(SkipListNode*)*level);
   if (!node->forward) {
      free(node);
      return NULL;
   }
   memset(node->forward,0,sizeof(SkipListNode*)*level);
  
   if (key && value) {
      node->key.data = malloc(key->len);
      if (!node->key.data) {
         free(node->forward);
         free(node);
         return NULL;
      }
      node->value.data = malloc(value->len); 
      if (!node->value.data) {
         free(node->key.data);
         free(node->forward);
         free(node);
         return NULL;
      }
      BStrCopy(&node->key,key);
      BStrCopy(&node->value,value);
   } else {
      memset(&node->key,0,sizeof(node->key));
      memset(&node->value,0,sizeof(node->value));
   }
   return node;
}

SkipList* SkipListInit(uint8_t maxLevel,BTreeKeyOps *keyOps){
   SkipList* list = malloc(sizeof(SkipList));
   if (!list) {
      return NULL;
   }
   list->header = SkipListNodeInit(maxLevel,NULL,NULL);
   if (!list->header) {
      free(list);
      return NULL;
   }
   list->gen =0;
   list->numElem = 0;
   list->currentLevel = 1;
   list->maxLevel = maxLevel;
   srand(time(NULL));
   list->keyOps = *keyOps;
   return list;
}

void SkipListFree(SkipList* list) {
   SkipListNode* node = list->header;
   while (node) {
      SkipListNode* next = node->forward[0];
      SkipListNodeFree(node);
      node = next;
   }
   free(list);
}

SkipListNode* SkipListFindLeaf(SkipList* list,BinaryStr* key) {
   SkipListNode* node = list->header;
   int i;
   for(i=list->currentLevel-1;i>=0;i--) {
      while(node->forward[i] && list->keyOps.cmp(&node->forward[i]->key,key)<0){
         node = node->forward[i];
      }
   }

   return node->forward[0];
}

uint8_t SkipListSearch(SkipList* list,BinaryStr* key,BinaryStr* outVal) {
   SkipListNode* node = list->header;
   int i;
   for(i=list->currentLevel-1;i>=0;i--) {
      while(node->forward[i] && list->keyOps.cmp(&node->forward[i]->key,key)<0){
         node = node->forward[i];
      }
   }
   node = node->forward[0];      
   if (!node || list->keyOps.cmp(&node->key,key) != 0) {
      SetSkipListError(list, MORPHDB_NOT_FOUND);
      return False;
   }
   *outVal = node->value;
   if (BinaryStrDup(outVal,&node->value)){

      SetSkipListError(list, MORPHDB_NO_MEMORY);
      return False;
   }
   return True;
}

int SkipListGenLevel(SkipList* list) {
   double i= rand();
   float p =1/3.0;
   int level=1;
   i/=RAND_MAX;
   //printf("RAND =  %lf,%lf\n",i,p);
   while ( i< p && level < list->maxLevel) {
      
      i = rand();
      i /=RAND_MAX;
      //printf("RAND = %lf\n",i);
      level++;
   }
   //printf("Level %d \n",level);
   return level;
}

uint8_t SkipListInsert(SkipList* list,BinaryStr* key,BinaryStr* value,uint8_t insert,uint8_t set) {
   SkipListNode **toUpdate = NULL;
   SkipListNode* node = list->header;
   SkipListNode* newNode;
   int i;
   int level;

   toUpdate = malloc(sizeof(SkipListNode*)*list->maxLevel);
   if (!toUpdate) {
      SetSkipListError(list,MORPHDB_NO_MEMORY); 
      return False;
   }
   for(i=list->currentLevel-1;i>=0;i--) {
      while(node->forward[i] && list->keyOps.cmp(&node->forward[i]->key,key)<0){
         node = node->forward[i];
      }
      toUpdate[i] = node;
   }
   node = node->forward[0];      

   if (node && list->keyOps.cmp(&node->key,key) == 0) {
      BinaryStr newVal;
      if (set) {
         SetSkipListError(list,MORPHDB_ENTRY_EXISTS);   
         return False;
      } 
      // set

      if (BinaryStrDup(&newVal,value)) {
         SetSkipListError(list,MORPHDB_NO_MEMORY); 
         return False;
      }
      BinaryStrFree(&node->value);
      node->value = newVal;
      return True;
   } else {
      // need to insert
      if (!insert) {
         SetSkipListError(list,MORPHDB_NOT_FOUND);   
         return False;
      }
   }

   level = SkipListGenLevel(list);
   if (level > list->currentLevel) {
      for (i=list->currentLevel;i<level;i++) {
         toUpdate[i] = list->header;
      }
      list->currentLevel = level;
   }

   newNode = SkipListNodeInit(level,key,value);
   if (!newNode) {
      free(toUpdate);
      SetSkipListError(list, MORPHDB_NO_MEMORY);
      return False;
   }
   for (i=0;i<level;i++) {
      newNode->forward[i] = toUpdate[i]->forward[i];
      toUpdate[i]->forward[i] = newNode; 
   }
   list->gen++; 
   list->numElem++;
   free(toUpdate);
   return True;
}

uint8_t SkipListDelete(SkipList* list,BinaryStr* key) {
   SkipListNode **toUpdate = NULL;
   SkipListNode* node = list->header;
   SkipListNode* newNode;
   int i;
   int level;

   toUpdate = malloc(sizeof(SkipListNode*)*list->maxLevel);
   if (!toUpdate) {
      SetSkipListError(list, MORPHDB_NO_MEMORY);
      return False;
   }
   for(i=list->currentLevel-1;i>=0;i--) {
      while(node->forward[i] && list->keyOps.cmp(&node->forward[i]->key,key)<0){
         node = node->forward[i];
      }
      toUpdate[i] = node;
   }

   node = node->forward[0];
   if (!node || list->keyOps.cmp(&node->key,key) != 0) {
      SetSkipListError(list, MORPHDB_NOT_FOUND);
      return False;
   }

   for (i=0;i<list->currentLevel;i++) {
      if (toUpdate[i]->forward[i] == node) {
         toUpdate[i]->forward[i] = node->forward[i];
      }
   }
   i = list->currentLevel-1;
   while (!list->header->forward[i]) {
      i--;
   }
   list->currentLevel = i+1;
   list->numElem--;
   free(toUpdate);
   return True;
}

void SkipListPrint(SkipList* list) {
   int i;
   SkipListNode* node = list->header;
   printf("PRINT START \n");
   for(i=0;i<list->currentLevel;i++) {
      node = list->header->forward[i];
      while(node) {
         list->keyOps.print(&node->key);
         node = node->forward[i];
      }
      
      printf("\n");
   }
   printf("PRINT END\n");
}

void SkipListNodeFree(SkipListNode* node) {
   free(node->forward);
   BinaryStrFree(&node->key);
   BinaryStrFree(&node->value);
   free(node);
}

SkipListNode* SkipListNodeCopy(SkipListNode* node) {
   SkipListNode* newNode = malloc(sizeof(*node));
   if (!newNode) {
      return NULL;
   }
   memcpy(newNode,node,sizeof(SkipListNode));
   if (BinaryStrDup(&newNode->key,&node->key)) {
   }
   if (BinaryStrDup(&newNode->value,&node->value)) {
   }
   newNode->forward = malloc(node->numLevels*sizeof(SkipListNode*));
   if (!newNode->forward) {
      free(newNode);
      return NULL;
   }
   memcpy(newNode->forward,node->forward,node->numLevels*sizeof(SkipListNode*));
   return newNode;
}

void SkipListCursorFree(SkipListCursor* cursor) {
   if (cursor->curNode) {
      SkipListNodeFree(cursor->curNode);
   }
   BinaryStrFree(&cursor->curKey);
   BinaryStrFree(&cursor->startKey);
   BinaryStrFree(&cursor->endKey);
   free(cursor);
}

static uint8_t 
SkipListCursorSetPosInt(SkipListCursor* cursor,BTreeKey key,uint8_t *curKeyChanged) {
   SkipList* dbHandle = cursor->sl;
   BTreeKey cur;
   BTreeKey newKey;
   SkipListNode* toCopy;

   if (key.data == NULL) {
      if (cursor->curNode) {
         SkipListNodeFree(cursor->curNode);
         cursor->curNode = NULL;
      }
      toCopy = cursor->sl->header->forward[0];
      if (toCopy) {
         cursor->curNode = SkipListNodeCopy(toCopy);
      } else {
         cursor->curNode = NULL;
      }
   } else {
      if (cursor->curNode) {
         SkipListNodeFree(cursor->curNode);
         cursor->curNode = NULL;
      }
      toCopy = SkipListFindLeaf(dbHandle,&key);
      if (toCopy) {
         cursor->curNode = SkipListNodeCopy(toCopy);
      } else {
         cursor->curNode = NULL;
      }
   }
   
   if (!cursor->curNode) {
      if (cursor->curKey.data && curKeyChanged) {
         *curKeyChanged = True;
      }
      // Dont free the key let it last
      //BinaryStrFree(&cursor->curKey);
      return True;
   }
   if (curKeyChanged) {
      if (cursor->curKey.data && cursor->curNode->key.data) {
         if (BTreeKeyCmp(dbHandle,&cursor->curKey,&cursor->curNode->key)){
            *curKeyChanged = True;
         } else {
            *curKeyChanged = False;
         }
      } else {
         *curKeyChanged = True;
      }

   }

   BinaryStrFree(&cursor->curKey);
   if (!BTreeKeyCopy(dbHandle,&cursor->curKey,&cursor->curNode->key,True)) {
      cursor->error = ENOMEM;
      return False;
   }
   cursor->gen = cursor->sl->gen; 
   return True;
}

uint8_t 
SkipListCursorSetPos(SkipListCursor* cursor,BTreeKey key) {
   return SkipListCursorSetPosInt(cursor,key,NULL);
}

uint8_t 
SkipListCursorSetToStart(SkipListCursor* cursor) {
   return SkipListCursorSetPos(cursor,cursor->startKey);
}

uint8_t 
SkipListCursorInit(SkipList* sl,SkipListCursor** cursor) {
   SkipListCursor* cur;

   cur = malloc(sizeof(SkipListCursor));
   if (!cur) {
      SetSkipListError(sl, MORPHDB_NO_MEMORY);
      return False;
   }
   memset(cur,0,sizeof(*cur));
   cur->startKey.data = NULL;
   cur->startKey.len = 0;

   cur->endKey.data = NULL;
   cur->endKey.len = 0;

   cur->sl = sl;
   if (!SkipListCursorSetToStart(cur)) {
      return False;
   }
   *cursor = cur;
   return True;
}

uint8_t 
SkipListCursorSetStartKey(SkipListCursor* cursor,BTreeKey key) {
   BinaryStrFree(&cursor->startKey);
   if (BinaryStrDup(&cursor->startKey,&key)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }
   if (!SkipListCursorSetToStart(cursor)) {
      return False;
   }
   return True;
}

uint8_t 
SkipListCursorSetEndKey(SkipListCursor* cursor,BTreeKey key) {
   BinaryStrFree(&cursor->endKey);
   if (BinaryStrDup(&cursor->endKey,&key)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }
   return True;
}

uint8_t 
SkipListCursorValue(SkipListCursor* cursor,
                   BTreeKey* key,
                   BTreeValue* value) {
   BTreeKey cur;

   if (cursor->curNode) {
      assert(cursor->curNode->key.len < 20);
   }
   if (cursor->gen != cursor->sl->gen) {
      if (!SkipListCursorSetPos(cursor,cursor->curKey)) {
         return False;
      }
      cursor->gen = cursor->sl->gen;
      if (!cursor->curNode) {
         return False;
      }
   }
   if (!cursor->curNode) {
      SetCursorError(cursor,MORPHDB_CURSOR_END);
      return False;
   }
   assert(cursor->curNode->key.len < 20);
   if (BinaryStrDup(key,&cursor->curNode->key)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }
   if (BinaryStrDup(value,&cursor->curNode->value)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }

   return True;
}

uint8_t 
SkipListCursorNext(SkipListCursor* cursor) {
   uint8_t curKeyChanged=  False;
   SkipList *db = cursor->sl;
   BTreeKey cur;
   SkipListNode* nextNode;

   if (cursor->gen != db->gen) {
      if (!SkipListCursorSetPosInt(cursor,cursor->curKey,&curKeyChanged)) {
         return False;
      }
      cursor->gen = db->gen;
   }

   if (cursor->curNode && cursor->endKey.data && BTreeKeyCmp(db,&cursor->curKey,&cursor->endKey) > 0) {
      cursor->error = MORPHDB_CURSOR_END;
      return False;
   }
   if (curKeyChanged) {
      // just check whether the key exceeds null
      if (!cursor->curNode) {
         cursor->error = MORPHDB_CURSOR_END;
         return False;
      }
      // already moved to next position
      return True;
   }

   nextNode = cursor->curNode->forward[0];
   if (nextNode) {
      cursor->curNode = SkipListNodeCopy(nextNode);
      if (!cursor->curNode) {
         cursor->error = MORPHDB_NO_MEMORY;
      }
   } else {
      cursor->curNode = nextNode;
      cursor->error = MORPHDB_CURSOR_END;
      return False;
   }
   if (cursor->curNode && cursor->endKey.data && BTreeKeyCmp(db,&cursor->curKey,&cursor->endKey) > 0) {
      cursor->error = MORPHDB_CURSOR_END;
      return False;
   }
   BinaryStrFree(&cursor->curKey);
   if (BinaryStrDup(&cursor->curKey,&cursor->curNode->key)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }

   return True;
}


uint8_t SkipListDBSet(void* beData,BinaryStr key,BinaryStr value,uint8_t insert) {
   return SkipListInsert(beData,&key,&value,insert,True);
}

uint8_t SkipListDBInsert(void* beData,BinaryStr key,BinaryStr value) {
   return SkipListInsert(beData,&key,&value,True,False);
}

uint8_t SkipListDBDelete(void* beData,BinaryStr key) {
   return SkipListDelete(beData,&key);
}

uint8_t SkipListDBCursorInit(void* beData,void** cursor) {
   
   return SkipListCursorInit(beData,(SkipListCursor**)cursor);
}

uint8_t SkipListDBGet(void* beData,BinaryStr key,BinaryStr* value) {
   return SkipListSearch(beData,&key,value);
}

void SkipListDBRemove(void* beData) {
   SkipListFree(beData);
}

uint8_t SkipListDBSync(void* beData) {
   return True;
}

void SkipListDBClose(void* beData) {
   SkipListFree(beData);
}

MorphDBError SkipListDBGetLastError(void* beData) {
   return tSkipListError;
   return ((SkipList*)beData)->error;
}

const char* SkipListDBGetLastErrorString(void* beData) {
   //
}

// cursor ops
uint8_t SkipListDBCursorSetEndKey(void* cursor,BTreeKey key) {
   return SkipListCursorSetEndKey(cursor,key);
}

uint8_t SkipListDBCursorSetStartKey(void* cursor,BTreeKey key) {
   return SkipListCursorSetStartKey(cursor,key);
}

uint8_t SkipListDBCursorNext(void* cursor) {
   return SkipListCursorNext(cursor);
}

uint8_t SkipListDBCursorValue(void* cursor,BTreeKey* key,BTreeValue* value) {
   return SkipListCursorValue(cursor,key,value);
}

uint8_t SkipListDBCursorSetPos(void* cursor,BTreeKey key) {
   return SkipListCursorSetPos(cursor,key);
}

uint8_t SkipListDBCursorPrev(void* cursor) {
   return False;
}

uint8_t SkipListDBCursorSetToStart(void* cursor) {
   return SkipListCursorSetToStart(cursor);
}

uint8_t SkipListDBCursorSetToEnd(void* cursor) {
   SkipListCursor* cur = cursor;
   return SkipListCursorSetPos(cursor,cur->endKey);
}

MorphDBError SkipListDBCursorGetLastError(void* cursor) {
   return tSkipListError;
   return ((SkipListCursor*)cursor)->error;
}

void SkipListDBCursorFree(void* cursor) {
   SkipListCursorFree(cursor);
}

void SkipListDBSetKeyOps(void* beData,BTreeKeyOps keyOps) {
   SkipList* sl = beData;
   sl->keyOps = keyOps;
}

const char* SkipListDBCursorGetLastErrorString(void* cursor) {
}

#undef free
DBOps skipListDBOps = {
   .set = SkipListDBSet,
   .insert = SkipListDBInsert,
   .del = SkipListDBDelete,
   .remove = SkipListDBRemove,
   .close = SkipListDBClose,
   .cursorInit = SkipListDBCursorInit,
   .get = SkipListDBGet,
   .sync = SkipListDBSync,
   .setKeyOps = SkipListDBSetKeyOps,
   .getLastError = SkipListDBGetLastError,
   .getLastErrorString = NULL
};


DBCursorOps skipListCursorOps = {
   .setEndKey = SkipListDBCursorSetEndKey,
   .setStartKey = SkipListDBCursorSetStartKey,
   .next  = SkipListDBCursorNext,
   .prev  = NULL,
   .value = SkipListDBCursorValue,
   .setPos = SkipListDBCursorSetPos,
   .getLastError = SkipListDBCursorGetLastError,
   .getLastErrorString = NULL,
   .free = SkipListDBCursorFree,
};

MorphDBError SkipListMDBInit(char* name,int maxLevels,MorphDB** outDB) {
   MorphDB* db = malloc(sizeof(MorphDB));
   SkipList* sl;
   BTreeKeyOps oidOps;
   oidOps.cmp = BTreeKeyBStrCmp;
   //oidOps.cmp = BTreeKeyOidCmp;
   oidOps.free = BTreeKeyBStrFree;
   oidOps.copy = BTreeKeyBStrCopy;
   oidOps.print = BTreeKeyBStrPrint;
   if (!db) {
      return MORPHDB_NO_MEMORY;   
   }
   db->beData = SkipListInit(maxLevels,&oidOps);

   if (!db->beData) {
      free(db);
      return MORPHDB_NO_MEMORY;   
   }
   snprintf(db->beName,50,"skiplistmdb");
   db->dbOps = skipListDBOps; 
   db->cursorOps = skipListCursorOps; 
   *outDB = db;
   return MORPHDB_OK;
}
