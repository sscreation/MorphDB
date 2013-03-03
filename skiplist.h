#ifndef __SKIP_LIST_H__
#define __SKIP_LIST_H__
#include <stdint.h>
#include "morphdb.h"
#include "stringutil.h"
#include "btreekeyops.h"

struct SkipListNode;

typedef struct SkipListNode {
   struct SkipListNode **forward;
   uint8_t numLevels;
   BinaryStr key;
   BinaryStr value;
}SkipListNode;

typedef struct SkipList {
   uint64_t gen;
   uint64_t numElem;
   uint8_t maxLevel;
   uint8_t currentLevel;
   SkipListNode* header;
   BTreeKeyOps keyOps;
   MorphDBError error;
}SkipList;

typedef struct SkipListCursor {
   uint64_t gen;
   SkipListNode* curNode;
   SkipList* sl;
   BTreeKey startKey;
   BTreeKey endKey;
   MorphDBError error;   
   BTreeKey curKey;
}SkipListCursor;

uint8_t SkipListCursorInit(SkipList* sl,SkipListCursor** cursor);
uint8_t 
SkipListCursorSetStartKey(SkipListCursor* cursor,BTreeKey key);
uint8_t 
SkipListCursorValue(SkipListCursor* cursor,
                   BTreeKey* key,
                   BTreeValue* value);
uint8_t SkipListCursorNext(SkipListCursor* cursor);
void SkipListCursorFree(SkipListCursor* cursor);
SkipList* SkipListInit(uint8_t maxLevel,BTreeKeyOps *keyOps);
uint8_t SkipListSearch(SkipList* list,BinaryStr* key,BinaryStr* outVal);
uint8_t SkipListInsert(SkipList* list,BinaryStr* key,BinaryStr* value,uint8_t insert,uint8_t set);
uint8_t SkipListDelete(SkipList* list,BinaryStr* key);

void SkipListPrint(SkipList* list);

#endif
