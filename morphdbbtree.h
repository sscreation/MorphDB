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

#ifndef __DB_TREE_H__
#define __DB_TREE_H__

#include "stringutil.h"
#include "blockmgr.h"
#include "list.h"
#include "btreekeyops.h"
#include "morphdb.h"
#include "hash.h"

#define MORPHDB_TREE_MAGIC 0x12345678
#pragma pack(1)

typedef BinaryStr BTreeKey;
typedef BinaryStr BTreeValue;

typedef struct BTreeNodePtr{
   struct BTreeNode *left;
   struct BTreeNode *right;
}BTreeNodePtr;

typedef struct MorphDBTreeHeader {
   uint32_t magic;
   uint32_t majorVersion;
   uint32_t minorVersion;
   uint32_t treeOrder;
   BlockMgrDiskPtr rootNodeOffset;
   BlockMgrDiskPtr countOffset;
   uint32_t countDirty;
}MorphDBTreeHeader;

typedef struct MorphDBIntNodeDesc {
   uint8_t isLeaf;
   uint32_t totalLength;
   uint32_t numKeys;
   uint32_t actualDataLength;
   uint32_t keyPtrs[4];
   uint32_t ptrPtrs[4];
   BlockMgrDiskPtr leafNext;
} MorphDBIntNodeDesc;

#pragma pack()

#define ISLEAF(node) (((node)->desc)->isLeaf)
#define LNEXT(node) (((node)->desc)->leafNext)
#define THANDLE(tree) ((tree)->handle)
//#define THDR(tree) (THANDLE(tree)->hdr)
#define TINTNODE_SIZE(tree) (THANDLE(tree)->hdr->intNodeSize)
#define THDR(tree) (tree->hdr)
#define TORDER(tree) (THDR(tree)->treeOrder)
//#define TKSIZE(tree) (THDR(tree)->keySize)
#define _K_ARR(tree,node,i) ((node)->keys+(i)*(tree)->keySize)
#define NKEYS(node) ((node)->desc->numKeys)
#define KEY_TO_BDATA1(tree,node,i,bData) (bData)->data = _K_ARR(tree,node,i); (bData)->len = 12;
#define DBTreeIsNodeFull(tree,node) (NKEYS(node) >=  TORDER(tree)-1)

#define DBTreeIsNodeHalfFull(tree,node) (NKEYS(node) >= CEIL(TORDER(tree),2)-1)
#define DBTreeMinNodes(tree) (CEIL(TORDER(tree),2)-1)

struct MorphDBIntNode;

typedef struct MorphDBIntNode {
//   MorphDBINode * dNode;
   //BlockMgrDiskPtr parent;
   struct MorphDBIntNode* parent;
   //uint8_t isLeaf;
   uint8_t isDirty;
   uint8_t pinned;
//   uint8_t *keys;
//   BlockMgrDiskPtr *ptrs;
   BlockMgrDiskPtr nodeOffset;
//   char* diskNode;
   ListNode lruNode;
   void* buf;
   MorphDBIntNodeDesc *desc;
   uint32_t *keyPtrs;
   uint32_t *ptrPtrs;
   uint32_t *keyTable;
   uint32_t *ptrTable;

   // keys []
   // ptrs []
}MorphDBIntNode;

typedef struct MorphDBDiskTree {
//   BTree* mTree;
   BTreeKeyOps keyOps;
   HashTable* nodeCache;
   MorphDBIntNode* root;
   BlockMgrHandle *handle;
   ListNode  lruNodeList;
   uint64_t gen;
   //int error;
   uint32_t numIntNodes;
   uint32_t numNodesInCache;
   uint32_t nodeCacheSize;
   uint32_t keySize;
   double dataWriteTime;
   double intNodeWriteTime;
   MorphDBTreeHeader *hdr;
   uint64_t count;
} MorphDBDiskTree;

typedef struct MorphDBDiskTree* BlockMgrHandlePtr;

typedef struct MorphDBBTreeCursor {
   BlockMgrHandlePtr db;
   uint64_t gen;
   MorphDBError error;
   BTreeKey startKey;
   BTreeKey endKey;
   BTreeKey curKey;
   MorphDBIntNode* curNode;
   uint32_t curIdx;
} MorphDBBTreeCursor;

static uint8_t MorphDBBTreeCursorInit(MorphDBDiskTree* db,MorphDBBTreeCursor** cursor);
static uint8_t MorphDBBTreeCursorSetEndKey(MorphDBBTreeCursor* cursor,BTreeKey key);
static uint8_t MorphDBBTreeCursorSetStartKey(MorphDBBTreeCursor* cursor,BTreeKey key);
static uint8_t MorphDBBTreeCursorNext(MorphDBBTreeCursor* cursor);
static uint8_t MorphDBBTreeCursorValue(MorphDBBTreeCursor* cursor,BTreeKey* key,BTreeValue* value);
static uint8_t MorphDBBTreeCursorSetPos(MorphDBBTreeCursor* cursor,BTreeKey key);

static uint8_t MorphDBBTreeCursorDeleteValue(MorphDBBTreeCursor* cursor);
static uint8_t MorphDBBTreeCursorPrev(MorphDBBTreeCursor* cursor);
static uint8_t MorphDBBTreeCursorSetToStart(MorphDBBTreeCursor* cursor);
static uint8_t MorphDBBTreeCursorSetToEnd(MorphDBBTreeCursor* cursor);
static uint8_t MorphDBIntNodeAddKey(MorphDBIntNode* node,BTreeKey key,int pos);
static uint8_t MorphDBIntNodeDeleteKey(MorphDBIntNode* node,int pos);

static uint8_t MorphDBIntNodeWrite(MorphDBDiskTree* tree,MorphDBIntNode* node,uint8_t* newPtr);
static uint8_t MorphDBInsertNodeToCache(MorphDBDiskTree* tree, MorphDBIntNode* node);
static uint8_t MorphDBIntNodeInit(MorphDBDiskTree* tree,MorphDBIntNode* parent,MorphDBIntNode** outNode);
void MorphDBBTreeCursorFree(MorphDBBTreeCursor* cursor);

static uint8_t
DBTreeLoadNode(MorphDBDiskTree* tree,BlockMgrDiskPtr nodePtr,MorphDBIntNode* parent,MorphDBIntNode ** outNode);
#endif
