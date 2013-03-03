#ifndef __MMDB_KEY_VAL_WIN_H__
#define __MMDB_KEY_VAL_WIN_H__

//#include "stdint_win.h"
/*#include <stdint.h>
#include "stringutil.h"
#include "list.h"
#include "hash.h"

#include <Windows.h>
#include "mmdbdef.h"*/

#define inline __inline  // windows needs this.

typedef struct BlockMgrFileHandle {
   HANDLE fd;
   uint64_t error;
   int osError;
}BlockMgrFileHandle;

#if 0
#pragma pack(1) 
#pragma pack(show)
typedef struct BlockMgrDiskPtr {
   uint64_t blockSize:4;
   uint64_t intNode:1;
   uint64_t unused:3;
   uint64_t offset:56;
} BlockMgrDiskPtr;

typedef struct BlockMgrHeader {
   uint64_t magic;
   uint32_t majorVersion;
   uint32_t minorVersion;
   uint32_t minBlockSize;
   uint64_t maxBlockSize;
   uint64_t features;
   uint32_t keySize;
   uint32_t treeOrder;
   uint32_t intNodeSize;
   uint64_t minClusterSize;
   uint64_t maxClusterSize;
   BlockMgrDiskPtr intNodeClusterOffset;   
   BlockMgrDiskPtr dataClusterMapStartOffset;
   BlockMgrDiskPtr rootNodeOffset;
} BlockMgrHeader;

typedef union DBlockMgrHeader {
   BlockMgrHeader hdr;
   uint8_t pad[DISK_BLOCK_SIZE];
} DBlockMgrHeader;

//structure to define each data cluster
typedef struct BlockMgrPtrMap {
   uint64_t blockSize;
   uint64_t numClusters;
   uint32_t freeClusterHint;
   BlockMgrDiskPtr next; 
   uint32_t pad[9];
   BlockMgrDiskPtr ptrs[56];
} BlockMgrPtrMap;

typedef union DBlockMgrPtrMap {
   BlockMgrPtrMap map;
   uint8_t pad[DISK_BLOCK_SIZE];
} DBlockMgrPtrMap;

//structure to define internal node cluster
typedef struct BlockMgrIntClusterMap {
   uint64_t intNodeSize;
   uint64_t numClusters;
   uint32_t freeClusterHint;
   BlockMgrDiskPtr next; 
   uint32_t pad[9];
   BlockMgrDiskPtr ptrs[56];
} BlockMgrIntClusterMap;

//Each data cluster has header
typedef struct BlockMgrDataClusterHdr {
   uint32_t totalResources;
   uint32_t numFree;
   uint32_t blockSize;
   uint32_t freeHint;
   uint32_t numBitArray;
   BlockMgrDiskPtr dataStartOffset;
   uint32_t pad[9];
   uint32_t bitmap[112]; // (DISK_BLOCK_SIZE-64)/12
} BlockMgrDataClusterHdr;

typedef struct BlockMgrBitmapArr {
   uint32_t numFree;
   uint32_t totalResources;
   uint32_t freeHint;
   uint32_t bitmap[125]; 
} BlockMgrBitmapArr;

typedef union DBlockMgrIntClusterMap {
   BlockMgrIntClusterMap map;
   uint8_t pad[DISK_BLOCK_SIZE];
} DBlockMgrIntClusterMap;

#define _K(tree,intNode,idx) ((intNode)->keys+(idx)*((tree)->keySize))

typedef struct BlockMgrINode {
   uint32_t numKeys;
   uint32_t isLeaf;
   BlockMgrDiskPtr parent;
   BlockMgrDiskPtr leafNext;
} BlockMgrINode;

typedef struct BlockMgrDataDesc {
   BlockMgrDiskPtr next;
   uint32_t checksum;
   uint32_t dataLen;
} BlockMgrDataDesc;
//XXXXXXXXXXX Unpack pragma here
#pragma pack()

struct BlockMgrIntNode;

typedef struct BlockMgrIntNode {
   BlockMgrINode * dNode;
   //BlockMgrDiskPtr parent;
   struct BlockMgrIntNode* parent;
   //uint8_t isLeaf;
   uint8_t isDirty;
   uint8_t pinned;
   uint8_t *keys;
   BlockMgrDiskPtr *ptrs;
   BlockMgrDiskPtr nodeOffset;
   char* diskNode;
   ListNode lruNode;
   // keys []
   // ptrs []
}BlockMgrIntNode;

typedef struct MKVError {
   uint32_t customError;
   int posixError;
}MKVError;

struct BlockMgrHandle;

typedef struct BlockMgrHdrCache { //Used for finding the next free block. So this has to be updated at the end of allocation to point to the free block.
   uint8_t isValid;   // If the dara in this is valid
   BlockMgrDataClusterHdr* hdr;
   BlockMgrBitmapArr* bArr;
   uint8_t bitMapValid; 
   uint32_t clusterHdrIndex;
   int32_t bitArrIndex;
   struct BlockMgrHandle *handle;
}BlockMgrHdrCache;

typedef struct BlockMgrHandle {
   BlockMgrFileHandle* fileHandle;
   BlockMgrHeader *hdr;
   DBlockMgrPtrMap *dataMap;
   DBlockMgrIntClusterMap *intMap;
   int error;
   BlockMgrHdrCache* cstrHeaderCache;
   double getFreeBlock;
}BlockMgrHandle;

typedef BinaryStr BTreeKey;
/*typedef struct BTreeValue {
   int value;
}BTreeValue;*/

typedef BinaryStr BTreeValue;

typedef int (*BTreeKeyCompareFn)(BTreeKey *key1,BTreeKey* key2);
typedef void (*BTreeKeyPrintFn)(BTreeKey* key);
typedef uint8_t (*BTreeKeyCopyFn)(BTreeKey* dst,BTreeKey* src,uint8_t new);

typedef void (*BTreeKeyFreeFn)(BTreeKey* key);


struct BTreeNode;

typedef struct BTreeKeyOps {
   BTreeKeyCompareFn cmp;
   BTreeKeyPrintFn print;
   BTreeKeyCopyFn copy;
   BTreeKeyFreeFn free;
}BTreeKeyOps;

typedef struct BTreeNodePtr{
   struct BTreeNode *left;
   struct BTreeNode *right;
}BTreeNodePtr;

typedef struct BTreeNode {
   uint8_t isLeaf;
//   int treeOrder;
   BTreeKey* keys;
   struct BTreeNode** ptrs;
   int numKeys;
   struct BTreeNode* leafNext;
   struct BTreeNode* parent;
   //int numEntries;
}BTreeNode;

typedef struct BTree {
   BTreeNode* root;
   int order;
   int64_t numEntries;
   BTreeKeyOps keyOps;
}BTree;


typedef struct BlockMgrDiskTree {
//   BTree* mTree;
   BTreeKeyOps keyOps;
   HashTable* nodeCache;
   BlockMgrIntNode* root;
   BlockMgrHandle *handle;
   ListNode  lruNodeList;
   int error;
   uint32_t numIntNodes;
   uint32_t numNodesInCache;
   uint32_t nodeCacheSize;
   uint32_t keySize;
   double dataWriteTime;
   double intNodeWriteTime;
} BlockMgrDiskTree;

#define ISLEAF(node) (((node)->dNode)->isLeaf)
#define LNEXT(node) (((node)->dNode)->leafNext)
#define THANDLE(tree) ((tree)->handle)
#define THDR(tree) (THANDLE(tree)->hdr)
#define TINTNODE_SIZE(tree) (THANDLE(tree)->hdr->intNodeSize)
#define THDR(tree) (THANDLE(tree)->hdr)
#define TORDER(tree) (THDR(tree)->treeOrder)
#define TKSIZE(tree) (THDR(tree)->keySize)
#define _K_ARR(tree,node,i) ((node)->keys+(i)*(tree)->keySize)
#define NKEYS(node) ((node)->dNode->numKeys)
#define KEY_TO_BDATA1(tree,node,i,bData) (bData)->data = _K_ARR(tree,node,i); (bData)->len = TKSIZE(tree);
#define DBTreeIsNodeFull(tree,node) (NKEYS(node) >=  TORDER(tree)-1)

uint8_t
DBTreeLoadNode(BlockMgrDiskTree* tree,BlockMgrDiskPtr nodePtr,BlockMgrIntNode* parent,BlockMgrIntNode ** outNode);
uint8_t BlockMgrAllocateDataBytes(BlockMgrHandle* handle,uint32_t bytes,BlockMgrDiskPtr *diskPtr);
int BlockMgrFormatFile(char* fileName,uint64_t features,uint32_t keySize);
uint8_t BlockMgrDataRead(BlockMgrDiskTree* tree,BlockMgrDiskPtr ptr,BTreeValue *val);
uint8_t BlockMgrDataWrite(BlockMgrDiskTree* tree,BTreeValue* val,BlockMgrDiskPtr * ptr);
uint8_t BlockMgrIntNodeInit(BlockMgrDiskTree* tree,BlockMgrIntNode* parent,BlockMgrIntNode** outNode);
uint8_t BlockMgrAllocIntNode(BlockMgrHandle* handle,BlockMgrDiskPtr* diskPtr);
int BlockMgrInitHandle(char* fileName,int mode,BlockMgrHandle ** outHandle);
uint8_t BlockMgrIntNodeWrite(BlockMgrDiskTree* tree,BlockMgrIntNode* node);
uint8_t BlockMgrInsertNodeToCache(BlockMgrDiskTree* tree, BlockMgrIntNode* node);

//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
uint8_t _BlockMgrGetFileSize(BlockMgrFileHandle* handle,uint64_t *size);
uint8_t _BlockMgrFileTruncate(BlockMgrFileHandle* handle, uint64_t size);
uint8_t _BlockMgrFileOpen(BlockMgrFileHandle* handle,char* fileName);
uint8_t _BlockMgrFileOpenRW(BlockMgrFileHandle* handle,char* fileName);
uint8_t _BlockMgrFileRead(BlockMgrFileHandle* handle,void *buffer,size_t size,size_t offset);
uint8_t _BlockMgrFileWrite(BlockMgrFileHandle* handle,void *buffer,size_t size,uint64_t offset);
uint8_t _BlockMgrFileClose(BlockMgrFileHandle* handle);
double UTime();
#endif
#endif
