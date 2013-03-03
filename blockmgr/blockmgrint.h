#ifndef __BLOCKMGR_INT_H__
#define __BLOCKMGR_INT_H__
#include "blockmgr.h"
#ifndef WIN32
#include "blockmgr_linux.h"
#else 
#include "blockmgr_win.h"
#endif
#include "list.h"
#include "hash.h"
#include "platformops.h"
#define BLOCK_MGR_MAGIC 0x4d4f525048204442
#define MIN_CLUSTER_SIZE (128*1024)  //128 kB
#define MAX_CLUSTER_SIZE (32*1024*1024) // 32 MB

#define DISK_BLOCK_SIZE 512

#define NUM_BLOCK_SIZES 16

#pragma pack(1)

// Ondisk structures go here.

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
   BlockMgrDiskPtr userDataOffset;
} BlockMgrHeader;

typedef union DBlockMgrHeader {
   BlockMgrHeader hdr;
   uint8_t pad[DISK_BLOCK_SIZE];
} DBlockMgrHeader;

#define PTR_MAP_SIZE (56+128)

//structure to define each data cluster
typedef struct BlockMgrPtrMap {
   uint64_t blockSize;
   uint64_t numClusters;
   uint32_t freeClusterHint;
   BlockMgrDiskPtr next; 
   uint32_t pad[9];
   BlockMgrDiskPtr ptrs[PTR_MAP_SIZE];
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
typedef struct BlockMgrHdrCache { 
   //Used for finding the next free block. 
   //So this has to be updated at the end of allocation to point to the free block.
   uint8_t isValid;   // If the data in this is valid
   BlockMgrDataClusterHdr* hdr;
   BlockMgrBitmapArr* bArr;
   uint8_t bitMapValid; 
   uint32_t clusterHdrIndex;
   int32_t bitArrIndex;
   struct BlockMgrHandle *handle;
}BlockMgrHdrCache;

typedef struct BlockMgrMMapDesc {
   uint64_t offset;
   uint32_t len;
   ListNode lruMMapNode;
   void* addr;
   void* map;
} BlockMgrMMapDesc;

struct BlockMgrHandle {
   BlockMgrFileHandle *fileHandle;
   BlockMgrHeader *hdr;
   DBlockMgrPtrMap *dataMap;
   DBlockMgrIntClusterMap *intMap;
   uint64_t clusterSizeCache[NUM_BLOCK_SIZES][PTR_MAP_SIZE];
   uint8_t checksumEnabled;
   BlockMgrHdrCache* cstrHeaderCache;
   ListNode mmapLruList;
   uint8_t numMMap;
   uint32_t mmapLen;
   int numNodesInCache;
   int nodeCacheSize;
   ListNode lruNodeList;
   HashTable* nodeCache;
   struct BlockMgrTransactHandle* journalHandle;

   double getFreeBlock;
   double dataWrite;
   uint64_t numDataWrite;
   uint64_t numMMapMiss;
   uint64_t numMMapHit;
   double writeMiss;
   double writeHit;   
   double readLink;
   RWLock rwLock;
};

uint8_t
BlockMgrFileReadDirect(BlockMgrHandle* handle,
                 void *buf,
                 uint32_t size,
                 uint64_t offset);
uint8_t
BlockMgrFileWriteDirect(BlockMgrHandle* handle,
                 void *buf,
                 uint32_t size,
                 uint64_t offset);
uint8_t BlockMgrDropCaches(BlockMgrHandle* handle);

uint8_t BlockMgrJournalCheck(BlockMgrHandle* handle);
void BlockMgrSetLastError(BlockMgrError err);
#define SetBMgrError(handle,err)\
   BlockMgrSetLastError(err);

#endif
