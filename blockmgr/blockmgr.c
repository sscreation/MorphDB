#include "blockmgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <errno.h>
#include <assert.h>
#include "fileops.h"
#include "blockmgrtransact.h"
#include "blockmgrint.h"


#define MIN_RES_CLUSTER 4
#define CEIL(a,b) ((a)%(b) == 0 ? (a)/(b) : ((a)/(b))+1)

#define USE_CACHE 1

#define MAX_LINKS 4
#define BLOCKMGR_NULL_PTR {0,0,0}
void Free2(void* ptr){
   printf("BMGR Freeing %p \n",ptr);
   free(ptr);
}

void* Malloc2(uint32_t len) {
   void* ptr = malloc(len);
   printf("BMGR Allocing %p %u \n",ptr,len);
   return ptr;
}
//#define malloc Malloc2
//#define free Free2
typedef struct BlockMgrMDataDesc {
   BlockMgrDiskPtr startPtr;
   BlockMgrDataDesc desc[MAX_LINKS];
   uint32_t totalSize;
}BlockMgrMDataDesc;

typedef struct BlockMgrMDataDescLRU {
   BlockMgrMDataDesc desc;
   ListNode lruNode;
}BlockMgrMDataDescLRU;

uint32_t crcTable[256];
#define CRC_POLY 0xEDB88320

uint8_t
BlockMgrDataFreeWithLock(BlockMgrHandle* handle, 
                 BlockMgrDiskPtr startPtr);

uint8_t 
BlockMgrDataWriteWithLock(BlockMgrHandle* handle,
                 BTreeValue* val,
                 BlockMgrDiskPtr *startPtr, // IN/OUT
                 uint8_t *newPtr);

uint8_t BlockMgrDataReadWithLock(BlockMgrHandle* handle,BlockMgrDiskPtr ptr,BTreeValue *val);
/* Run this function previously */
void InitCRCTable() {
   int i;
   for (i=0;i<256;i++) {
      int j=0;
      uint32_t c = i;
      for (j=0;j<8;j++) {
         c = (c & 1) ? (CRC_POLY ^ (c >> 1)) : (c >> 1);
      }
      crcTable[i] = c;
   }
}
 
uint32_t CRC32(uint8_t *buf,uint32_t len) {
   uint32_t c = 0xFFFFFFFF;
   int i;
   for (i = 0; i < len; i++) {
      c = crcTable[(c ^ buf[i]) & 0xFF] ^ (c >> 8);
   }
   return c ^ 0xFFFFFFFF;
}

int BlockMgrCreateCache(BlockMgrHandle* handle);
uint64_t dataWriteTimeUS;

static __thread BlockMgrError tBMgrError;

#define BMgrGetError(handle)\
   tBMgrError

#define MAX_NUM_MMAP 10
#define MAX_MMAP_LEN (100*1024*1024)

BlockMgrError BlockMgrGetLastError() {
   return tBMgrError;
}

void BlockMgrSetLastError(BlockMgrError err) {
   tBMgrError = err;
}

void BlockMgrVerifyMMapList(BlockMgrHandle* handle) {
   ListNode* node;
   printf("Inverification \n");
   LIST_FOR_ALL(&handle->mmapLruList,node) {
      BlockMgrMMapDesc *desc = NULL;
      assert(node->data);
      desc = node->data;
      assert(desc->addr);
      printf("%p %p\n",desc,desc->addr);
   }
   printf("Inverification END\n");
}

uint8_t
BlockMgrAddMMap(BlockMgrHandle* handle,uint64_t offset,uint32_t len){
   BlockMgrMMapDesc *desc = NULL;
   if (len > MAX_MMAP_LEN) {
      return False;
   }
   if (handle->journalHandle) {
      return False;
   }
   //return True;
   if (offset % 4096) {
      offset -= (offset%4096);
   }
   if (len % 4096) {
      len -= (len%4096);
   }
   assert(offset % 4096 == 0);
   assert(len %4096 == 0);
   if (handle->numMMap >= MAX_NUM_MMAP) {
      //assert(0);
      BlockMgrMMapDesc* desc = LIST_LAST(&handle->mmapLruList)->data;
   assert(desc->addr);
      LIST_REMOVE(&handle->mmapLruList,&desc->lruMMapNode);
   assert(desc->addr);
      _BlockMgrFileUnmap(handle->fileHandle,desc->addr,desc->map,desc->len);
      //printf("Freeing %p\n",desc);
      free(desc);
      handle->numMMap--;
   }
   desc = malloc(sizeof(*desc));
   if (!desc) {
      SetBMgrError(handle,BLOCKMGR_NO_MEMORY);
      return False;
   }
   desc->offset = offset;
   desc->len = len;
   desc->addr = NULL;
   desc->map = NULL;
   if (!_BlockMgrFileMap(handle->fileHandle,desc->offset,desc->len,&desc->addr,&desc->map)){
      //assert(0);
      free(desc);
      return False;
   }

   handle->numMMap++;
   //printf("Adding mmap %lu %u %p %p %d\n",offset,len,desc,desc->addr,handle->numMMap);
   desc->lruMMapNode.data = desc;
   LIST_ADD_BEFORE(&desc->lruMMapNode,LIST_FIRST(&handle->mmapLruList));

   return True;
}

uint8_t
BlockMgrFileWriteDirect(BlockMgrHandle* handle,
                 void *buf,
                 uint32_t size,
                 uint64_t offset) {
   ListNode* node,*tmpNode;
   BlockMgrMMapDesc *desc = NULL;
   uint64_t tmpOffset = offset;
   uint64_t startOffset = offset;
   uint64_t endOffset = offset +size;
   uint32_t curLen = size;
   double t1,t2;
   
   //printf("Write on %lu for %lu\n",offset,size);
   //t1 = UTime();

   LIST_FOR_ALL_SAFE(&handle->mmapLruList,node,tmpNode){

      desc = node->data;
      if (startOffset >= endOffset) {
         break;
      }
      //printf("mmap %lu startOffset , %u len \n",desc->offset,desc->len);
      if (desc->offset <=startOffset && (desc->offset+desc->len) >= startOffset) {
      LIST_REMOVE(&handle->mmapLruList,node);
      LIST_ADD_BEFORE(node,LIST_FIRST(&handle->mmapLruList));
         //printf("mmap hit %lu startOffset , %u len \n",desc->offset,desc->len);
         void* dataAddr = desc->addr+startOffset-desc->offset;
         uint32_t len = (desc->len+desc->offset) > endOffset ? (endOffset-startOffset): (desc->len+desc->offset - startOffset);
         memcpy(dataAddr,buf+startOffset-offset,len);
         startOffset += len;
         
      }
   }
   if (startOffset < endOffset) {
      //assert(0);
   //printf("MMAP IO MISS %lu %lu\n",startOffset,endOffset);
      if (!_BlockMgrFileWrite(handle->fileHandle,buf+startOffset-offset,endOffset-startOffset,startOffset)) {
         SetBMgrError(handle,handle->fileHandle->error);
         printf("IO FAILED \n");
         return False;
      }
      handle->numMMapMiss++;
      //t2= UTime();
      handle->writeMiss += (t2-t1);
   } else {
      handle->numMMapHit++;
      //t2= UTime();
      handle->writeHit += (t2-t1);
   }
   return True;
}

uint8_t
BlockMgrFileWrite(BlockMgrHandle* handle,
                 void *buf,
                 uint32_t size,
                 uint64_t offset) {
   if (handle->journalHandle) {
      return BlockMgrJournalWrite(handle,buf,size,offset);
   }
   return BlockMgrFileWriteDirect(handle,buf,size,offset);
}


uint8_t
BlockMgrFileReadDirect(BlockMgrHandle* handle,
                 void *buf,
                 uint32_t size,
                 uint64_t offset) {
   ListNode* node;
   BlockMgrMMapDesc *desc = NULL;
   uint64_t tmpOffset = offset;
   uint64_t startOffset = offset;
   uint64_t endOffset = offset +size;
   uint32_t curLen = size;
   double t1,t2;
   
   //printf("Write on %lu for %lu\n",offset,size);
   //t1 = UTime();
   LIST_FOR_ALL(&handle->mmapLruList,node){
      desc = node->data;
      if (startOffset >= endOffset) {
         break;
      }
      //printf("mmap %lu startOffset , %u len \n",desc->offset,desc->len);
      if (desc->offset <=startOffset && (desc->offset+desc->len) >= startOffset) {
         //printf("mmap hit %lu startOffset , %u len \n",desc->offset,desc->len);
         void* dataAddr = desc->addr+startOffset-desc->offset;
         uint32_t len = (desc->len+desc->offset) > endOffset ? (endOffset-startOffset): (desc->len+desc->offset - startOffset);
         memcpy(buf+startOffset-offset,dataAddr,len);
         startOffset += len;
         
      }
   }
   if (startOffset < endOffset) {
      //assert(0);
      //printf("MMAP READ MISS %lu %lu\n",startOffset,endOffset);
      if (!_BlockMgrFileRead(handle->fileHandle,buf+startOffset-offset,endOffset-startOffset,startOffset)) {
         SetBMgrError(handle,handle->fileHandle->error);
         return False;
      }
      handle->numMMapMiss++;
      //t2= UTime();
      handle->writeMiss += (t2-t1);
   } else {
      handle->numMMapHit++;
      //t2= UTime();
      handle->writeHit += (t2-t1);
   }
   return True;
}

static inline uint8_t
BlockMgrFileRead(BlockMgrHandle* handle,
                 void *buf,
                 uint32_t size,
                 uint64_t offset) {
   if (handle->journalHandle) {
      return BlockMgrJournalRead(handle,buf,size,offset);
   }
   return BlockMgrFileReadDirect(handle,buf,size,offset);
}

// Only one in ternal node cluster pointer is allocated initially. 
uint8_t 
BlockMgrInitIntNodeCluster(BlockMgrFileHandle *handle,BlockMgrHeader* hdr) {
   BlockMgrIntClusterMap *map = malloc(512);
   memset(map,0,512);
   map->intNodeSize = hdr->intNodeSize;

   if(!_BlockMgrFileWrite(handle,map,512,_O(&hdr->intNodeClusterOffset))) {
      free(map);
      return False;
   } 
   free(map);
   return True;
}


uint8_t 
BlockMgrInitDataPtrMap(BlockMgrFileHandle *handle,BlockMgrHeader* hdr) {
   BlockMgrPtrMap *map = malloc(sizeof(*map));
   int i;
   memset(map,0,sizeof(*map));
   for (i=0;i<NUM_BLOCK_SIZES;i++) {
      map->blockSize = hdr->minBlockSize << i;
      if (map->blockSize > hdr->maxBlockSize) {
         break;
      }
      if(!_BlockMgrFileWrite(handle,map,sizeof(*map),_O(&hdr->dataClusterMapStartOffset)+i*sizeof(*map))) {
         free(map);
         return False;
      }
   }
   free(map);
   return True;
}

static inline 
uint32_t BlockMgrGetIntNodeSize(uint32_t keySize,uint32_t treeOrder) {
   uint32_t intNodeSize = keySize*(treeOrder)+sizeof(BlockMgrDiskPtr) * (treeOrder+1)+ sizeof(BlockMgrINode);
   
   //intNodeSize = CEIL(intNodeSize,512)*512;
   return intNodeSize;
}

uint32_t 
BlockMgrTreeOrder(uint32_t keySize,uint32_t*  outIntNodeSize) {
   uint32_t minTreeOrder = 32,i,curIntNodeSize;
   uint32_t minIntNodeSize = 1024;
   uint32_t intNodeSize = BlockMgrGetIntNodeSize(keySize,minTreeOrder);

   if (intNodeSize > minIntNodeSize) {
      intNodeSize = CEIL(intNodeSize,512)*512;
   } else {
      intNodeSize = 1024;
   }

   i=minTreeOrder;
   curIntNodeSize = BlockMgrGetIntNodeSize(keySize,i);
   //rew - what is the use of this while loop ?
   while (intNodeSize > curIntNodeSize ) {
      i++;
      curIntNodeSize = BlockMgrGetIntNodeSize(keySize,i); 
   }
   *outIntNodeSize = intNodeSize;
   return 3;
   return i-1;
}


/*static inline uint8_t BlockMgrIsNullPtr(BlockMgrDiskPtr *ptr) {
   return (ptr->offset == 0);
}*/

// id = k when blockSize = 2^k
static inline uint8_t 
BlockMgrBlockSizeToId(BlockMgrHeader* hdr,uint64_t blockSize,uint8_t *id) {
   uint8_t i=0;
   uint64_t tmpBs; 
   if (blockSize % hdr->minBlockSize) {
      return False;
   }
   if (blockSize > hdr->maxBlockSize) {
      return False;
   }
   tmpBs= hdr->minBlockSize;
   for (i=0;i<NUM_BLOCK_SIZES;i++) {
      if ((tmpBs<<i) == blockSize) {
         *id = i;
         return True;
      }
   }
   return False;
}

#define RES_PER_BMAP_ARR (125*32)
#define RES_IN_CLUSTER_HDR (112*32)

uint32_t BlockMgrGetClusterHdrSize(uint64_t blockSize,uint64_t clusterSize) {

   uint32_t maxResources = clusterSize/blockSize;
   uint32_t numBitMapArr = maxResources > RES_IN_CLUSTER_HDR ? CEIL((maxResources-RES_IN_CLUSTER_HDR),RES_PER_BMAP_ARR):0;

   uint32_t headerSize = sizeof(BlockMgrDataClusterHdr)+numBitMapArr*sizeof(BlockMgrBitmapArr);
   return headerSize;
}

//Write the header for each cluster
uint8_t BlockMgrInitCluster(BlockMgrHandle* handle, 
                           uint64_t blockSize,
                           uint64_t clusterSize,
                           BlockMgrDiskPtr ptr,
                           uint8_t dataCluster) 
{
   uint32_t maxResources = clusterSize/blockSize;
   uint32_t numBitMapArr = maxResources > RES_IN_CLUSTER_HDR ? 
      CEIL((maxResources-RES_IN_CLUSTER_HDR),RES_PER_BMAP_ARR):0;

   uint32_t headerSize = BlockMgrGetClusterHdrSize(blockSize,clusterSize);
   int i;
   BlockMgrDataClusterHdr hdr;
   BlockMgrBitmapArr arr;
   //maxResources -= CEIL(headerSize,blockSize);
   hdr.totalResources = maxResources;
   hdr.numFree = maxResources < RES_IN_CLUSTER_HDR ? maxResources:RES_IN_CLUSTER_HDR;
   hdr.freeHint = 0;
   hdr.numBitArray = numBitMapArr;
   hdr.blockSize = blockSize;
   assert(blockSize);
   memset(hdr.bitmap,0xff,sizeof(hdr.bitmap));
   _SO(&hdr.dataStartOffset,_O(&ptr)+headerSize);

   if(!BlockMgrFileWrite(handle,&hdr,sizeof(hdr),_O(&ptr))) {
      return False;
   }

   maxResources -= RES_IN_CLUSTER_HDR;
   if(numBitMapArr > 0) {
      memset(arr.bitmap,0xff,sizeof(arr.bitmap));
       
      for (i=1;i<=numBitMapArr;i++) {
         arr.numFree = arr.totalResources = maxResources > RES_PER_BMAP_ARR ? RES_PER_BMAP_ARR : maxResources; 
         arr.freeHint = 0;
         if(!BlockMgrFileWrite(handle,&arr,sizeof(arr),_O(&ptr)+i*sizeof(arr))) {
            return False;
         } 
          maxResources -= RES_PER_BMAP_ARR;
      }
   }

   return True;
}

//Is cluster size fixed or increases ?
uint8_t BlockMgrAllocDataCluster(BlockMgrHandle* handle,uint64_t blockSize) {
   uint8_t id;
   uint64_t fileSize;
   uint64_t clusterSize;
   BlockMgrPtrMap *map;
   int numClusters = 0;
   BlockMgrDiskPtr tmpPtr;
   uint32_t headerSize;
   if (blockSize > handle->hdr->maxBlockSize) {
      SetBMgrError(handle,BLOCKMGR_BLOCK_TOO_BIG);
      return False;
   }
   //struct stat st;
   /*if (fstat(handle->fd,&st) <0) {
      SetBMgrError(handle,MORPH_DB_IO_ERR;
      handle->posixError = errno;
      return False;
   }*/

   if(!_BlockMgrGetFileSize(handle->fileHandle,&fileSize)) {
      return False;
   }
   clusterSize = handle->hdr->minClusterSize;
   if (!BlockMgrBlockSizeToId(handle->hdr,blockSize,&id)){ // IF minBlockSize is not 512 then this might fail because dataMap[id] assumes each one to be 512 with each pointers occupying 512 bytes.
      SetBMgrError(handle,BLOCKMGR_INVALID_BLOCK_SIZE);
      return False;
   }
   map = &handle->dataMap[id].map;
   while (!BlockMgrIsNullPtr(&map->ptrs[numClusters])) {
      clusterSize = clusterSize <<1;
      numClusters++;
      if (numClusters >= 56) {
         // Extend Pointer map;
         // not supported as of now
         assert(0); 
         break;
      }
   }

   //tmpPtr.intNode = 0;
   tmpPtr.blockSize = id;
   _SO(&tmpPtr,fileSize);
   if (clusterSize > handle->hdr->maxClusterSize) {
      clusterSize = handle->hdr->maxClusterSize;
   }
   if (clusterSize < MIN_RES_CLUSTER * blockSize ) {
      clusterSize = MIN_RES_CLUSTER*blockSize;
   } 
   headerSize = BlockMgrGetClusterHdrSize(blockSize,clusterSize); 
   if (!_BlockMgrFileTruncate(handle->fileHandle,fileSize+clusterSize+headerSize) ) {
      return False;
   }
   //printf(" **Cluster Size %d ,%lu\n",clusterSize,_O(&tmpPtr));
   if (!BlockMgrInitCluster(handle,blockSize,clusterSize,tmpPtr,True)) {
      if (!_BlockMgrFileTruncate(handle->fileHandle,fileSize)) {
         return False;
      }
   }
   handle->clusterSizeCache[id][numClusters] = fileSize + clusterSize+headerSize;
   if(numClusters <= PTR_MAP_SIZE) {
      map->ptrs[numClusters] = tmpPtr;
   }
   BlockMgrAddMMap(handle,_O(&tmpPtr),clusterSize+headerSize);
   map->numClusters ++ ;
   //XXX To do - write above two changes to the disk.
   
   if(!BlockMgrFileWrite(handle,map,sizeof(*map),4096+id*sizeof(BlockMgrPtrMap))) {
      return False;
   }

   return True;
}

//Is internal cluster size fixed ?
uint8_t BlockMgrAllocIntCluster(BlockMgrHandle* handle) {
   BlockMgrIntClusterMap* map = &handle->intMap->map;
   uint64_t clusterSize = handle->hdr->minClusterSize;
   int i=0;
   //struct stat st;
   
   uint32_t blockSize = handle->hdr->intNodeSize;
   uint32_t headerSize;
   uint64_t fileSize;
   BlockMgrDiskPtr tmpPtr;
   if(!_BlockMgrGetFileSize(handle->fileHandle,&fileSize)) {
      return False;
   }
   /*
   if (fstat(handle->fd,&st) <0) {
      SetBMgrError(handle,MORPH_DB_IO_ERR;
      handle->posixError = errno;
      return False;
   }
   */
   while (!BlockMgrIsNullPtr(&map->ptrs[i])) {
      clusterSize = clusterSize <<1;
      i++;
   }
   if (clusterSize > handle->hdr->maxClusterSize) {
      clusterSize = handle->hdr->maxClusterSize;
   }
   headerSize = BlockMgrGetClusterHdrSize(blockSize,clusterSize); 
   if (!_BlockMgrFileTruncate(handle->fileHandle,fileSize+clusterSize+headerSize)) {
      return False;
   }
 
   //tmpPtr.intNode = 1;
   tmpPtr.blockSize = 0;
   _SO(&tmpPtr,fileSize);
   if (!BlockMgrInitCluster(handle,blockSize,clusterSize,tmpPtr,False)) {
      if (!_BlockMgrFileTruncate(handle->fileHandle,fileSize)) {
         return False;
      }
   }
   return True;
}

uint8_t
BlockMgrSetUserData(BlockMgrHandle* handle,BTreeValue* value){
   BlockMgrDiskPtr ptr,oldPtr;
   uint8_t newPtr;
   _SO(&ptr,0);
   if (PlatformRWGetWLock(handle->rwLock) != 0) {
      return False;
   }
   if (!BlockMgrDataWriteWithLock(handle,value,&ptr,&newPtr)) {
      PlatformRWUnlock(handle->rwLock);
      return False;
   }
   oldPtr = handle->hdr->userDataOffset;
   handle->hdr->userDataOffset = ptr;
   if(!BlockMgrFileWrite(handle,handle->hdr,sizeof(DBlockMgrHeader),0)) {
      PlatformRWUnlock(handle->rwLock);
      return False;
   }
   BlockMgrDataFreeWithLock(handle,oldPtr);
   if (PlatformRWUnlock(handle->rwLock) != 0) {
      return False;
   }
   return True;
}

BlockMgrError
BlockMgrFormatFile(char* fileName,uint64_t features) {
   BlockMgrFileHandle fileHandle;
   BlockMgrHeader * hdr;
   BlockMgrHandle *h;
   //BlockMgrDiskTree tree;
   //BlockMgrIntNode *rootNode;
   if(!_BlockMgrFileOpen(&fileHandle,fileName)) {
      return fileHandle.error;
   }
   _BlockMgrFileTruncate(&fileHandle,0);

   hdr = malloc(512);
   if (!hdr) {
      return ENOMEM;
   }
   //printf("\nsizeof hdr %d",sizeof(BlockMgrDataClusterHdr));
   memset(hdr,0,512);
   hdr->magic = BLOCK_MGR_MAGIC;
   hdr->majorVersion = 0;
   hdr->minorVersion = 1;
   hdr->minBlockSize = 128;
   hdr->maxBlockSize = hdr->minBlockSize << 15;
   hdr->features  = features;
   hdr->minClusterSize = MIN_CLUSTER_SIZE;
   hdr->maxClusterSize = MAX_CLUSTER_SIZE;
   /*_SO(&hdr->intNodeClusterOffset,512);
   if(!BlockMgrInitIntNodeCluster(&fileHandle,hdr)) { 
      free(hdr);
      return False;
   }*/
   _SO(&hdr->dataClusterMapStartOffset,4096); 
   if(!BlockMgrInitDataPtrMap(&fileHandle,hdr)) {
      free(hdr);
      return False;
   }
 
   // initialize dbheader with rootnode info.
   //hdr->rootNodeOffset.intNode = 1;
   hdr->rootNodeOffset.offset = 0;   
   // write the DB header to the disk.
   if(!_BlockMgrFileWrite(&fileHandle,hdr,sizeof(DBlockMgrHeader),0)) {
      free(hdr);
      return -1;
   }

   /*if(BlockMgrInitHandle(fileName,0,&h)) {
      return -1;
   }

   LIST_INIT(&tree.lruNodeList);
   tree.handle = h;
   tree.nodeCache = NULL;
   if (!BlockMgrIntNodeInit(&tree,NULL,&rootNode)) {
      assert(0);
      return EINVAL;
   }
   //rootNode->dNode->isLeaf = True;
   //rootNode->dNode->numKeys = 100000;
   memset(&rootNode->dNode->leafNext,0,sizeof(rootNode->dNode->leafNext));
   
   BlockMgrIntNodeWrite(&tree,rootNode);*/
   /*BlockMgrAllocIntCluster(h);
   BlockMgrAllocIntNode(h,&hdr->rootNodeOffset);*/
   /*hdr->rootNodeOffset = rootNode->nodeOffset;
   assert(hdr->rootNodeOffset.offset);
   if(!_BlockMgrFileWrite(&fileHandle,hdr,sizeof(DBlockMgrHeader),0)) {
      free(hdr);
      return -1;
   }
   BlockMgrIntNodeFree(rootNode);*/
   free(hdr);
   _BlockMgrFileClose(&fileHandle);
   return 0;
}

int MKVValidateHeader(BlockMgrHeader* hdr) {
   if (hdr->magic != BLOCK_MGR_MAGIC) {
      return -1;
   } 
   return 0;
}

uint8_t
BlockMgrInitClusterSizeCache(BlockMgrHandle* handle) {
   int i;
   int iter=0;

   /*handle->clusterSizeCache = malloc(sizeof(uint64_t)*NUM_BLOCK_SIZES*56);
   if (!handle->clusterSizeCache) {
      return False;
   }*/
   for (i=0;i<NUM_BLOCK_SIZES;i++) {
      BlockMgrPtrMap *map;
      BlockMgrDataClusterHdr cHdr;
      uint64_t o;

      map = &handle->dataMap[i].map;
      iter =0;
      while(!BlockMgrIsNullPtr(&map->ptrs[iter])) {
         if (!BlockMgrFileRead(handle,&cHdr,sizeof(cHdr),_O(&map->ptrs[iter]))) {
            return False;
         }
         o = _O(&map->ptrs[iter]);;
         o += (cHdr.blockSize * cHdr.totalResources);
         o += (cHdr.numBitArray * 512)+512;
         handle->clusterSizeCache[i][iter] = o;
         iter++;
         
      }
   }
   return True;
}

//XXX wouldnt it crash if the handle is not mem not allocated ?

void BlockMgrFreeCache(BlockMgrHandle* handle) {
   int i;
   if (!handle->cstrHeaderCache) {
      return;
   }
   for (i=0;i<NUM_BLOCK_SIZES;i++) {
      free(handle->cstrHeaderCache[i].hdr);
      free(handle->cstrHeaderCache[i].bArr);
   }
   free(handle->cstrHeaderCache);
}


static void BlockMgrFreeHandleInt(BlockMgrHandle* handle,uint8_t toRemove) {
   ListNode* node, *next;
   BlockMgrFreeCache(handle);
   free(handle->hdr);
   free(handle->dataMap);
   free(handle->intMap);
   if (handle->numMMap) {   
      LIST_FOR_ALL_SAFE(&handle->mmapLruList,node,next) {
         BlockMgrMMapDesc* desc = node->data;
         LIST_REMOVE(&handle->mmapLruList,node);
         _BlockMgrFileUnmap(handle->fileHandle,desc->addr,desc->map,desc->len);
         free(desc);
      }
   }
   if (handle->nodeCache) {
      HTDestroy(handle->nodeCache,free);
   }
   //free(handle->clusterSizeCache);

   if (handle->fileHandle) {
      if (!toRemove) {
         _BlockMgrFileClose(handle->fileHandle);
      } else {
         _BlockMgrFileRemove(handle->fileHandle);
      }
   }
   free(handle->fileHandle);
   PlatformRWLockFree(handle->rwLock);
   free(handle);
}

void BlockMgrFreeHandle(BlockMgrHandle* handle){
   return BlockMgrFreeHandleInt(handle,False);
}

void BlockMgrRemove(BlockMgrHandle* handle){
   return BlockMgrFreeHandleInt(handle,True);
}

/*
 * Initialize handle mode parameter unused.
 *
 * Initializes the handle for the file specified by name.
 * fails if file does not exists.
 * BlockMgrFormatFile should have been called before on the file.
 *
 */

uint32_t BlockMgrDiskPtrHashFn(const void* data,uint32_t size) {
   const BlockMgrDiskPtr * ptr = data;
   return ptr->offset/128;
}

uint8_t BlockMgrNodeCacheCmp(void* fullData,void* searchData) {
   BlockMgrMDataDescLRU* node = fullData;
   return node->desc.startPtr.offset == ((BlockMgrDiskPtr*)searchData)->offset;
}

BlockMgrError 
BlockMgrInitHandle(char* fileName,int mode,BlockMgrHandle ** outHandle) {

   //DBlockMgrHeader dbHeader;
   BlockMgrHandle* handle = malloc(sizeof(BlockMgrHandle));
   if (!handle) {
      return BLOCKMGR_NO_MEMORY;
   }
   *outHandle = NULL;
   memset(handle,0,sizeof(*handle));
   LIST_INIT(&handle->mmapLruList);
   handle->fileHandle = malloc(sizeof(BlockMgrFileHandle));
   if(!_BlockMgrFileOpenRW(handle->fileHandle,fileName)) {
      BlockMgrFreeHandle(handle);
      return BLOCKMGR_FILE_NOT_FOUND;
   }

   handle->hdr = malloc(sizeof(DBlockMgrHeader));
   if (!handle->hdr) {
      BlockMgrFreeHandle(handle);
      return BLOCKMGR_NO_MEMORY;
   }
   if(!_BlockMgrFileRead(handle->fileHandle,handle->hdr,sizeof(DBlockMgrHeader),0)) {
      BlockMgrFreeHandle(handle);
      return BLOCKMGR_IO_ERR;
   }
   if (MKVValidateHeader(handle->hdr)) {
      BlockMgrFreeHandle(handle);
      return BLOCKMGR_INVALID_MAGIC;
   }
   if ((handle->hdr->features & BLOCKMGR_FEATURE_CHECKSUM)){
      handle->checksumEnabled = True;
   }
   handle->dataMap = malloc(sizeof(DBlockMgrPtrMap)*NUM_BLOCK_SIZES);
   if (!handle->dataMap) {
      BlockMgrFreeHandle(handle);
      return BLOCKMGR_NO_MEMORY;
   }

   if(!_BlockMgrFileRead(handle->fileHandle,handle->dataMap,(sizeof(DBlockMgrPtrMap)*(NUM_BLOCK_SIZES)),4096)) {
      BlockMgrFreeHandle(handle);
      return BLOCKMGR_IO_ERR;
   }

   if (!BlockMgrInitClusterSizeCache(handle)) {
      BlockMgrFreeHandle(handle);
      return BMgrGetError(handle);
   }
   
   handle->intMap = malloc(sizeof(DBlockMgrIntClusterMap)*(NUM_BLOCK_SIZES-1));
   if (!handle->intMap) {
      BlockMgrFreeHandle(handle);
      return BLOCKMGR_NO_MEMORY;
   }
   //Reading a single IntClusterMap pointer, since only one is allocated.
   if(!_BlockMgrFileRead(handle->fileHandle,handle->intMap,sizeof(DBlockMgrIntClusterMap),512)) { //rew- why do we read only 512 bytes? I thought we wrote 1024 bytes or something like that ?
      BlockMgrFreeHandle(handle);
      return BLOCKMGR_IO_ERR;
   }


   if(USE_CACHE) {
      BlockMgrCreateCache(handle);
   }
   handle->dataWrite = 0;
   if (!BlockMgrJournalCheck(handle)) {
      BlockMgrFreeHandle(handle);
      return BMgrGetError(handle);
   }   
   handle->nodeCache = HTInit(BlockMgrDiskPtrHashFn,BlockMgrNodeCacheCmp,1023);
   if (!handle->nodeCache) {
      BlockMgrFreeHandle(handle);
      return BLOCKMGR_NO_MEMORY;
   }
   LIST_INIT(&handle->lruNodeList);
   handle->numNodesInCache =0;
   handle->nodeCacheSize = 1024;
   if((handle->rwLock = PlatformRWLockInit()) == NULL) {
      BlockMgrFreeHandle(handle);
      return BLOCKMGR_LOCK_FAILED;
   }
   *outHandle = handle;
   return BLOCKMGR_OK;
}

int BlockMgrCreateCache(BlockMgrHandle* handle) {
   int i;
   handle->cstrHeaderCache = malloc(sizeof(BlockMgrHdrCache) * NUM_BLOCK_SIZES);

   for(i =0;i< NUM_BLOCK_SIZES;i++) {
      handle->cstrHeaderCache[i].isValid = False;
      handle->cstrHeaderCache[i].bitMapValid = False;
      handle->cstrHeaderCache[i].handle = handle;
      handle->cstrHeaderCache[i].clusterHdrIndex = 0;
      handle->cstrHeaderCache[i].hdr = malloc(sizeof(BlockMgrDataClusterHdr));
      handle->cstrHeaderCache[i].bArr = malloc(sizeof(BlockMgrBitmapArr));  
   }
   return True;
}

static inline uint8_t BlockMgrBytesToId(BlockMgrHeader* hdr,uint64_t bytes,uint8_t *id) {
   /*if(bytes > hdr->maxBlockSize * DISK_BLOCK_SIZE)
      return False;*/
   uint64_t tmpBs= hdr->minBlockSize;
   int i=0;
   for(;i<NUM_BLOCK_SIZES;i++)
   {
      if((tmpBs<<i) >= bytes) {
         *id=i;
         return True;
      }
   }
   return False;
}

static inline uint8_t BlockMgrIdToBytes(BlockMgrHeader* hdr,uint64_t *bytes,uint8_t id) {
   if(id >= NUM_BLOCK_SIZES)
      return False;
   *bytes = hdr->minBlockSize <<id;
   return True;
}

#define ISBITSET(a,i) ((a) & (1<<i))
#define UNSETBIT(a,i) ((a) &= (~(1<<i)))
#define SETBIT(a,i)   ((a) |= (1<<i))

uint8_t BlockMgrCheckFreeBitnSet(BlockMgrDataClusterHdr *hdr,uint64_t* id) {
   uint32_t resNum = 0,found = False;
   int i;
   for (i=0;i<112;i++){
      int j=0;
      if (!hdr->bitmap[i]){
         resNum+=32;
         continue;
      }

      while(!ISBITSET(hdr->bitmap[i],j)){
         resNum++;
         j++;
         //break;
      }
      if (resNum <= hdr->totalResources) {
         UNSETBIT(hdr->bitmap[i],j);
         found =True;
         break;
      }
   }
  // printf("resNum=%d\n",resNum);
   *id = resNum;
   return True;
}

#define _MAP(handle,id) (&(handle)->dataMap[id].map)

#define BlockMgrCacheBMapArrOffset(cache,id)  (_O(&_MAP(cache->handle,id)->ptrs[cache->clusterHdrIndex])+sizeof(BlockMgrDataClusterHdr)+(cache)->bitArrIndex*sizeof(BlockMgrBitmapArr))

uint8_t BlockMgrGetBlockFromArray(BlockMgrHdrCache *cache, BlockMgrDiskPtr* outPtr) {
   uint32_t resNum = 0,found = False;
   uint8_t id;
   int i; 
   uint64_t bitArrOffset;
   BlockMgrBitmapArr *bArr;
   BlockMgrBytesToId(cache->handle->hdr,cache->hdr->blockSize,&id);

   bitArrOffset = BlockMgrCacheBMapArrOffset(cache,id);
   bArr = cache->bArr;
   //printf("Freehint %d\n",bArr->freeHint);
   for (i=bArr->freeHint;i<sizeof(bArr->bitmap)/sizeof(bArr->bitmap[0]);i++){
      int j=0;
      if (!bArr->bitmap[i]){
         //resNum+=32;
         continue;
      }
      resNum = i*32;
      bArr->freeHint = i;

      while(!ISBITSET(bArr->bitmap[i],j)){
         resNum++;
         j++;
         //break;
      }
      if (resNum <= bArr->totalResources) {
         UNSETBIT(bArr->bitmap[i],j);
         found =True;
         break;
      }
   }
   //printf("bitmapArrNum=%d\n",resNum);

   outPtr->blockSize = id;
   //outPtr->intNode = 0;
   _SO(outPtr,_O(&cache->hdr->dataStartOffset)+
              ((RES_IN_CLUSTER_HDR+resNum+cache->bitArrIndex*RES_PER_BMAP_ARR) * cache->hdr->blockSize));
    bArr->numFree--;

                  //_SO(&handle->cstrHeaderCache[id]->bitArrOffset,_O(&map->ptr[iter])+512+i*sizeof(bArr));
#if 0
   if(!_BlockMgrFileWrite(cache->handle->fileHandle,bArr,
                         sizeof(BlockMgrBitmapArr),bitArrOffset)) {
      return False;
   }  
#endif
   if (!BlockMgrFileWrite(cache->handle,bArr,sizeof(BlockMgrBitmapArr),bitArrOffset)){
      return False;
   }      
   return True;
}

uint8_t BlockMgrGetFreeBlock(BlockMgrHandle* handle,
                            BlockMgrDiskPtr *ClusterStPtr,
                            BlockMgrDataClusterHdr* hdr,
                            BlockMgrDiskPtr *diskPtr) {
   uint64_t nbit;
   uint8_t id;
//   double t1=UTime(),t2;
   //Check the block which is free and allocate.
   if(!BlockMgrCheckFreeBitnSet(hdr,&nbit)) {
      //Bitmap and cluster header are not in correlation.
      assert(0);
      return False;
   }

   BlockMgrBytesToId(handle->hdr,hdr->blockSize,&id);
   diskPtr->blockSize = id;
   //diskPtr->intNode = 0;
   //id will be offset starting from cluster data start. Herre is it id*256 ? no ?
   _SO(diskPtr,_O(&hdr->dataStartOffset)+nbit * (hdr->blockSize)); 

   //Update the header field
   hdr->numFree--;
   //Write to disk
#if 0
   if(!_BlockMgrFileWrite(handle->fileHandle,hdr,sizeof(BlockMgrDataClusterHdr),_O(ClusterStPtr))) {
      return False;
   }
#endif
   if(!BlockMgrFileWrite(handle,hdr,sizeof(BlockMgrDataClusterHdr),_O(ClusterStPtr))) {
      return False;
   }
  // t2=UTime();
  // handle->getFreeBlock += (t2-t1);
   return True;
}

static inline uint8_t BlockMgrResInCluster(BlockMgrHandle* handle,
                                          BlockMgrDataClusterHdr* hdr,
                                          BlockMgrDiskPtr ptrToHdr,
                                          BlockMgrDiskPtr diskPtr) {
   uint64_t startOffset = _O(&ptrToHdr);
   uint64_t resSize;
   uint64_t endOffset;
   BlockMgrIdToBytes(handle->hdr,&resSize,diskPtr.blockSize);
   endOffset = startOffset + hdr->totalResources*resSize+sizeof(*hdr);
   return ( (startOffset < _O(&diskPtr)) && (endOffset > _O(&diskPtr)));
}

static inline void 
BlockMgrFreeFromBMap(uint32_t* bmap,int resNum) {
   int i = resNum/32;
   int bitNum = resNum %32;
   SETBIT(bmap[i],bitNum);
}

uint8_t 
BlockMgrFreeResFromCluster(BlockMgrHdrCache* cache,
                        BlockMgrDataClusterHdr* hdr,
                        uint32_t clusterHdrIndex,
                        BlockMgrDiskPtr diskPtr) {

   uint64_t o = _O(&diskPtr);
   uint32_t resNum;
   uint32_t bmapNum;
   BlockMgrBitmapArr bArr;
   BlockMgrDiskPtr hdrOffset;
   BlockMgrHandle* handle = cache->handle;
   uint8_t id = diskPtr.blockSize;
   BlockMgrPtrMap *map;
   
   map = &handle->dataMap[id].map;
   hdrOffset = map->ptrs[clusterHdrIndex];
   o-=_O(&hdr->dataStartOffset); 
   resNum = o/hdr->blockSize;
   bmapNum = (resNum - RES_IN_CLUSTER_HDR) / RES_PER_BMAP_ARR;

   if (clusterHdrIndex < cache->clusterHdrIndex) {
      cache->clusterHdrIndex = clusterHdrIndex;
      cache->isValid = True;
      cache->bitMapValid = False;
      /*if(!_BlockMgrFileRead(handle->fileHandle,cache->hdr,
                           sizeof(BlockMgrDataClusterHdr),
                           _O(&hdrOffset))) {

         return False;
      }*/
      memcpy(cache->hdr,hdr,sizeof(*hdr));
      // do all modifications in cache copy
      hdr = cache->hdr;
   }
   
   if (resNum < RES_IN_CLUSTER_HDR ) {
      // Res in header bitmap
      
      hdr->numFree++;
      if ((resNum / 32 ) < hdr->freeHint) {
         hdr->freeHint = resNum/32;
      }
      BlockMgrFreeFromBMap(hdr->bitmap,resNum);
      o = _O(&hdrOffset);
      if (!BlockMgrFileWrite(handle,hdr,sizeof(*hdr),o)){
         return False;
      }
      return True;
   }
   //printf("Bitmap Array \n");

   resNum -= RES_IN_CLUSTER_HDR;
   resNum %= RES_PER_BMAP_ARR;
   o = _O(&hdrOffset) +( bmapNum+1) * 512; 
   if (!BlockMgrFileRead(handle,&bArr,sizeof(bArr),o)){
      return False;
   }

   bArr.numFree++;
   bArr.freeHint = bArr.freeHint > (resNum /32) ? resNum/32 : bArr.freeHint;
   BlockMgrFreeFromBMap(bArr.bitmap,resNum);

   if (!BlockMgrFileWrite(handle,&bArr,sizeof(bArr),o)){
      return False;
   }
   
   if ((!cache->bitMapValid) || (cache->bitArrIndex >= bmapNum) ) {
      cache->bitMapValid = True;
      cache->bitArrIndex = bmapNum;
      memcpy(cache->bArr,&bArr,sizeof(bArr));
   }
   return True;
}

uint8_t 
BlockMgrFreeBlock(BlockMgrHandle* handle,
                 BlockMgrDiskPtr diskPtr) {
   // find out which cluster the offset falls into and mark the bit free
   uint8_t id = diskPtr.blockSize;
   BlockMgrPtrMap *map = &handle->dataMap[diskPtr.blockSize].map;
   int iter=0;
   BlockMgrHdrCache * cache;
   int i;

   /*for (i=0;i<NUM_BLOCK_SIZES;i++) {
      iter =0;
      while (handle->clusterSizeCache[i][iter]) {
         printf("Cluster size %d %d %ld \n",i,iter,handle->clusterSizeCache[i][iter]);
         iter++;
      }
   }*/
   iter=0;
   cache = &handle->cstrHeaderCache[id];
   while(!BlockMgrIsNullPtr(&map->ptrs[iter])) {
      uint64_t o;
      BlockMgrDataClusterHdr hdr;
      BlockMgrDataClusterHdr* hPtr = &hdr;
      
      o = _O(&map->ptrs[iter]);
      if (_O(&diskPtr) > o && handle->clusterSizeCache[id][iter] > _O(&diskPtr)) {
         if (iter == cache->clusterHdrIndex) {
            hPtr = cache->hdr;
         } else {
            if(!BlockMgrFileRead(handle,&hdr,
                                 sizeof(BlockMgrDataClusterHdr),
                                 _O(&map->ptrs[iter]))) {

               return False;
            }
         }
         if (!BlockMgrFreeResFromCluster(cache,hPtr,iter,diskPtr)) {
            return False;
         }
         return True;
      }
      /*
      if(!_BlockMgrFileRead(handle->fileHandle,&hdr,
                           sizeof(BlockMgrDataClusterHdr),
                           _O(&map->ptrs[iter]))) {

         return False;
      }
      if (BlockMgrResInCluster(handle,&hdr,map->ptrs[iter],diskPtr)) {
         // Found the cluster free the resource
         // return True;;
         //cache->isValid = False;
         assert(hdr.blockSize);
         if (!BlockMgrFreeResFromCluster(cache,&hdr,iter,diskPtr)) {
            assert(0);
         }
         return True;
      }*/
      iter++;
   }
   return True;
}

uint8_t BlockMgrDropCaches(BlockMgrHandle* handle) {
   int i;
   if(!BlockMgrFileRead(handle,handle->dataMap,(sizeof(DBlockMgrPtrMap)*(NUM_BLOCK_SIZES)),4096)) {
      SetBMgrError(handle,BLOCKMGR_IO_ERR);
      return False;
   }
   for(i =0;i< NUM_BLOCK_SIZES;i++) {
      handle->cstrHeaderCache[i].isValid = False;
   }
   return True;
}

uint8_t BlockMgrAllocateDataBytes(BlockMgrHandle* handle,
                                 uint32_t bytes,
                                 BlockMgrDiskPtr *diskPtr) //what type bytes should be ?
{  
   uint8_t id;
   uint64_t bArrOffset;
   BlockMgrPtrMap *map;
   BlockMgrHdrCache *cache;
   uint64_t iter;
   BlockMgrDataClusterHdr hdr;
   //return error incase.
   handle->readLink += 1;
   if(!BlockMgrBytesToId(handle->hdr,bytes,&id)) {
      SetBMgrError(handle,BLOCKMGR_BLOCK_TOO_BIG);
      assert(0);
      return False;
   }
   map = &handle->dataMap[id].map;
   cache = &handle->cstrHeaderCache[id];
   iter = 0;
   //BlockMgrDiskPtr clusterOffset;
   if (cache->isValid) {
      assert(cache->hdr->blockSize);
      //printf("Cache is valid %d\n",cache->isValid);
      iter = cache->clusterHdrIndex;
      if (cache->hdr->numFree) {
         return BlockMgrGetFreeBlock(handle,&map->ptrs[cache->clusterHdrIndex],
                                    cache->hdr,diskPtr);
      }
      if (cache->hdr->numBitArray) {
         if (!cache->bitMapValid) {
            cache->bitArrIndex =0; 
            cache->bitMapValid = True;
            bArrOffset = BlockMgrCacheBMapArrOffset(cache,id); 
            if(!BlockMgrFileRead(handle,cache->bArr,
                                 sizeof(BlockMgrBitmapArr),bArrOffset)) {
            /*if (((int)rv) <0 || rv < sizeof(BlockMgrBitmapArr)) {
               SetBMgrError(handle,MORPH_DB_IO_ERR;
               handle->posixError = errno;
               return False;
            } */
               return False;
            }
         }
         while (True ) {
            if (cache->bArr->numFree) {
               return BlockMgrGetBlockFromArray(cache,diskPtr);
            }
            cache->bitArrIndex++;
            if (cache->bitArrIndex >= cache->hdr->numBitArray) {
               //All bit map has run out
               break;
            }
            bArrOffset = BlockMgrCacheBMapArrOffset(cache,id); 
            if(!BlockMgrFileRead(handle,cache->bArr,sizeof(BlockMgrBitmapArr),bArrOffset)) {
            /*if (((int)rv) <0 || rv < sizeof(BlockMgrBitmapArr)) {
               SetBMgrError(handle,MORPH_DB_IO_ERR;
               handle->posixError = errno;
               return False;
            } */
               return False;
            }
         }
      }
      iter++; 
   }
   // go to next cluster to find resource
//   if(!USE_CACHE || !handle->cstrHeaderCache[id]->isValid) {
   //printf("Cache miss\n" );

   while(!BlockMgrIsNullPtr(&map->ptrs[iter])) {
      BlockMgrDataClusterHdr hdr;
      if(!BlockMgrFileRead(handle,&hdr,sizeof(BlockMgrDataClusterHdr),_O(&map->ptrs[iter]))) {
      /*if (((int)rv) <0 || rv < sizeof(hdr)) {
         SetBMgrError(handle,MORPH_DB_IO_ERR;
         handle->posixError = errno;
         return False;
      }*/
         return False;
      }
      memcpy(cache->hdr,&hdr,sizeof(hdr));
      cache->clusterHdrIndex = iter;
      cache->bitMapValid = False;
      cache->isValid = True;
      if(hdr.numFree > 0) {  //rew-for 1024 block size, numfree = 127 for the first time? 
         return BlockMgrGetFreeBlock(handle,&map->ptrs[iter],cache->hdr,diskPtr);
      } else if(hdr.numBitArray > 0) {  // This code path is only for the clusters if no of blocks exceed bitmap = 112 * 32
         int i;
         uint64_t bArrResources = 0; 
         for (i=0;i<hdr.numBitArray;i++) {
            //BlockMgrBitmapArr bArr;
            //rew- changed from bArr
            if(!BlockMgrFileRead(handle,cache->bArr,sizeof(BlockMgrBitmapArr),
                                 _O(&map->ptrs[iter])+512+i*sizeof(BlockMgrBitmapArr))) {
            /*if (((int)rv) <0 || rv < sizeof(hdr)) {
               SetBMgrError(handle,MORPH_DB_IO_ERR;
               handle->posixError = errno;
               return False;
            }*/
               return False;
            }
            cache->bitMapValid = True;
            //memcpy(cache->bArr,&bArr,sizeof(bArr));
            cache->bitArrIndex = i;
            if(cache->bArr->numFree > 0) {
               return BlockMgrGetBlockFromArray(cache,diskPtr);

                  //_SO(&handle->cstrHeaderCache[id]->bitArrOffset,_O(&map->ptr[iter])+512+i*sizeof(bArr));
               }  
            }
         }
       iter++;
   }
  //All existing clusters of this block size are full.
      if(!BlockMgrAllocDataCluster(handle,(handle->hdr->minBlockSize<<id))) { //Better to declare another function to take id itself ?	
         return False;
      }

      if(!BlockMgrFileRead(handle,&hdr,sizeof(BlockMgrDataClusterHdr),_O(&map->ptrs[iter]))) {
      /*
      if (((int)rv) <0 || rv < sizeof(hdr)) {
         SetBMgrError(handle,MORPH_DB_IO_ERR;
         handle->posixError = errno;
         return False;
      }*/
         return False;
      }
      cache->clusterHdrIndex = iter;
      memcpy(cache->hdr,&hdr,sizeof(hdr));
      cache->bitMapValid = False;
      cache->isValid = True;
      // Now iter points to newly allocated block.
      if(!BlockMgrGetFreeBlock(handle,&map->ptrs[iter],cache->hdr,diskPtr)) {
         SetBMgrError(handle,BLOCKMGR_INT_ERROR);
         assert(0);
         return False;
      }
   return True;
}  

uint8_t BlockMgrInsertToLinksCache(BlockMgrHandle* handle,BlockMgrMDataDesc* in) {
   void* exist,*rm;
   BlockMgrMDataDescLRU * lruNode = malloc(sizeof(*lruNode));
   lruNode->desc = *in;
   lruNode->lruNode.data = lruNode;
   //printf("Allocating %p\n",lruNode);
   while (handle->numNodesInCache && handle->numNodesInCache >= handle->nodeCacheSize) {
      ListNode* node = LIST_FIRST(&handle->lruNodeList);
      BlockMgrMDataDescLRU* rmNode = node->data;
      LIST_REMOVE(&handle->lruNodeList,&rmNode->lruNode);
      rm = HTRemove(handle->nodeCache,&rmNode->desc.startPtr,sizeof(rmNode->desc.startPtr));
      //printf("Freeing %p %p\n",rmNode,node);
      free(rmNode);
      handle->numNodesInCache--;
   }
   handle->numNodesInCache++;
   HTInsert(handle->nodeCache,lruNode,&lruNode->desc.startPtr,sizeof(lruNode->desc.startPtr),&exist);
   LIST_ADD_AFTER(&lruNode->lruNode,LIST_LAST(&handle->lruNodeList));
   return True;   
}

static uint8_t
BlockMgrReadLinks(BlockMgrHandle* handle,
                 BlockMgrDiskPtr startPtr,
                 BlockMgrMDataDesc* out) {
   BlockMgrDataDesc *outDesc = out->desc;
   BlockMgrDiskPtr nextPtr = startPtr;
   uint64_t bytes;
   uint32_t curSize = 0;
   int i=0;
   double t1,t2;

   //t1=UTime();
   /*BlockMgrMDataDescLRU* node =  HTFind(handle->nodeCache,&startPtr,sizeof(startPtr));
   if (node) {
      LIST_REMOVE(&handle->lruNodeList,&node->lruNode);
      LIST_ADD_AFTER(&node->lruNode,LIST_LAST(&handle->lruNodeList));
      *out = node->desc;
      //printf("Cache hit\n");
      return True;
   }*/
   while (!BlockMgrIsNullPtr(&nextPtr)) {
      if(!BlockMgrFileRead(handle,&outDesc[i],sizeof(outDesc[i]),_O(&nextPtr))) {
         return False;
      }
      BlockMgrIdToBytes(handle->hdr,&bytes,nextPtr.blockSize);
      curSize += (bytes - sizeof(outDesc[i]));
      nextPtr = outDesc[i].next; 
      i++;
   }
   out->startPtr = startPtr;
   out->totalSize = curSize;
   //BlockMgrInsertToLinksCache(handle,out);
   //t2 = UTime();
   //handle->readLink +=1;
   return True;
}

uint32_t BinaryStrCheckSum(BlockMgrHandle* handle,BTreeValue *val) {
   if (handle->checksumEnabled) {
      return CRC32((uint8_t*)val->data,val->len);
   }
   return 0;
}

uint8_t
BlockMgrDataFreeWithLock(BlockMgrHandle* handle, 
                 BlockMgrDiskPtr startPtr) {
   BlockMgrMDataDesc oldDesc;
   BlockMgrDiskPtr nextPtr;
   int i=0;

   if (!BlockMgrReadLinks(handle,startPtr,&oldDesc)) {
      return False;
   }
   nextPtr = oldDesc.startPtr; 
   while (!BlockMgrIsNullPtr(&nextPtr)) {
      BlockMgrFreeBlock(handle,nextPtr);
      nextPtr = oldDesc.desc[i].next;
      i++;
   }
   return True;
}

uint8_t
BlockMgrDataFree(BlockMgrHandle* handle, 
                 BlockMgrDiskPtr startPtr) {
   int ret = False;
   PlatformRWGetWLock(handle->rwLock);
   ret = BlockMgrDataFreeWithLock(handle,startPtr);
   PlatformRWUnlock(handle->rwLock);
   return ret;
}

static uint8_t
BlockMgrDataFreeFromDesc(BlockMgrHandle* handle,BlockMgrMDataDesc* desc) {
   BlockMgrDiskPtr nextPtr;
   int i=0;

   nextPtr = desc->startPtr; 
   while (!BlockMgrIsNullPtr(&nextPtr)) {
      BlockMgrFreeBlock(handle,nextPtr);
      nextPtr = desc->desc[i].next;
      i++;
   }
   return True;
}

static uint8_t
BlockMgrDataWriteFromDesc(BlockMgrHandle* handle,
                         BlockMgrMDataDesc* desc,
                         BTreeValue * val) {
   BlockMgrDataDesc *dDesc = desc->desc;
   BlockMgrDiskPtr nextPtr = desc->startPtr;
   int i=0;
   uint32_t curSegLen=0;
   uint32_t curOff=0;
   uint64_t bytes;

   dDesc[0].checksum = BinaryStrCheckSum(handle,val);
   dDesc[0].dataLen = val->len;
   assert(val->len);
   while (curOff < val->len) {
      assert(i < sizeof(desc->desc)/sizeof(dDesc[0]));
      BlockMgrIdToBytes(handle->hdr,&bytes,nextPtr.blockSize);

      curSegLen = (curOff + bytes-sizeof(*dDesc)) > val->len ? 
                        (val->len-curOff) :
                        bytes-sizeof(*dDesc);
      if (i) {
         dDesc[i].checksum = dDesc[0].checksum;
         dDesc[i].dataLen = curSegLen;
      }
      //printf("Writing to %lu %lu %u %lu nextptr %lu \n",
      //      _O(&nextPtr),_O(&nextPtr)+sizeof(dDesc[i]),curSegLen,val->len,
      //      _O(&dDesc[i].next));
      if (!BlockMgrFileWrite(handle,
                             &dDesc[i],
                             sizeof(dDesc[i]),
                             _O(&nextPtr))) {
         return False;
      }
      if (!BlockMgrFileWrite(handle,
                             val->data+curOff,
                             curSegLen,
                             _O(&nextPtr)+sizeof(*dDesc))) {
         return False;
      }

      curOff += curSegLen;
      nextPtr = dDesc[i].next;
      i++;
   }

   return True;
}

uint8_t
BlockMgrDataRewrite(BlockMgrHandle* handle,
                  BlockMgrDiskPtr startPtr,
                  BTreeValue* val,
                  BlockMgrDiskPtr *out) {
   int i=0;
   BlockMgrDiskPtr nextPtr = startPtr;
   uint32_t curSize=0;
   int j=0;
   uint64_t bytes;
   uint32_t newSize = val->len;
   BlockMgrMDataDesc oldDesc;
   BlockMgrDataDesc *outDesc = oldDesc.desc;
   BlockMgrMDataDesc temp;
   
   /*if (_O(&startPtr)) {
      BlockMgrIdToBytes(handle->hdr,&bytes,startPtr.blockSize);
      *out =startPtr;
      if (bytes >= (val->len +sizeof(BlockMgrDataDesc))){
         return BlockMgrFileWrite(handle,val->data,val->len,_O(&startPtr)+sizeof(BlockMgrDataDesc));
      }

   }*/
   if (val->len > (MAX_LINKS*(handle->hdr->maxBlockSize+sizeof(BlockMgrDataDesc)))) {
      SetBMgrError(handle,BLOCKMGR_DATA_TOO_BIG);
      return False;
   }
   if (!BlockMgrReadLinks(handle,startPtr,&oldDesc)) {
            assert(0);
      return False;
   }
   curSize = oldDesc.totalSize;
   memset(&temp,0,sizeof(temp));

   /*if (curSize >= newSize) {
      // nothing to do already enough space is there.
      if (!BlockMgrDataWriteFromDesc(handle,&oldDesc,val)){
         assert(0);
         return False;
      }
      *out = startPtr;
      return True;
   }*/
   nextPtr = oldDesc.startPtr; 
   while (!BlockMgrIsNullPtr(&nextPtr)) {
      nextPtr = outDesc[i].next;
      i++;
   }
   //printf("New size cursize %u %u\n",newSize,curSize);
   if (newSize > curSize ) {
      uint32_t remSize = newSize - curSize;   
      uint32_t linksReq = (remSize / (handle->hdr->maxBlockSize -16));
      if (remSize % (handle->hdr->maxBlockSize-16)) {
         linksReq++;
      }
      // expand
      if (i) {
         *out = startPtr;
      }
      if ((i+linksReq) <= MAX_LINKS) {
         while (remSize) {
            // enough room for more links 
            BlockMgrDiskPtr ptr;
            curSize = remSize > (handle->hdr->maxBlockSize-sizeof(outDesc[0])) ? (handle->hdr->maxBlockSize-sizeof(outDesc[0])):remSize;
            if (!BlockMgrAllocateDataBytes(handle,
                                          curSize+sizeof(outDesc[i-1]),
                                          &ptr)) {

               //printf("Allocation failed.\n");
               //assert(0);
               return False;
            }

            if (!i) {
               oldDesc.startPtr = ptr;
               *out = ptr;
            } else {
               outDesc[i-1].next = ptr;
            }
            memset(&outDesc[i],0,sizeof(outDesc[i]));

            remSize -= curSize;
            i++;
         }
         if (!BlockMgrDataWriteFromDesc(handle,&oldDesc,val)){
            // leak block if the write fails
            // if we free here we dont know the status
            // of the allocated block whether it is
            // referenced or not referenced
            assert(0);
            return False;
         }
         return True;
      }
      // not enough links start over
      // try allocating new data before freeing
      // Handle data greater than max blocksize
      remSize = newSize;
      curSize = 0;
      int k = 0;
      BlockMgrDataDesc newDesc;
      while (remSize) {
         BlockMgrDiskPtr ptr;
         _SO(&ptr,0);
         curSize = remSize > (handle->hdr->maxBlockSize-sizeof(outDesc[0])) ? (handle->hdr->maxBlockSize-sizeof(outDesc[0])):remSize;
         if (!BlockMgrAllocateDataBytes(handle,
                                      curSize+sizeof(outDesc[i-1]),
                                      &ptr)) {
            assert(0);
            return False;
         }
         if (!k) {
            temp.startPtr = ptr;
            *out = ptr;
         } else {
            temp.desc[k-1].next = ptr;
         }
         memset(&temp.desc[k],0,sizeof(outDesc[k]));

         remSize -= curSize;
         k++;
      }
      assert(k<= MAX_LINKS);
      if (!BlockMgrDataWriteFromDesc(handle,&temp,val)){
         BlockMgrDataFreeFromDesc(handle,&temp);
         assert(0);
         return False;
      }

      *out = temp.startPtr;
      // dont check errors let it leak but no data loss
      //
      BlockMgrDataFreeFromDesc(handle,&oldDesc);

      memset(&outDesc[0],0,sizeof(outDesc[i]));
      return True;
   }
   // Data shrink
   //printf("Setting last desc next %d %lu \n",j,_O(&temp.desc[j-1].next));
   curSize = 0;
   j = 0;
   nextPtr = startPtr;
   *out = startPtr;
   temp.startPtr= startPtr;
   while (!BlockMgrIsNullPtr(&nextPtr)) {
      BlockMgrIdToBytes(handle->hdr,&bytes,nextPtr.blockSize);
      nextPtr = outDesc[j].next;
      temp.desc[j].next = nextPtr; 
      j++;
      curSize += (bytes-sizeof(outDesc[j]));
      if (curSize >= newSize) {
         break;
      }
   }
   _SO(&temp.desc[j-1].next,0);
   //printf("Setting last desc next %d %lu \n",j,_O(&temp.desc[j-1].next));
   // Free the extra blocks.
   // We could delay it too incase the object grows back.
   if (!BlockMgrDataWriteFromDesc(handle,&temp,val)){
      // we will leak the blocks if the above partial write
      // passes or the next step of freeing block fails.
            assert(0);
      return False;
   }
   while (i>j) {
      if (!BlockMgrIsNullPtr(&outDesc[i-1].next)) {
         if (!BlockMgrFreeBlock(handle,outDesc[i].next)){
           // There is enough space already freeing is not required
           // return False;
         }
         memset(&outDesc[i-1].next,0,sizeof(outDesc[i-1].next));
      }
      i--;
   }

   return True;
}

uint8_t 
BlockMgrDataWriteWithLock(BlockMgrHandle* handle,
                 BTreeValue* val,
                 BlockMgrDiskPtr *startPtr, // IN/OUT
                 uint8_t *newPtr) {
   //size_t rv;
   BlockMgrDataDesc * d;
   double t1,t2;
   uint32_t totalSize;
   uint64_t bytes;
   BlockMgrDiskPtr newDataPtr;
   
   *newPtr = False; 
   //t1 = UTime();
   if (!BlockMgrDataRewrite(handle,*startPtr,val,&newDataPtr)) {
      return False;
   }
   if (_O(startPtr) != _O(&newDataPtr)) {
      *startPtr = newDataPtr;
      *newPtr = True;
   }
   //t2= UTime();
   handle->numDataWrite++;
   //handle->dataWrite += (t2-t1);
   return True;
#if 0
   d->next.offset = 0;
   d->dataLen = val->len;
   d->checksum = 0;
   memcpy(d+1,val->data,val->len);
   //printf("Writing data @ %lu len %u \n",ptr->offset,val->len);
   /*rv = PWrite(tree->handle->fd,&desc,sizeof(desc),_O(ptr));
   if ((int)rv < 0||rv <val->len) {
      return False;
   }*/
   t1=UTime(),t2;
   if(!_BlockMgrFileWrite(handle->fileHandle,d,val->len+sizeof(*d),_O(ptr))) {
      free(d);
      return False;
   }
   t2=UTime();
   //printf("%lf\n",t2-t1);
   //tree->dataWriteTime += (t2-t1);
   free(d);
   return True;
#endif
}


uint8_t 
BlockMgrDataWrite(BlockMgrHandle* handle,
                 BTreeValue* val,
                 BlockMgrDiskPtr *startPtr, // IN/OUT
                 uint8_t *newPtr) {
   uint8_t ret = False;
   PlatformRWGetWLock(handle->rwLock);
   ret = BlockMgrDataWriteWithLock(handle,val,startPtr,newPtr);
   PlatformRWUnlock(handle->rwLock);
   return ret;
}

uint8_t
BlockMgrReadUserData(BlockMgrHandle* handle,BTreeValue* val){
   uint8_t ret;
   PlatformRWGetRLock(handle->rwLock);
   ret = BlockMgrDataReadWithLock(handle,handle->hdr->userDataOffset,val);
   PlatformRWUnlock(handle->rwLock);
   return ret;
}

uint8_t BlockMgrDataSync(BlockMgrHandle* handle) {
   uint8_t ret = False;
   PlatformRWGetWLock(handle->rwLock);
   ret = _BlockMgrFileSync(handle->fileHandle);
   PlatformRWUnlock(handle->rwLock);
   return ret;
}

uint8_t BlockMgrDataReadWithLock(BlockMgrHandle* handle,BlockMgrDiskPtr ptr,BTreeValue *val) {
   BlockMgrDiskPtr nextPtr = ptr;
   char* tmpData=NULL;
   uint64_t bytes;
   BlockMgrDataDesc *desc;
   uint32_t curOff =0;
   uint32_t curSegLen =0;
   uint32_t oldCheckSum =0;
   uint32_t newCheckSum;

   val->data = NULL;
   while (!BlockMgrIsNullPtr(&nextPtr)){
      //printf("Next pointer %lu\n",_O(&nextPtr));
      BlockMgrIdToBytes(handle->hdr,&bytes,nextPtr.blockSize);
      tmpData = malloc(bytes);
      if (!tmpData) {
         free(val->data);
         return False;
      }
      desc = (BlockMgrDataDesc*)tmpData;
      if (!BlockMgrFileRead(handle,tmpData,bytes,_O(&nextPtr))){
         free(tmpData);
         free(val->data);
         return False;
      }
      if (!val->data) {
         val->data = malloc(desc->dataLen);
         
         if (!val->data) {
            SetBMgrError(handle,BLOCKMGR_NO_MEMORY);
            free(tmpData);
            return False;
         }
         oldCheckSum = desc->checksum;
         val->len = desc->dataLen;
      } else {
         if (oldCheckSum != desc->checksum) {
            SetBMgrError(handle,BLOCKMGR_CHECKSUM_ERR);
            val->data = NULL;
            val->len =0;
            free(val->data);
            free(tmpData);
            return False;
         }
      }

      curSegLen = desc->dataLen > bytes-sizeof(*desc) ?
                      bytes-sizeof(*desc):desc->dataLen;
      memcpy(val->data+curOff,tmpData+sizeof(*desc),curSegLen);
      curOff += curSegLen;

     // assert(curOff <= val->len);
      nextPtr = desc->next;
      free(tmpData);
      if (curOff > val->len) {
         SetBMgrError(handle,BLOCKMGR_CORRUPT_DATA);
         BinaryStrFree(val);
         return False;
      }
   }
   if (BinaryStrCheckSum(handle,val) != oldCheckSum) {
      SetBMgrError(handle,BLOCKMGR_CHECKSUM_ERR);
      free(val->data);
      val->data = NULL;
      val->len =0;
      return False;
   }
   return True;
}

uint8_t BlockMgrDataRead(BlockMgrHandle* handle,BlockMgrDiskPtr ptr,BTreeValue *val) {
   uint8_t ret = False;
   PlatformRWGetRLock(handle->rwLock);
   ret = BlockMgrDataReadWithLock(handle,ptr,val);
   PlatformRWUnlock(handle->rwLock);
   return ret;
}
__attribute__((constructor)) void BlockMgrModuleInit(){
   InitCRCTable();
}

#if 0
uint8_t MorphDBInsertNodeToCache(MorphDBDiskTree* tree, MorphDBIntNode* node){
   void *exist,*rm;
   if (!tree->nodeCache) {
      return True;
   }
   nodeCount++;
   
   //printf("Inserting node %p %lu %p %u\n",node,node->nodeOffset.offset,&node->lruNode,nodeCount);
   node->lruNode.data = node;
   assert(node->lruNode.data);
   ListNode* lNode = LIST_FIRST(&tree->lruNodeList);
   while (tree->numNodesInCache && tree->numNodesInCache >= tree->nodeCacheSize){
      //ListNode* node = LIST_FIRST(&tree->lruNodeList);
      
      MorphDBIntNode* rmNode = lNode->data;
      assert(rmNode);

      if (rmNode == tree->root) {
         lNode = lNode->next;
         if (lNode == LIST_FIRST(&tree->lruNodeList)) {
            break;
         }
         continue;
      } 
      //printf("cache limit reached freeing nodes %p %lu\n",rmNode,_O(&rmNode->nodeOffset));
      LIST_REMOVE(&tree->lruNode, &rmNode->lruNode);
      rm = HTRemove(tree->nodeCache,&rmNode->nodeOffset,sizeof(node->nodeOffset));
      
      assert(!HTFind(tree->nodeCache,&rmNode->nodeOffset,sizeof(node->nodeOffset)));
     // printf("%p\n",rm);
#if DEBUG
      printf("Removing nodes from cache %p %lu \n",rmNode,rmNode->nodeOffset.offset);
#endif
      MorphDBIntNodeFree(rmNode);
      tree->numNodesInCache--;
   }
   tree->numNodesInCache++;
   HTInsert(tree->nodeCache,node,&node->nodeOffset,sizeof(node->nodeOffset),&exist);
   LIST_ADD_AFTER(&node->lruNode,LIST_LAST(&tree->lruNodeList));
   return True;
}
#endif
