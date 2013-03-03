#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <assert.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "morphdb.h"
#include "blockmgr.h"
#include "list.h"
#include "bson.h"
#include "stringutil.h"
#include "btreekeyops.h"
#include "morphdbbtree.h"
#include <pthread.h>


#define DEBUG 0
#define THIS_LOW_VERSION 0
#define THIS_HIGH_VERSION 0
static __thread MorphDBError tMorphDBBTreeError;

void Free(void* ptr) {
   printf("Freeing %p\n",ptr);
   free(ptr);
}

void* Realloc(void* old, size_t len){
   void* ptr = realloc(old,len);
   printf("Old %p New %p \n",old,ptr);
   return ptr;
}

//#define free Free
//#define realloc Realloc

MorphDBError MorphDBErrorFromBMgrError(BlockMgrError error);

void MorphDBTreeSetError(MorphDBError err) {
   tMorphDBBTreeError = err;
}
#define SetDBTreeError(tree,err)\
   tMorphDBBTreeError = err;

#define BErr(tree)\
   BlockMgrGetLastError()

#define BErrToMErr(tree)\
   SetDBTreeError(tree,MorphDBErrorFromBMgrError(BErr(tree)));

#define NKEYS2(node) ((node)->desc->numKeys)
//#define KEY_PTR_LEN(tOrder) (CEIL(tOrder,4))
//#define PTR_PTR_LEN(tOrder) (CEIL(tOrder+1,4))
#define KEY_PTR_LEN(tOrder) (tOrder)
#define PTR_PTR_LEN(tOrder) (tOrder+1)
#define PTR_PTR_OFF(pos,tOrder) ((pos)/PTR_PTR_LEN(tOrder))
#define PTR_PTR(node,pos,tOrder) ((node)->desc->ptrPtrs[PTR_PTR_OFF(pos,tOrder)])

#define PTR_OFFSET(node,pos,tOrder) (PTR_PTR(node,pos,tOrder) + sizeof(BlockMgrDiskPtr) * ((pos)%PTR_PTR_LEN(tOrder)))


#define PTR_SIZE(tOrder) (PTR_PTR_LEN(tOrder)*sizeof(BlockMgrDiskPtr))

//#define PTR_DATA(node,pos,tOrder) ((BlockMgrDiskPtr*)((node)->buf + PTR_OFFSET(node,pos,tOrder)))

#define PTR_DATA(node,pos,tOrder) (((BlockMgrDiskPtr*)(node->ptrTable))+(pos))
#define KEY_PTR_OFF(pos,tOrder) ((pos)/KEY_PTR_LEN(tOrder))
#define KEY_PTR(node,pos,tOrder) ((node)->desc->keyPtrs[KEY_PTR_OFF(pos,tOrder)])

//#define KEY_OFFSET(node,pos,tOrder) (((uint32_t*)(node->buf+KEY_PTR(node,pos,tOrder)))[((pos)%KEY_PTR_LEN(tOrder))*2])

#define KEY_DATA_OFFSET(node,pos,tOrder)\
   ((uint32_t *)((node)->buf+(KEY_PTR(node,pos,tOrder))+(KEY_PTR_LEN(tOrder)*2*sizeof(uint32_t))))

//#define KEY_DATA(node,pos,tOrder) ((node->buf)+KEY_OFFSET(node,pos,tOrder))
#define KEY_DATA(node,pos,tOrder) ((node)->buf+(node)->keyTable[(pos)*2])

//#define KEY_LEN(node,pos,tOrder) (((uint32_t*)(node->buf+KEY_PTR(node,pos,tOrder)))[((pos)%KEY_PTR_LEN(tOrder))*2+1])

#define KEY_LEN(node,pos,tOrder) ((node)->keyTable[(pos)*2+1])
#define KEY_OFFSET(node,pos,tOrder) ((node)->keyTable[(pos)*2])
#define KEY_PTR_SIZE(tOrder) (KEY_PTR_LEN(tOrder)*sizeof(uint32_t)*2)

#define KEY_TO_BDATA(tree,node,pos,bdata)\
   do {\
      /*uint32_t * d= KEY_DATA_OFFSET(node,pos,TORDER(tree));\
      (bdata)->data = node->buf+d[0];\
      (bdata)->len  = d[1];*/\
      (bdata)->data = (node)->buf+(node)->keyTable[(pos)*2];\
      (bdata)->len = (node)->keyTable[(pos)*2+1];\
      /*(bdata)->data = KEY_DATA(node,pos,TORDER(tree));\
      (bdata)->len  = KEY_LEN(node,pos,TORDER(tree));*/\
   }while(0);


pthread_mutex_t dbLock = PTHREAD_MUTEX_INITIALIZER;
void DBTreeIntNodePrint(MorphDBDiskTree* tree,MorphDBIntNode* node,uint8_t ptrs);
static void MorphDBIntNodeFree(MorphDBIntNode* node);
static void MorphDBIntNodePrint(BlockMgrHandlePtr tree,MorphDBIntNode* node);
static inline uint8_t 
MorphDBIntNodeSetPtr(BlockMgrHandlePtr tree,
                     MorphDBIntNode *node,
                     int pos,
                     BlockMgrDiskPtr ptr);

void DBTreePrint(MorphDBDiskTree* tree);
static uint8_t 
MorphDBIntNodeInsertKey(BlockMgrHandlePtr tree,
                        MorphDBIntNode *node,
                        BTreeKey key,
                        int pos,BlockMgrDiskPtr ptr);

void 
DBTreeIntNodeCheck(MorphDBDiskTree* tree,
                   MorphDBIntNode* node,
                   MorphDBIntNode* parent , int parIdx);

MorphDBIntNode*
DBTreeGetRoot(MorphDBDiskTree* tree) {
   if (!tree->nodeCache) {
      return tree->root;
   }   
   LIST_REMOVE(&tree->lruNodeList,&tree->root->lruNode);
   LIST_ADD_AFTER(&tree->root->lruNode,LIST_LAST(&tree->lruNodeList));
   return tree->root;
}

uint8_t
DBTreeSetRoot(MorphDBDiskTree* tree,MorphDBIntNode* root) {
   BTreeValue val;
   uint8_t newPtr;


   tree->root = root;
   tree->hdr->rootNodeOffset = root->nodeOffset;
   val.data = (void*)tree->hdr;
   val.len = sizeof(*tree->hdr);
   if (!BlockMgrSetUserData(tree->handle,&val)) {
      BErrToMErr(tree);
      return False;
   }

   DBTreeGetRoot(tree);
   return True;
}

static uint8_t DBTreeSetCountDirty(MorphDBDiskTree* tree,uint32_t v) {

   BTreeValue val;
   val.data = (char*)tree->hdr;
   val.len  = sizeof(*tree->hdr);
   tree->hdr->countDirty = v;
   if (!BlockMgrSetUserData(tree->handle,&val)) {
      BErrToMErr(tree);
      return False;
   }
   return True;
}

static uint8_t DBTreeSetCount(MorphDBDiskTree* tree) {
   BTreeValue val;
   uint8_t rv;
   uint8_t newPtr;
   val.data = (char*)&tree->count;
   val.len  = sizeof(tree->count);
   rv = BlockMgrDataWrite(tree->handle,&val,&tree->hdr->countOffset,&newPtr);
   BErrToMErr(tree);
   if (!rv) {
      return rv;
   }
   return DBTreeSetCountDirty(tree,0);;
}

static int8_t
DBTreeIncCount(MorphDBDiskTree* tree) {
   BTreeValue val;
   uint8_t newPtr;
   if (!tree->hdr->countDirty) {
      if (!DBTreeSetCountDirty(tree,1)){
         return False;
      }
      
   }

   tree->count++;
   //printf("Current count %lu \n",tree->count);
   return True;
   val.data = (char*)&tree->count;
   val.len  = sizeof(tree->count);
   return BlockMgrDataWrite(tree->handle,&val,&tree->hdr->countOffset,&newPtr);
}

static int8_t
DBTreeDecCount(MorphDBDiskTree* tree) {
   BTreeValue val;
   uint8_t newPtr;
   if (!tree->hdr->countDirty) {
      if (!DBTreeSetCountDirty(tree,0)){
         return False;
      }
      
   }

   tree->count--;
   return True;
   if (!tree->hdr->countDirty) {

   }
   tree->count--;
   val.data = (char*)&tree->count;
   val.len  = sizeof(tree->count);
   return BlockMgrDataWrite(tree->handle,&val,&tree->hdr->countOffset,&newPtr);
}

static uint8_t DBTreeGetCount(MorphDBDiskTree* tree,uint64_t* count) {
   uint64_t c=0;
   MorphDBBTreeCursor* cursor;
   //DBTreePrint(tree);
   if (!MorphDBBTreeCursorInit(tree,&cursor)){
      return False;
   }
   do {
      c += NKEYS(cursor->curNode);
      cursor->curIdx = NKEYS(cursor->curNode); 
   }while(MorphDBBTreeCursorNext(cursor));
   *count = c;
   MorphDBBTreeCursorFree(cursor);
   return True;
}

uint8_t MorphDBNodeCacheCmp(void* fullData,void* searchData) {
   MorphDBIntNode* node = fullData;
   return node->nodeOffset.offset == ((BlockMgrDiskPtr*)searchData)->offset;
}

void
DBTreeIntNodePrint(MorphDBDiskTree* tree,MorphDBIntNode* node,uint8_t ptrs) {
   int i;
   assert(NKEYS(node) <= TORDER(tree));
   if (node->parent) {
      //assert(NKEYS(node) > ((TORDER(tree)/2)-1));
   } else {
      //printf("Root keys %u \n",NKEYS(node));
   }
   if (ptrs) {
      printf("%p %lu:%s [ ",node,node->nodeOffset.offset,ISLEAF(node)?"L":"I");
   } else {
      printf("%s [ ",ISLEAF(node)?"L":"I");
   }
   for (i=0;i<NKEYS(node);i++) {
      BTreeKey key;
      BlockMgrDiskPtr ptr;

      ptr = *PTR_DATA(node,i,TORDER(tree));
      KEY_TO_BDATA(tree,node,i,&key);
      BTreeKeyPrint(tree,&key);
      if (ptrs) {
         printf(":%lu ",ptr.offset);
      } else {
         printf(" ");
      }
      /*
      if (ptrs) {
         printf(" %d:%p ",node->keys[i].key,node->ptrs[i]);
      } else {
         printf(" %d ",node->keys[i].key,node->ptrs[i]);
      }*/
   }
   if (ptrs) {
      printf(" %lu ]  ",ISLEAF(node) ? LNEXT(node).offset : _O(PTR_DATA(node,NKEYS(node),TORDER(tree))));

   } else {
      printf("]");
   }
  // DBTreeIntNodeCheck(tree,node,node->parent,0);
}

void
DBTreePrint(MorphDBDiskTree* tree) {
   ListNode l1,l2,*l;
   ListNode* node;
   BlockMgrDiskPtr *tmpPtr;
   LIST_INIT(&l1);
   LIST_INIT(&l2);

   node = malloc(sizeof(ListNode));
   node->data = tree->root; 
   tmpPtr = malloc(sizeof(BlockMgrDiskPtr)*2);
   memset(&tmpPtr[0],0,sizeof(tmpPtr[0]));
   tmpPtr[1] = tree->root->nodeOffset;

   node->data = tmpPtr;
   LIST_ADD_AFTER(node,LIST_LAST(&l1));
   l = &l1;
   printf("\n BTREE PRINT START \n");
   while (!LIST_ISEMPTY(l)) {
      int i;
      MorphDBIntNode* node=  LIST_FIRST(l)->data,*child,*parent=NULL;
      BlockMgrDiskPtr *nodePtr = LIST_FIRST(l)->data;
      if (nodePtr[0].offset) {
         assert(DBTreeLoadNode(tree,nodePtr[0],NULL,&parent));
      }
      assert(DBTreeLoadNode(tree,nodePtr[1],parent,&node));
      DBTreeIntNodePrint(tree,node,1);
      if (!ISLEAF(node)){
         ListNode *lnode;
         for (i=0;i<=NKEYS(node);i++) {
            BlockMgrDiskPtr ptr;
            BlockMgrDiskPtr *tmpPtr;
            lnode = malloc(sizeof(ListNode));
            ptr = *PTR_DATA(node,i,TORDER(tree));
            //lnode->data = malloc(sizeof(BlockMgrDiskPtr));
            //DBTreeLoadNode(tree,ptr,node,&child);  
            //printf("Adding node %lu\n",node->ptrs[i].offset);
            tmpPtr = malloc(sizeof(BlockMgrDiskPtr)*2);
            tmpPtr[1] = ptr;
            tmpPtr[0] = node->nodeOffset;
            assert(tmpPtr[1].offset);
            assert(tmpPtr[0].offset);
            lnode->data = tmpPtr; 
            if (!ptr.offset) {
               DBTreeIntNodePrint(tree,node,1);
               printf("\n");
               assert(lnode->data);
            }
            if (l == &l1) {
               LIST_ADD_AFTER(lnode,LIST_LAST(&l2));
            }else{
               LIST_ADD_AFTER(lnode,LIST_LAST(&l1));
            }
         }
      }
      /*if (node != tree->root) {
         MorphDBIntNodeFree(node);
      }*/
      LIST_REMOVE(l,LIST_FIRST(l));
      free(nodePtr);
      if (LIST_ISEMPTY(l)){
         printf("\n");
         if (l == &l1) {
            l = &l2;
         } else {
            l = &l1;
         }
      }
   }
   printf("\n BTREE PRINT END \n");
}

void 
DBTreeIntNodeCheck(MorphDBDiskTree* tree,
                   MorphDBIntNode* node,
                   MorphDBIntNode* parent , int parIdx) {
   int i,j;
   int k = ISLEAF(node) ? NKEYS(node)-1:NKEYS(node);
   BTreeKey src,dst;
   assert(node);
   assert(node->parent == parent);
   //assert(NKEYS(node)< TORDER(tree));
   assert(!parent || NKEYS(node) >= DBTreeMinNodes(tree));
   /*printf("Node check\n");
   printf("Node : ");
   DBTreeIntNodePrint(tree,node,1);
   printf("Parent : ");
   DBTreeIntNodePrint(tree,parent,1);
   printf("\n");*/
   for (i=0;i<=k;i++){
      assert(_O(PTR_DATA(node,i,TORDER(tree))));
      for (j=0;j<=k;j++){
         if (i!=j) {
            assert(_O(PTR_DATA(node,i,TORDER(tree))) != _O(PTR_DATA(node,j,TORDER(tree))));
            KEY_TO_BDATA(tree,node,i,&src);
            KEY_TO_BDATA(tree,node,j,&src);
            //assert(src.data != dst.data);
         }
      }
   }
   for (i=0;i<NKEYS(node);i++) {
      BTreeKey src,dst;
      KEY_TO_BDATA(tree,node,i,&src);
      if (i) {

         KEY_TO_BDATA(tree,node,i-1,&dst);
         assert(BTreeKeyCmp(tree,&src,&dst)>0);
      }
/*      if (parIdx < NKEYS(parent)){
         KEY_TO_BDATA(tree,parent,parIdx,&dst);
         assert(BTreeKeyCmp(tree,&dst,&src)>=0);
      }*/
   }
}

void
DBTreeCheck(MorphDBDiskTree* tree) {
   ListNode l1,l2,*l;
   ListNode* node;
   LIST_INIT(&l1);
   LIST_INIT(&l2);

   node = malloc(sizeof(ListNode));
   node->data = tree->root; 
   LIST_ADD_AFTER(node,LIST_LAST(&l1));
   l = &l1;
   //printf("\n BTREE PRINT START \n");
   while (!LIST_ISEMPTY(l)) {
      int i;
      ListNode* n = LIST_FIRST(l);
      MorphDBIntNode* node=  LIST_FIRST(l)->data,*child;
      //DBTreeIntNodePrint(tree,node,1);
      if (!ISLEAF(node)){
         ListNode *lnode;
         for (i=0;i<=NKEYS(node);i++) {
            BlockMgrDiskPtr ptr;
            lnode = malloc(sizeof(ListNode));
            ptr = *PTR_DATA(node,i,TORDER(tree));
            assert(DBTreeLoadNode(tree,ptr,node,&child));
            //printf("Adding node %lu\n",node->ptrs[i].offset);
            lnode->data = child; 
            DBTreeIntNodeCheck(tree,child,node,i);
            if (!ptr.offset) {
               //DBTreeIntNodePrint(tree,node,1);
               printf("\n");
               assert(lnode->data);
            }
            if (l == &l1) {
               LIST_ADD_AFTER(lnode,LIST_LAST(&l2));
            }else{
               LIST_ADD_AFTER(lnode,LIST_LAST(&l1));
            }
         }
      }
      /*if (node != tree->root) {
         MorphDBIntNodeFree(node);
      }*/

      LIST_REMOVE(l,LIST_FIRST(l));
      free(n);
      if (LIST_ISEMPTY(l)){
    //     printf("\n");
         if (l == &l1) {
            l = &l2;
         } else {
            l = &l1;
         }
      }
   }
//   printf("\n BTREE PRINT END \n");
}
void
DBTreeCheckLRUList(MorphDBDiskTree* tree) {
   ListNode* node=NULL;
   LIST_FOR_ALL(&tree->lruNodeList,node) {
      assert(node->data);
   }
}
static inline int
DBTreeNodeBinSearch(MorphDBDiskTree* tree,MorphDBIntNode* node,BTreeKey* key) {
   int numKeys = NKEYS(node),rv=0;
   int start = 0, end = numKeys-1,i=start+((end-start)/2);
   BTreeKey key2;
   if (!numKeys) {
      return 0;
   }
   assert(NKEYS(node) < TORDER(tree));
   //printf("NODE  ");
   //DBTreeIntNodePrint(tree,node,True);
   //printf("\n");
   //printf("STARTING\n");
   while (True) {
      //printf("START %d , END %d \n",start,end);
      if (end-start == 1) {
         //rv = BTreeKeyCmp(tree,&node->keys[start],key);
         KEY_TO_BDATA(tree,node,start,&key2);
         if (BTreeKeyCmp(tree,&key2,key )>= 0) {
            return start;
         } 
         KEY_TO_BDATA(tree,node,end,&key2);
         if (BTreeKeyCmp(tree,&key2,key) >=0) {
            return end;
         }
      }
      if (end == start) {
         KEY_TO_BDATA(tree,node,end,&key2);
         if (BTreeKeyCmp(tree,&key2,key)>=0){
            return end;
         }
         break;
      }
      
      KEY_TO_BDATA(tree,node,i,&key2);
      rv = BTreeKeyCmp(tree,&key2,key);
      if (rv == 0) {
         return i;
      }

      if (rv>0) {
         end = i;
         i = start+ (end-start)/2;
      } else {
         start = i+1;
         i = start+ (end-start)/2;
      }
      assert(i >= 0);
      assert(end >= start);
   }
   return numKeys;
}

static inline uint8_t
DBTreeNodeBinSearchEq(MorphDBDiskTree* tree,MorphDBIntNode* node,BTreeKey* key,int* pos) {
   int numKeys = NKEYS(node),rv=0;
   int start = 0, end = numKeys-1,i=start+((end-start)/2);
   BTreeKey key2;
   if (!numKeys) {
      *pos = 0;
      return False;
   }
   *pos =  DBTreeNodeBinSearch(tree,node,key);
   if (*pos >= NKEYS(node)) {
      return False;
   }
   KEY_TO_BDATA(tree,node,*pos,&key2);
   if (BTreeKeyCmp(tree,&key2,key) ==0) {
      return True;
   }
   return False;
#if 0
   while (True) {
      if (end-start == 1) {
         //rv = BTreeKeyCmp(tree,&node->keys[start],key);
         KEY_TO_BDATA1(tree,node,start,&key2);
         if (BTreeKeyCmp(tree,&key2,key ) == 0) {
            return start;
         } 
         KEY_TO_BDATA1(tree,node,end,&key2);
         if (BTreeKeyCmp(tree,&key2,key)==0) {
            return end;
         }

      }
      if (end == start) {
         KEY_TO_BDATA1(tree,node,end,&key2);
         if (BTreeKeyCmp(tree,&key2,key)==0){
            return end;
         }
         break;
      }
      KEY_TO_BDATA1(tree,node,i,&key2);
      rv = BTreeKeyCmp(tree,&key2,key);
      if (rv == 0) {
         return i;
      }

      if (rv>0) {
         end = i;
         i = start+ (end-start)/2;
      } else {
         start = i+1;
         i = start+ (end-start)/2;
      }
   }
   return -1;
#endif
}

MorphDBIntNode* DBTreeCachedNode(MorphDBDiskTree* tree,BlockMgrDiskPtr nodePtr){
   MorphDBIntNode *node;
   node = HTFind(tree->nodeCache,&nodePtr,sizeof(nodePtr));
   return node;
}

static uint8_t
DBTreeLoadNode(MorphDBDiskTree* tree,
               BlockMgrDiskPtr nodePtr,
               MorphDBIntNode* parent,
               MorphDBIntNode ** outNode){

   MorphDBIntNode* node;
   void *exist;
   BTreeValue val;

   assert(nodePtr.offset);
   //printf("Load node hash table %lu\n",nodePtr.offset);
   node = HTFind(tree->nodeCache,&nodePtr,sizeof(nodePtr));
   if (node) {
      //printf("Returning from hash table %p %lu\n",node,nodePtr.offset);
      LIST_REMOVE(&tree->lruNodeList,&node->lruNode);
      LIST_ADD_AFTER(&node->lruNode,LIST_LAST(&tree->lruNodeList));
      // when a leaf node is loaded using LNEXT()
      // parent value can get screwed up.
      // is this setting to parent node needed ?
      // y do we need to set the parent node if it is
      // in cache?
      // XXX XXX XXX
      node->parent = parent;
      *outNode = node;
      return True;
   }
#if DEBUG
   printf("Cache miss %lu \n",_O(&nodePtr));
   //assert(!a);
   //a++;
#endif
   
   node = malloc(sizeof(*node));
   if (!node) {
      SetDBTreeError(tree,MORPHDB_NO_MEMORY);
      return False;   
   }
   
   if (!BlockMgrDataRead(tree->handle,nodePtr,&val)){
      BErrToMErr(tree);
      free(node);
      return False;
   }
   node->desc = (void*) val.data;
   node->keyPtrs = node->desc->keyPtrs;
   node->ptrPtrs = node->desc->ptrPtrs;
   node->buf = node->desc;
   node->keyTable = (uint32_t*)(node->buf + node->desc->keyPtrs[0]);
   node->ptrTable = (uint32_t*)(node->buf + node->desc->ptrPtrs[0]);
   node->nodeOffset = nodePtr;
   node->parent = parent;
   
   //DBTreeIntNodePrint(tree,node,True);

   MorphDBInsertNodeToCache(tree,node);
   /*tree->numNodesInCache++;
   printf("Number of nodes in cache %u\n",tree->numNodesInCache);
   HTInsert(tree->nodeCache,node,&nodePtr,sizeof(nodePtr),&exist);*/
   //printf("LOADED NODE %p %p\n",node,node->desc);
   *outNode = node;
   //assert(node->desc->totalLength <1000);
   return True;   
}

static uint8_t
DBTreeFindLeaf(MorphDBDiskTree* tree,BTreeKey* key,MorphDBIntNode** outNode) {
   MorphDBIntNode* node = DBTreeGetRoot(tree);
   //printf("Root node %p\n",node);
   while (!ISLEAF(node)) {
      uint8_t found = False;
      int j = DBTreeNodeBinSearch(tree,node,key);
      //printf("Loading node %lu\n",node->ptrs[j].offset);
      //MorphDBIntNodePrint(tree,node);
      BlockMgrDiskPtr ptr = *PTR_DATA(node,j,TORDER(tree));
      //assert(ptr.offset);
      if (!DBTreeLoadNode(tree,ptr,node,&node)) {
         return False;
      }
      //printf("Loaded node %p\n",node);
      //DBTreeIntNodePrint(tree,node,True);
   } 
   //assert(node);
   *outNode = node;
   return True;
}

static void
MorphDBIntNodeFree(MorphDBIntNode* node) {
   free(node->desc);
   memset(node,0,sizeof(*node));
   //node->diskNode = NULL;
   //node->keys = NULL;
   //node->ptrs = NULL;
   free(node);
}

int nodeCount;
static uint8_t MorphDBInsertNodeToCache(MorphDBDiskTree* tree, MorphDBIntNode* node){
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

static uint8_t 
MorphDBIntNodeInit(MorphDBDiskTree* tree,
                   MorphDBIntNode* parent,
                   MorphDBIntNode** outNode){
   //printf("In allocate node \n");
   MorphDBIntNode *node;
   uint32_t tOrder = TORDER(tree);
   uint32_t initLength = sizeof(*node->desc) + KEY_PTR_SIZE(tOrder)*1+PTR_SIZE(tOrder)*1;

   node = malloc(sizeof(*node));
   
   if (!node) {
      SetDBTreeError(tree,MORPHDB_NO_MEMORY);
      return False;
   }
   memset(node,0,sizeof(*node));
   memset(&node->nodeOffset,0,sizeof(node->nodeOffset));
   node->desc = malloc(initLength);
   if (!node->desc) {
      SetDBTreeError(tree,MORPHDB_NO_MEMORY);
      return False;
   }
   NKEYS(node) = 0;
   memset(node->desc,0,initLength);
   node->buf = node->desc;
   node->desc->totalLength = initLength;
   node->desc->actualDataLength = initLength;
   node->desc->keyPtrs[0] = sizeof(*node->desc);
   //node->desc->keyPtrs[1] = node->desc->keyPtrs[0]+KEY_PTR_SIZE(tOrder);
   node->desc->ptrPtrs[0] = node->desc->keyPtrs[0]+KEY_PTR_SIZE(tOrder);
   //node->desc->ptrPtrs[1] = node->desc->ptrPtrs[0]+PTR_SIZE(tOrder);
   node->keyTable = (uint32_t*)(node->buf + node->desc->keyPtrs[0]);
   node->ptrTable = (uint32_t*)(node->buf + node->desc->ptrPtrs[0]);
   //tree->numIntNodes++;  //rew- why tree has 3435973837 nodes already ?
   
   //assert(node->nodeOffset.offset < 1000);
   //memset(node->diskNode,0,TINTNODE_SIZE(tree));
   node->lruNode.data = node;
#if 0
   node->dNode = (MorphDBINode*) node->diskNode;
   node->parent = parent;
   node->keys = (uint8_t*)(node->diskNode+sizeof(MorphDBINode)); 
   node->ptrs = (BlockMgrDiskPtr*)(node->keys+TKSIZE(tree)*TORDER(tree));
#endif
   //node->nodeOffset = nodePtr;
   NKEYS(node) = 0;
   ISLEAF(node) = True;
   node->parent = parent;
   //MorphDBInsertNodeToCache(tree,node);
   //LIST_ADD_AFTER(&node->lruNode,LIST_LAST(&tree->lruNodeList));

   //printf("Intnode init %p %p %p\n",node,node->desc,parent);
   *outNode = node;
   return True;   
}

uint8_t 
MorphDBIntNodeWrite(MorphDBDiskTree* tree,MorphDBIntNode* node,uint8_t* newPtr) {
   size_t s;
   double t1=UTime(),t2;
   BTreeValue val;
   BlockMgrDiskPtr ptr,oldPtr;
   void * exist = NULL;
   uint8_t flag;
   
   /*
   s = TINTNODE_SIZE(tree);
   if(!_MorphDBFileWrite(tree->handle->fileHandle,node->diskNode,s,_O(&node->nodeOffset))) {
      return False;
   } */
//   printf("IntNODE WRITE \n");
   if (node->parent) {
//      assert(DBTreeIsNodeHalfFull(tree,node));
   }
   val.data = node->buf;
   val.len = node->desc->totalLength;
   //printf("Node length %u\n",val.len);
   ptr = node->nodeOffset;

   if (!BlockMgrDataWrite(tree->handle,&val,&ptr,newPtr)){
      BErrToMErr(tree);
      return False;
   }
   oldPtr = node->nodeOffset;
   node->nodeOffset = ptr;
   if (*newPtr && tree->nodeCache){
      if (_O(&oldPtr)) {
         int i =0 ;
         MorphDBIntNode* parent = node->parent;
         HTRemove(tree->nodeCache,&node->nodeOffset,sizeof(node->nodeOffset));
         LIST_REMOVE(&tree->lruNodeList,&node->lruNode);
         tree->numNodesInCache--;
         if (!parent) {
            // Set root offset;
            //assert(tree->root == parent);
            return DBTreeSetRoot(tree,node);
         }
         while (i <= NKEYS(parent) && _O(&oldPtr) != _O(PTR_DATA(parent,i,tOrder))) {
            i++;
         }
         if (i > NKEYS(parent)) {
            assert(0);
         }
         *PTR_DATA(parent,i,tOrder) = node->nodeOffset;
         if (!MorphDBIntNodeWrite(tree,parent,&flag)) {
            return False;
         }
      } 
      
      tree->numIntNodes++;
      MorphDBInsertNodeToCache(tree,node);
      void* tmp = DBTreeCachedNode(tree,node->nodeOffset);
      assert(tmp == node);

      //HTInsert(tree->nodeCache,node,&node->nodeOffset,sizeof(node->nodeOffset),&exist);

      //LIST_ADD_AFTER(&node->lruNode,LIST_LAST(&tree->lruNodeList));
      //assert(!exist);
   }

#if DEBUG
   printf("WRITING %lu %u ",_O(&node->nodeOffset),node->desc->totalLength);

   DBTreeIntNodePrint(tree,node,True);
   printf("\n");
#endif
   t2=UTime();
   tree->intNodeWriteTime += (t2-t1);
   return True;
}

int
MorphDBFormatTree(char* fileName,int treeOrder) {
   MorphDBDiskTree tree;
   int rv;
   MorphDBTreeHeader hdr;
   BTreeValue val;
   BlockMgrDiskPtr ptr;
   uint8_t newPtr;
   MorphDBIntNode *root;
      
   memset(&tree,0,sizeof(tree));
   hdr.magic = MORPHDB_TREE_MAGIC;
   hdr.majorVersion = 0;
   hdr.minorVersion = 1;
   hdr.treeOrder = treeOrder > 35 ? 35: treeOrder;
   hdr.countDirty = 0; 
   _SO(&hdr.rootNodeOffset,0);
   _SO(&hdr.countOffset,0);
   if ((rv=BlockMgrFormatFile(fileName,0))) {
      return rv;
   }
   if((rv=BlockMgrInitHandle(fileName,0,&tree.handle))) { 
      return rv;
   }
   val.data = (void*)&hdr;
   val.len = sizeof(hdr);
   if (!BlockMgrSetUserData(tree.handle,&val)){
      BErrToMErr(tree);
      return tMorphDBBTreeError;
   }
   tree.count = 0;
   val.data = (char*)&tree.count;
   val.len = sizeof(tree.count);
   tree.hdr = &hdr;
   if (!BlockMgrDataWrite(tree.handle,&val,&tree.hdr->countOffset,&newPtr) ) {
      BErrToMErr(tree);
      return tMorphDBBTreeError;
   }
   if (!MorphDBIntNodeInit(&tree,NULL,&root)) {
      BlockMgrFreeHandle(tree.handle);
      return tMorphDBBTreeError;
   }
   if (!MorphDBIntNodeWrite(&tree,root,&newPtr)) {
      BlockMgrFreeHandle(tree.handle);
      return tMorphDBBTreeError;
   }
   if (!DBTreeSetRoot(&tree,root)){
      return tMorphDBBTreeError;
   }
   MorphDBIntNodeFree(root);
   BlockMgrFreeHandle(tree.handle);
   return 0;
}

void MorphDBIntNodeDestFn(void * data) {
   return MorphDBIntNodeFree(data);
}

void
MorphDBTreeFree(MorphDBDiskTree * tree) {
   DBTreeSetCount(tree);
   HTDestroy(tree->nodeCache,MorphDBIntNodeDestFn);
   BlockMgrFreeHandle(tree->handle);
   free(tree->hdr);
   free(tree);
}

void
MorphDBTreeRemove(MorphDBDiskTree* tree) {
   HTDestroy(tree->nodeCache,MorphDBIntNodeDestFn);
   BlockMgrRemove(tree->handle);
   free(tree->hdr);
   free(tree);
}

static MorphDBError MorphDBBTreeValidateHeader(MorphDBTreeHeader * hdr) {
   if (hdr->magic != MORPHDB_TREE_MAGIC) {
      return MORPHDB_INVALID_MAGIC; 
   }   
   if (hdr->majorVersion < THIS_LOW_VERSION && hdr->majorVersion > THIS_HIGH_VERSION) {
      return MORPHDB_INVALID_VERSION;
   }
   if (hdr->treeOrder > 35) {
      return MORPHDB_INVALID_HEADER;
   } 
   if (_O(&hdr->rootNodeOffset) == 0) {
      return MORPHDB_INVALID_HEADER;
   } 
   if (_O(&hdr->countOffset) == 0) {
      return MORPHDB_INVALID_HEADER;
   } 
   return MORPHDB_OK;
}

static MorphDBError 
MorphDBInitTreeHandle(char* fileName,int tOrder,uint64_t features,MorphDBDiskTree ** outTree){
   MorphDBDiskTree * tree = malloc(sizeof(MorphDBDiskTree));
   BTreeValue val;
   int rv;

   if (!tree) {
      return MORPHDB_NO_MEMORY;
   }
   memset(tree,0,sizeof(tree));
   if((rv=BlockMgrInitHandle(fileName,0,&tree->handle))) { 
      if ((rv=MorphDBFormatTree(fileName,tOrder))){
         free(tree);
         return rv;
      }
      if((rv=BlockMgrInitHandle(fileName,0,&tree->handle)) ){ 
         free(tree);
         return MorphDBErrorFromBMgrError(rv); //rew-need to return proper error.
      }
   }
   LIST_INIT(&tree->lruNodeList);
   tree->numIntNodes = 0;
   tree->numNodesInCache = 0;
   tree->nodeCacheSize = 1023;
   tree->dataWriteTime = 0;
   tree->intNodeWriteTime = 0;
   tree->count =0;
   tree->gen = 0;
   tree->nodeCache = HTInit(BlockMgrDiskPtrHashFn,MorphDBNodeCacheCmp,1023);
   
   //BlockMgrFormatFile(fileName,features,keySize);

   if (!BlockMgrReadUserData(tree->handle,&val)) {
      BErrToMErr(tree);
      MorphDBTreeFree(tree);
      return tMorphDBBTreeError;
   }
   tree->hdr = (void*)val.data;
   if (val.len != sizeof(*tree->hdr)){
      return MORPHDB_INVALID_HEADER;
   }
   
   if ((rv =MorphDBBTreeValidateHeader(tree->hdr))) {
      MorphDBTreeFree(tree);
      return rv;
   }
   
/*   if (!BlockMgrDataRead(tree->handle,tree->hdr->countOffset,&val)) {
      BErrToMErr(tree);
      MorphDBTreeFree(tree);
      return tMorphDBBTreeError;
   }*/

   if (!DBTreeLoadNode(tree,tree->hdr->rootNodeOffset,NULL,&tree->root)) {
      //XXX FreeHandle
      MorphDBTreeFree(tree);
      //free(tree);
      return MORPHDB_INVALID_MAGIC;
   }
   //tree->keySize = TKSIZE(tree);
   tree->keyOps.cmp = BTreeKeyBStrCmp;
   //tree->keyOps.free = BTreeKeyBStrFree;
   tree->keyOps.print = BTreeKeyBStrPrint; 
   tree->keyOps.copy = BTreeKeyBStrCopy;
   if (tree->hdr->countDirty) {
      if (!DBTreeGetCount(tree,&tree->count)){
         MorphDBTreeFree(tree);
         return tMorphDBBTreeError;
      }
      if (!DBTreeSetCount(tree)) {
         BErrToMErr(tree);
         MorphDBTreeFree(tree);
         return tMorphDBBTreeError;
      }
   } else {
      BTreeValue val;
      if (!BlockMgrDataRead(tree->handle,tree->hdr->countOffset,&val)) {
         BErrToMErr(tree);
         MorphDBTreeFree(tree);
         return tMorphDBBTreeError;
      }
      tree->count = *(uint64_t*)val.data;
      free(val.data);
   }
   
   *outTree = tree;

   return 0;
}

void
DBTreeNodeShiftLeft(MorphDBDiskTree* tree,MorphDBIntNode* node,int shift,uint8_t freeKeys) {
   int i=0;
   BTreeKey src,dst;
   uint32_t tOrder = TORDER(tree);

   if (freeKeys) {
      /*for (i=0;i<shift;i++) {
         free(&node->keys[i]);
      }*/
   } 
   for (i=0;i<NKEYS(node)-shift;i++){
/*      KEY_TO_BDATA1(tree,node,i,&dst);
      KEY_TO_BDATA1(tree,node,i+shift,&src);
      BTreeKeyCopy(tree,&dst,&src,False);
      node->ptrs[i] = node->ptrs[i+shift];*/
      KEY_OFFSET(node,i,tOrder) = KEY_OFFSET(node,i+shift,tOrder);
      node->desc->actualDataLength -= KEY_LEN(node,i,tOrder);
      KEY_LEN(node,i,tOrder) = KEY_LEN(node,i+shift,tOrder);
      *PTR_DATA(node,i,tOrder) = *PTR_DATA(node,i+shift,tOrder);
   } 
   *PTR_DATA(node,i,tOrder) = *PTR_DATA(node,i+shift,tOrder);
   //node->ptrs[i] = node->ptrs[i+shift];
   NKEYS(node)-=shift;
}

uint8_t
MorphDBIntNodeAllocKeysPtrs(MorphDBDiskTree* tree,
                            MorphDBIntNode* node, 
                            int startPos,
                            int endPos) {
   uint32_t tOrder = TORDER(tree);
   int i = startPos/KEY_PTR_LEN(tOrder);
   uint32_t expandLen = 0;
   uint32_t currentLength = node->desc->totalLength;
   void* tmp;
   //printf("i %d %d \n",i,KEY_PTR_LEN(tOrder));
   while ((((i+1)*KEY_PTR_LEN(tOrder))-1) < endPos){
      if (!node->desc->keyPtrs[i]) {
         expandLen += KEY_PTR_SIZE(tOrder);
      }
      i++;
   }
   i = startPos/PTR_PTR_LEN(tOrder);
   while ((((i+1)*PTR_PTR_LEN(tOrder))-1) < endPos){
      if (!node->desc->ptrPtrs[i]) {
         expandLen += PTR_SIZE(tOrder);
      }
      i++;
   }
   tmp = realloc(node->desc,expandLen+currentLength); 
   if (!tmp) {
      SetDBTreeError(tree,MORPHDB_NO_MEMORY);
      return False;
   }
   
   if (tmp!=node->desc) {
      node->desc = tmp;
      node->keyPtrs = node->desc->keyPtrs;
      node->ptrPtrs = node->desc->ptrPtrs;
      node->buf = node->desc;
      node->keyTable = (uint32_t*)(node->buf + node->desc->keyPtrs[0]);
      node->ptrTable = (uint32_t*)(node->buf + node->desc->ptrPtrs[0]);
   }
   memset(node->buf+currentLength,0,expandLen);
   while ((((i+1)*KEY_PTR_LEN(tOrder))-1) < endPos+1){
      if (!node->desc->keyPtrs[i]) {
         node->desc->keyPtrs[i] = currentLength;
         currentLength += KEY_PTR_SIZE(tOrder);
      }
      i++;
   }
   i = startPos/PTR_PTR_LEN(tOrder);
   while ((((i+1)*PTR_PTR_LEN(tOrder))-1) < endPos){
      if (!node->desc->ptrPtrs[i]) {
         node->desc->ptrPtrs[i] = currentLength;
         currentLength += PTR_SIZE(tOrder);
      }
      i++;
   }
   node->desc->totalLength += expandLen;  
   return True;
}

uint8_t 
MorphDBIntNodeSetKey(MorphDBDiskTree* tree,
                     MorphDBIntNode* node,
                     int keyIdx,
                     BTreeKey srcKey) {
   uint32_t expandLen = srcKey.len;   
   uint32_t currentLen = node->desc->totalLength;
   void *tmp;

   uint32_t actualDataLength = node->desc->actualDataLength;
   uint32_t tOrder = TORDER(tree);

   actualDataLength -= KEY_LEN(node,keyIdx,TORDER(tree));
   tmp = realloc(node->desc,expandLen+currentLen); 
   if (!tmp) {
      SetDBTreeError(tree,MORPHDB_NO_MEMORY);
      return False;
   }
   
   if (tmp!=node->desc) {
      node->desc = tmp;
      node->keyPtrs = node->desc->keyPtrs;
      node->ptrPtrs = node->desc->ptrPtrs;
      node->buf = node->desc;
      node->keyTable = (uint32_t*)(node->buf + node->desc->keyPtrs[0]);
      node->ptrTable = (uint32_t*)(node->buf + node->desc->ptrPtrs[0]);
   }
   KEY_OFFSET(node,keyIdx,tOrder) = currentLen;
   KEY_LEN(node,keyIdx,tOrder) = srcKey.len;
   memcpy(node->buf+currentLen,srcKey.data,srcKey.len);
   node->desc->totalLength += expandLen;  
   node->desc->actualDataLength = actualDataLength;
   return True; 
}

uint8_t
DBTreeNodeShiftRight(MorphDBDiskTree* tree,MorphDBIntNode* node,int shift) {
   int i=0;
   BTreeKey src,dst;
   BlockMgrDiskPtr *ptr;
   uint32_t tOrder = TORDER(tree);
   
   //assert((node->numKeys + shift) < sizeof(node->keys)/sizeof(node->keys[0]));
   if (!MorphDBIntNodeAllocKeysPtrs(tree,node,NKEYS(node),NKEYS(node)+shift+1)){
      return False;
   }
   ptr = PTR_DATA(node,NKEYS(node)+shift,tOrder);
   *ptr = *PTR_DATA(node,NKEYS(node),tOrder);
   //node->ptrs[NKEYS(node)+shift] = node->ptrs[NKEYS(node)];
   for (i=NKEYS(node)-1;i>=0;i--){
      /*KEY_TO_BDATA1(tree,node,i+shift,&dst);
      KEY_TO_BDATA1(tree,node,i,&src);
      BTreeKeyCopy(tree,&dst,&src,False);*/
      //node->keys[i+shift] = node->keys[i];
      KEY_OFFSET(node,i+shift,tOrder) = KEY_OFFSET(node,i,tOrder);
      KEY_LEN(node,i+shift,tOrder) = KEY_LEN(node,i,tOrder);
      *PTR_DATA(node,i+shift,tOrder) = *PTR_DATA(node,i,tOrder);
      //node->ptrs[i+shift] = node->ptrs[i];
   } 
   //node->ptrs[i] = node->ptrs[i+shift];
   NKEYS(node)+=shift;
   return True;
}


uint8_t
DBTreeRemoveFromNode(MorphDBDiskTree* tree,MorphDBIntNode* node,int keyIdx,BlockMgrDiskPtr ptr,uint8_t* newPtr) {
   int i= keyIdx;
   BTreeKey dst,src;
   uint32_t tOrder = TORDER(tree);
   BlockMgrDiskPtr *dPtr;

   node->desc->actualDataLength -= KEY_LEN(node,i,tOrder);
   for (;i<NKEYS(node)-1;i++) {
      /*KEY_TO_BDATA1(tree,node,i,&dst);
      KEY_TO_BDATA1(tree,node,i+1,&src);
      BTreeKeyCopy(tree,&dst,&src,False);*/
      KEY_OFFSET(node,i,tOrder) = KEY_OFFSET(node,i+1,tOrder);
      KEY_LEN(node,i,tOrder) = KEY_LEN(node,i+1,tOrder);
   } 
   i=0;
   while (1) {
      dPtr = PTR_DATA(node,i,tOrder);
      if (_O(dPtr) ==_O(&ptr)) {
         break;
      }
      i++;
   }
   if (ISLEAF(node)) {
      // free the ptr
   } else {
      // free the node
   }
   for (;i<NKEYS(node);i++) {
      dPtr = PTR_DATA(node,i,tOrder);
      memcpy(dPtr,dPtr+1,sizeof(*dPtr));
      //node->ptrs[i] = node->ptrs[i+1]; 
      //printf("%d,%p %p\n",i,node->ptrs[i],node->ptrs[i+1]);
   } 
   NKEYS(node)--;
   return MorphDBIntNodeWrite(tree,node,newPtr);
}

#define SET_KEY(tree,node,keyIdx,srcKey,rv)\
   if ((keyIdx) < NKEYS(node) &&  KEY_LEN(node,keyIdx,TORDER(tree)) >= (srcKey).len) {\
      memcpy(KEY_DATA(node,keyIdx,TORDER(tree)),(srcKey).data,(srcKey).len);\
      KEY_LEN(node,keyIdx,TORDER(tree)) = (srcKey).len;\
      (rv) = True;\
   }else{\
      (rv) = MorphDBIntNodeSetKey(tree,node,keyIdx,srcKey);\
   }


uint8_t
DBTreeNodeRedistribute(MorphDBDiskTree* tree,
                       MorphDBIntNode* sibling,
                       MorphDBIntNode* underflow,
                       int parIdx,
                       uint8_t siblingOnLeft){
   BTreeKey src,dst,key;
   MorphDBIntNode* tmp ;
   int i;
   uint32_t tOrder = TORDER(tree);
   BlockMgrDiskPtr *ptr;
   uint8_t rv = True;
   uint8_t oldPtr;

#if DEBUG
   printf("Redistribute %d \n ",siblingOnLeft);
   DBTreeIntNodePrint(tree,sibling->parent,1);
   DBTreeIntNodePrint(tree,underflow,1);
   DBTreeIntNodePrint(tree,sibling,1);
   printf("\n");
#endif
   if (ISLEAF(sibling)) {
      if (!siblingOnLeft) {

         // sibling is on the right
         int totalKeys = NKEYS(sibling)+NKEYS(underflow);
         int numToShift = totalKeys/2 - NKEYS(underflow);

         for (i=0;i<numToShift;i++) {
            /*KEY_TO_BDATA1(tree,underflow,NKEYS(underflow)+i,&dst);
            KEY_TO_BDATA1(tree,sibling,i,&src);
            BTreeKeyCopy(tree,&dst,&src,False);
            underflow->ptrs[NKEYS(underflow)+i] = sibling->ptrs[i];*/
            ptr = PTR_DATA(sibling,i,tOrder);
            KEY_TO_BDATA(tree,sibling,i,&src);
            if (!MorphDBIntNodeInsertKey(tree,underflow,src,NKEYS(underflow)+i,*ptr)) {
               return False;
            }
            //KEY_OFFSET(node,i,tOrder) = KEY_OFFSET(node,i+1,tOrder);
            //KEY_LEN(node,i,tOrder) = KEY_LEN(node,i+1,tOrder);
            //BTreeInsertToNode(underflow,&sibling->keys[i],(BTreeValue*)sibling->ptrs[i],NULL);
         }

         //NKEYS(underflow) += numToShift;
         DBTreeNodeShiftLeft(tree,sibling,numToShift,False);

         //KEY_TO_BDATA1(tree,sibling->parent,parIdx,&dst);
         KEY_TO_BDATA(tree,underflow,NKEYS(underflow)-1,&src);
         SET_KEY(tree,sibling->parent,parIdx,src,rv);
         if (!rv) {
            return rv;
         }
         //BTreeKeyCopy(tree,&dst,&src,False);

      } else {
         // sibling is on the left
         int numToShift = (NKEYS(sibling)+NKEYS(underflow))/2-(NKEYS(underflow));
         DBTreeNodeShiftRight(tree,underflow,numToShift);
         for (i=0;i<numToShift;i++) {
            KEY_TO_BDATA(tree,sibling,NKEYS(sibling)-numToShift+i,&src);
            SET_KEY(tree,underflow,i,src,rv)
            if (!rv) {
               return rv;
            }
            //KEY_TO_BDATA1(tree,underflow,i,&dst);
            //BTreeKeyCopy(tree,&dst,&src,False);
            *PTR_DATA(underflow,i,tOrder) = *PTR_DATA(sibling,NKEYS(sibling)-numToShift+i,tOrder);
            //underflow->ptrs[i] = sibling->ptrs[NKEYS(sibling)-numToShift+i];
         }
         NKEYS(sibling) -= numToShift;
         /*KEY_TO_BDATA1(tree,sibling->parent,parIdx,&dst);
         KEY_TO_BDATA1(tree,sibling,NKEYS(sibling)-1,&src);
         BTreeKeyCopy(tree,&dst,&src,False);*/
         KEY_TO_BDATA(tree,sibling,NKEYS(sibling)-1,&src);
         SET_KEY(tree,sibling->parent,parIdx,src,rv);
         if (!rv) {
            return rv;
         }
      }
      if (!MorphDBIntNodeWrite(tree,sibling,&oldPtr)){
         return False;
      } 
      if (!MorphDBIntNodeWrite(tree,underflow,&oldPtr)){
         return False;
      } 
      if (!MorphDBIntNodeWrite(tree,underflow->parent,&oldPtr)){
         return False;
      } 
#if DEBUG
      printf("Redistribute END %d \n ",siblingOnLeft);
      DBTreeIntNodePrint(tree,sibling->parent,1);
      DBTreeIntNodePrint(tree,underflow,1);
      DBTreeIntNodePrint(tree,sibling,1);
      printf("\n");
#endif
      return True;
   }

   if (!siblingOnLeft) {

      /*KEY_TO_BDATA1(tree,sibling->parent,parIdx,&src);
      KEY_TO_BDATA1(tree,underflow,NKEYS(underflow),&dst);
      BTreeKeyCopy(tree,&dst,&src,False);*/

      KEY_TO_BDATA(tree,sibling->parent,parIdx,&src);
      SET_KEY(tree,underflow,NKEYS(underflow),src,rv);
      if (!rv) {
         return False;
      }
      *PTR_DATA(underflow,NKEYS(underflow)+1,tOrder) = *PTR_DATA(sibling,0,tOrder);
      //underflow->ptrs[NKEYS(underflow)+1] = sibling->ptrs[0];
      //sibling->ptrs[0]->parent = underflow;
      tmp = DBTreeCachedNode(tree,*PTR_DATA(sibling,0,tOrder));
      if (tmp) {
         tmp->parent = sibling;
      }
      /*KEY_TO_BDATA1(tree,sibling->parent,parIdx,&dst);
      KEY_TO_BDATA1(tree,sibling,0,&src);
      BTreeKeyCopy(tree,&dst,&src,False);*/

      KEY_TO_BDATA(tree,sibling,0,&src);
      SET_KEY(tree,sibling->parent,parIdx,src,rv);
      if (!rv) {
         return False;
      }
      DBTreeNodeShiftLeft(tree,sibling,1,False);
      NKEYS(underflow)++;

   } else {
      // sibling on left
      DBTreeNodeShiftRight(tree,underflow,1); 
      /*
      KEY_TO_BDATA1(tree,underflow->parent,parIdx,&src);
      KEY_TO_BDATA1(tree,underflow,0,&dst);
      BTreeKeyCopy(tree,&dst,&src,False);*/

      KEY_TO_BDATA(tree,sibling->parent,parIdx,&src);
      SET_KEY(tree,underflow,0,src,rv);
      if (!rv) {
         return False;
      }

      //underflow->ptrs[0] = sibling->ptrs[NKEYS(sibling)];
      *PTR_DATA(underflow,0,tOrder) = *PTR_DATA(sibling,NKEYS(sibling),tOrder);
      //underflow->ptrs[0]->parent = underflow;
      tmp = DBTreeCachedNode(tree,*PTR_DATA(sibling,0,tOrder));
      if (tmp) {
         tmp->parent = underflow;
      }
      /*
      KEY_TO_BDATA1(tree,underflow->parent,parIdx,&dst);
      KEY_TO_BDATA1(tree,sibling,NKEYS(sibling)-1,&src);

      BTreeKeyCopy(tree,&dst,&src,False);*/

      KEY_TO_BDATA(tree,sibling,NKEYS(sibling)-1,&src);
      SET_KEY(tree,underflow->parent,parIdx,src,rv);
      if (!rv) {
         return False;
      }
      NKEYS(sibling)--;
      //printf("after Redistribute left sibling,underflow:");BTreeNodePrint(tree,sibling,1);BTreeNodePrint(tree,underflow,1);printf("\n");
   }

   if (!MorphDBIntNodeWrite(tree,sibling,&oldPtr)){
      return False;
   } 
   if (!MorphDBIntNodeWrite(tree,underflow,&oldPtr)){
      return False;
   } 
   if (!MorphDBIntNodeWrite(tree,sibling->parent,&oldPtr)) {
      return False;
   }
   return True;
}

uint8_t
DBTreeMergeNodes(MorphDBDiskTree* tree,
                MorphDBIntNode* left,
                MorphDBIntNode* right ,
                int parIdx) {
   BTreeKey src,dst;
   MorphDBIntNode* tmp;
   int i,j;
   uint8_t newPtr;
   uint8_t rv;
   uint32_t tOrder = TORDER(tree);
   
#if DEBUG
   printf("Merge nodes %d \n ",parIdx);
   DBTreeIntNodePrint(tree,left->parent,1);
   DBTreeIntNodePrint(tree,left,1);
   DBTreeIntNodePrint(tree,right,1);
   printf("\n");
#endif
   if (!MorphDBIntNodeAllocKeysPtrs(tree,left,NKEYS(left),NKEYS(left)+NKEYS(right))){
      return False;
   }

   if (ISLEAF(left)) {
      LNEXT(left) = LNEXT(right);
      for (i=(NKEYS(left)),j=0;j<NKEYS(right);j++,i++) {
   /*      KEY_TO_BDATA1(tree,left,i,&dst);
         KEY_TO_BDATA1(tree,right,j,&src);
         BTreeKeyCopy(tree,&dst,&src,False);*/

         //KEY_TO_BDATA1(tree,left,i,&dst);
         KEY_TO_BDATA(tree,right,j,&src);
         KEY_TO_BDATA(tree,left,i,&dst);
         *PTR_DATA(left,i,tOrder) = *PTR_DATA(right,j,tOrder);
         SET_KEY(tree,left,i,src,rv);
         if (!rv) {
            return False;
         }
      }
      NKEYS(left) += NKEYS(right);
      if (!DBTreeRemoveFromNode(tree,left->parent,parIdx,right->nodeOffset,&newPtr)) {
         return False;
      }
#if DEBUG
   printf("After Merge nodes %d \n ",parIdx);
   DBTreeIntNodePrint(tree,left->parent,1);
   DBTreeIntNodePrint(tree,left,1);
   DBTreeIntNodePrint(tree,right,1);
   printf("\n");
#endif

      return MorphDBIntNodeWrite(tree,left,&newPtr);
   }
   /* Internal node merge.
    * 1. Bring the parent node down.
    * 2. Add all keys from right to left.
    * 3. Remove parent key and right pointer from parent.
    */
   /*KEY_TO_BDATA1(tree,left,NKEYS(left),&dst);
   KEY_TO_BDATA1(tree,right->parent,parIdx,&src);
   BTreeKeyCopy(tree,&dst,&src,False);*/

   KEY_TO_BDATA(tree,right->parent,parIdx,&src);
   SET_KEY(tree,left,NKEYS(left),src,rv);
   if (!rv){
      return False;
   }
   NKEYS(left)++;

   for (i=NKEYS(left),j=0;j<NKEYS(right);j++,i++) {

      KEY_TO_BDATA(tree,left,i,&dst);
      KEY_TO_BDATA(tree,left,i-1,&src);
      assert(src.data != dst.data);
      //BTreeKeyCopy(tree,&dst,&src,False);
      
      KEY_TO_BDATA(tree,right,j,&src);
      SET_KEY(tree,left,i,src,rv);
      if (!rv){
         return False;
      }
      //left->keys[i] = right->keys[j];
      //left->ptrs[i] = right->ptrs[j];
      *PTR_DATA(left,i,tOrder) = *PTR_DATA(right,j,tOrder);
      
      tmp = DBTreeCachedNode(tree,*PTR_DATA(left,i,tOrder));
      if (tmp) {
         tmp->parent = left;
      }
      NKEYS(left)++;
   }
   //NKEYS(left) += NKEYS(right)+1;
   *PTR_DATA(left,i,tOrder) = *PTR_DATA(right,j,tOrder);
   //left->ptrs[i] = right->ptrs[j];
   tmp = DBTreeCachedNode(tree,*PTR_DATA(left,i,tOrder));
   if (tmp) {
      tmp->parent = left;
   }
   //left->ptrs[i]->parent = left;
   //printf("After merge int left,right:");BTreeNodePrint(tree,left,1);BTreeNodePrint(tree,right,1);printf("\n");
   // Key from parent has moved to the child dont free the key
   if (!DBTreeRemoveFromNode(tree,left->parent,parIdx,right->nodeOffset,&newPtr)) {
      return False;
   }
   
#if DEBUG
   printf("After merge nodes %d \n ",parIdx);
   DBTreeIntNodePrint(tree,left->parent,1);
   DBTreeIntNodePrint(tree,left,1);
   DBTreeIntNodePrint(tree,right,1);
   printf("\n");
#endif
   // Remove parIdx from parent
   return MorphDBIntNodeWrite(tree,left,&newPtr);
   return True;
}

uint8_t
DBTreeDelete(MorphDBDiskTree* tree,BTreeKey* key) {
   MorphDBIntNode* node = NULL;
   MorphDBIntNode* parent,*sibling=NULL;
   int i;
   uint8_t newPtr;
   BlockMgrDiskPtr *ptr;

   if (!DBTreeFindLeaf(tree,key,&node)){
      return False;
   }

   if (!DBTreeNodeBinSearchEq(tree,node,key,&i)) {
      //assert(0);
      SetDBTreeError(tree,MORPHDB_NOT_FOUND);
      return False;
   }
   ptr = PTR_DATA(node,i,TORDER(tree));
   if (!DBTreeRemoveFromNode(tree,node,i,*ptr,&newPtr)) {
      return False;
   }
   while (1) {
      if (DBTreeIsNodeHalfFull(tree,node)) {
         break;
      }
      if (node == tree->root) {
         if (ISLEAF(node)) {
            break;
         }
         if (!NKEYS(node)) {
            // root node is empty should it be freed ?
            // reduce the tree height
            MorphDBIntNode* oldRoot,*newRoot;
            oldRoot = tree->root;
            if (!DBTreeLoadNode(tree,*PTR_DATA(node,0,TORDER(tree)),NULL,&newRoot)) {
               return False;
            }
            DBTreeSetRoot(tree,newRoot);
         }
         break;
      }
      parent = node->parent;
      i = DBTreeNodeBinSearch(tree,parent,key);
      
      if (i) {
         // try to redistribute to left sibling
         ptr = PTR_DATA(parent,i-1,TORDER(tree));
         if (!DBTreeLoadNode(tree,*ptr,parent,&sibling)) {
            return False;
         }
         if (NKEYS(sibling) > DBTreeMinNodes(tree)) {
            if (!DBTreeNodeRedistribute(tree,sibling,node,i-1,True)) {
               return False;
            }
            break;
         }
      }
      if (i!=NKEYS(parent)){
         ptr = PTR_DATA(parent,i+1,TORDER(tree));
         // try to redistribute to right sibling
         if (!DBTreeLoadNode(tree,*ptr,parent,&sibling)) {
            return False;
         }
         if (NKEYS(sibling) > DBTreeMinNodes(tree)) {
            if (!DBTreeNodeRedistribute(tree,sibling,node,i,False)) {
               return False;
            }
            break;
         }
      }
      // could not redistribute to either siblings;
      // merge one sibling
      if (i==NKEYS(parent)) {
         // sibling has only left sibling   
         if (!DBTreeMergeNodes(tree,sibling, node,i-1)) {
            return False;
         }
      } else {
         // sibling is right sibling
         if (!DBTreeMergeNodes(tree,node,sibling,i)) {
            return False;
         }
      }
      node = parent;
   }
   return DBTreeDecCount(tree);
}


uint8_t
DBTreeInsertToNode(MorphDBDiskTree* tree,
                   MorphDBIntNode* node,
                   BTreeKey* key,
                   BTreeValue* val,
                   BlockMgrDiskPtr ptr,
                   uint8_t writeNode,
                   int i) {
   //int i= BTreeFindIndexInNode(node,key);
   //printf("INSERTING NODE %d\n",i);
   BTreeKey src,dst;
   int j=0;
   uint8_t newPtr;
   BlockMgrDiskPtr p;
   BlockMgrDiskPtr* nPtr = NULL;
   
   //printf("Inserting2 %d val %p ptr %p ",i,val,ptr);BTreeKeyPrint(tree,key);printf("\n");

#if DEBUG
   printf("Inserting to node ");
   DBTreeIntNodePrint(tree,node,1);
   printf(" key : ");
   BTreeKeyPrint(tree,key);
   printf("\n");
#endif
   if (i<0) {
      i=DBTreeNodeBinSearch(tree,node,key);
   }

   // we cant do this because ptr table has not been allocated there is no room for ptr
   nPtr = PTR_DATA(node,i,TORDER(tree));
   if (ISLEAF(node)) {
      // Allocate new data block and copy the value 
      //node->ptrs[i] = malloc(sizeof(BTreeValue));
      //memcpy(node->ptrs[i],val,sizeof(BTreeValue));
      /*if (!MorphDBAllocateDataBytes(tree->handle,val->len,&node->ptrs[i])) {
         assert(0);
         return False;
      }*/
      //memset(&node->ptrs[i],0,sizeof(node->ptrs[i]));
      memset(&p,0,sizeof(BlockMgrDiskPtr));
      if (!BlockMgrDataWrite(tree->handle,val,&p,&newPtr)) {
         BErrToMErr(tree);
         assert(0);
         return False;
      }
   } else {
      assert(_O(&ptr));
      // Allocate from internal cluster
      //node->ptrs[i] = *nPtr;
      p = ptr;
   }
   //assert(node->desc->totalLength);
   //assert(node->desc->totalLength < 1000);
   if (!MorphDBIntNodeInsertKey(tree,node,*key,i,p)) {
      assert(0);
      return False;
   }
   /*MorphDBIntNodePrint(tree,node);
   DBTreeIntNodePrint(tree,node,True);   */
   //NKEYS(node)++;
   if (writeNode) {
#if DEBUG
   printf("AFTER WRITE Inserting to node ");
   DBTreeIntNodePrint(tree,node,1);
   printf("\n");
#endif
      return MorphDBIntNodeWrite(tree,node,&newPtr);
   }
   //DBTreeIntNodePrint(tree,node,True);   
   //BTreeNodePrint(tree,node,1);
#if DEBUG
   printf("AFTER Inserting to node ");
   DBTreeIntNodePrint(tree,node,1);
   printf("\n");
#endif
   return True;
}

uint8_t
DBTreeInsertToIntNode(MorphDBDiskTree *tree,
                      MorphDBIntNode* node, 
                      MorphDBIntNode* oldNode, 
                      MorphDBIntNode* newNode,
                      uint8_t writeParent) {
   int i=0,j=-1;
   BTreeKey key;
   //printf("Before IntNode:"); BTreeNodePrint(node,1);
   uint32_t tOrder = TORDER(tree);
   BlockMgrDiskPtr *ptr;
   uint8_t newPtr;
   assert(node->desc->totalLength);
   assert(oldNode->desc->totalLength);
   assert(newNode->desc->totalLength);
   while (i<=NKEYS(node) ){
      ptr = PTR_DATA(node,i,tOrder);
      if (_O(ptr) == oldNode->nodeOffset.offset) {
         break;
      }
      i++;
   }
   /*if (!MorphDBIntNodeWrite(tree,newNode,&newPtr)) {
      return False;
   }*/
   assert(_O(&newNode->nodeOffset)); 
   *ptr = newNode->nodeOffset;
   //node->ptrs[i] = newNode->nodeOffset;

   assert(oldNode->desc->totalLength);
   assert(node->desc->totalLength);
   assert(newNode->desc->totalLength);
   KEY_TO_BDATA(tree,oldNode,NKEYS(oldNode)-1,&key);
   
   assert(oldNode->desc->totalLength);
   assert(newNode->desc->totalLength);
   //printf("InsertToInt parentWrite %d \n",writeParent);
   DBTreeInsertToNode(tree,node,&key,NULL,oldNode->nodeOffset,writeParent,-1);
   if (!ISLEAF(oldNode)) {
      //BTreeKeyFree(tree,&oldNode->keys[oldNode->numKeys-1]);
      NKEYS(oldNode)--;
      //oldNode->numKeys--;
   }

   // XXX if new ptr is set replace the old ptr in parent
   if (!MorphDBIntNodeWrite(tree,oldNode,&newPtr)) {
      return False;
   }
   // XXX if new ptr is set replace the old ptr in parent
   /*printf("Int node split ");
   DBTreeIntNodePrint(tree,oldNode,True);
   DBTreeIntNodePrint(tree,newNode,True);
   printf("\n");*/
   // write both old node and new node to disk
   //printf("After IntNode:"); BTreeNodePrint(node,1); printf("\n");
   return True;
}

static inline uint8_t
MorphDBCopyNodeKeys(MorphDBDiskTree* tree,
                    MorphDBIntNode* src,
                    MorphDBIntNode* dst,
                    int start,int end) {
   void* tmp;
   MorphDBIntNode* node = dst;
   uint32_t expandLen = 0;
   uint32_t curLength = node->desc->totalLength; 
   int i;
   uint32_t tLength = curLength;
   uint32_t tOrder = TORDER(tree);

   if (src == dst) {
      if (!MorphDBIntNodeInit(tree,src->parent,&node)){
         SetDBTreeError(tree,MORPHDB_NO_MEMORY);
         return False;         
      }
      curLength = node->desc->totalLength;
   }
   for (i=start;i<end;i++) {
      expandLen += KEY_LEN(src,i,tOrder);
   }
   //printf("ExpandLength %u \n",expandLen);

   tmp = realloc(node->desc,curLength+expandLen);
   if (!tmp) {
      SetDBTreeError(tree,MORPHDB_NO_MEMORY);
      MorphDBIntNodeFree(node);
      return False;
   }
   if (tmp!=node->desc) {
      node->desc = tmp;
      node->keyPtrs = node->desc->keyPtrs;
      node->ptrPtrs = node->desc->ptrPtrs;
      node->buf = node->desc;
      node->keyTable = (uint32_t*)(node->buf + node->desc->keyPtrs[0]);
      node->ptrTable = (uint32_t*)(node->buf + node->desc->ptrPtrs[0]);
      
   }
   for (i=0;i<end-start;i++){
      uint32_t keyLen = KEY_LEN(src,i+start,tOrder);
      //printf("keyLen %lu\n",keyLen);
      KEY_OFFSET(node,i,tOrder) = curLength;
      KEY_LEN(node,i,tOrder) = keyLen;
      memcpy(node->buf+curLength,KEY_DATA(src,i+start,tOrder),keyLen);
      *PTR_DATA(node,i,tOrder) = *PTR_DATA(src,i+start,tOrder);
      assert(curLength<= (tLength+expandLen));
      curLength+=keyLen;
   }
   if (!ISLEAF(src)&& !start) {
      // copy the last pointer
      // There are NKEYS and one pointer.
      *PTR_DATA(node,i,tOrder) = *PTR_DATA(src,i+start,tOrder);
   }
   NKEYS(node) = i;
   node->desc->totalLength = curLength;
   if (dst != node) {

      tmp= dst->desc;
      
      dst->desc = node->desc;
      node->desc = tmp;
      dst->desc->isLeaf = node->desc->isLeaf;
      dst->keyPtrs = dst->desc->keyPtrs;
      dst->ptrPtrs = dst->desc->ptrPtrs;
      dst->buf = dst->desc;
      dst->keyTable = (uint32_t*)(dst->buf + dst->desc->keyPtrs[0]);
      dst->ptrTable = (uint32_t*)(dst->buf + dst->desc->ptrPtrs[0]);
      MorphDBIntNodeFree(node);
   }
   assert(DBTreeIsNodeHalfFull(tree,dst));
   return True;
}

static uint8_t
DBTreeSplitNode(MorphDBDiskTree* tree,MorphDBIntNode* node,MorphDBIntNode** intNode) {
   BTreeKey src,dst;
   int i=0,j;
   MorphDBIntNode* newNode;
   BlockMgrDiskPtr ptr,*ePtr; 
   uint8_t newPtr;
   uint32_t tOrder = TORDER(tree);
   void* tmp;

   if (!MorphDBIntNodeInit(tree,node->parent,&newNode)) {
      return False;
   }

   /*if (!newNode) {
      return False;
   }*/
   ISLEAF(newNode) = ISLEAF(node);
   //newNode->isLeaf = node->isLeaf;
   //printf("Before split node    : "); BTreeNodePrint(node,1); printf("\n");
#if DEBUG
   printf("Before split node    : "); DBTreeIntNodePrint(tree,node,1); printf("\n");
#endif
   
#if 0
   for (i=CEIL(NKEYS(node),2),j=0;i<NKEYS(node);i++,j++) {
      //newNode->keys[i] = node->keys[i];
      ePtr = PTR_DATA(node,i,TORDER(tree));
      //KEY_TO_BDATA1(tree,newNode,j,&dst);
      KEY_TO_BDATA(tree,node,i,&src);
      if (!MorphDBIntNodeInsertKey(tree,newNode,src,j,*ePtr)){
         assert(0);
         return False;
      }
      //BTreeKeyCopy(tree,&dst,&src,False);
      //memcpy(&newNode->keys[j],&node->keys[i],sizeof(node->keys[i]));

      //newNode->ptrs[j] = node->ptrs[i];

   }
#else
   if (!MorphDBCopyNodeKeys(tree,node,newNode,CEIL(NKEYS(node),2),NKEYS(node))) {
      return False;
   }

#endif
   if (!ISLEAF(newNode)) {
      ePtr = PTR_DATA(node,NKEYS(node),TORDER(tree));
      assert(_O(ePtr));
      //printf("JJJ %d\n",j);
      if (!MorphDBIntNodeSetPtr(tree,newNode,NKEYS(node)- (CEIL(NKEYS(node),2)),*ePtr)) {
         return False;
      }
      //DBTreeIntNodePrint(tree,newNode,True);
      //newNode->ptrs[j]=node->ptrs[i];
      //newNode->ptrs[j]->parent = newNode;
   } else {
      LNEXT(newNode) = LNEXT(node);
   }

   if (!MorphDBCopyNodeKeys(tree,node,node,0,CEIL(NKEYS(node),2))) {
      return False;
   }
   assert(ISLEAF(node) == ISLEAF(newNode));
   if (!MorphDBIntNodeWrite(tree,newNode,&newPtr)){
      return False;
   }
   if (ISLEAF(node)){
      LNEXT(node) = newNode->nodeOffset;
   }
   if (!MorphDBIntNodeWrite(tree,node,&newPtr)){
      return False;
   }
   //NKEYS(newNode) = j;

   //NKEYS(node) = CEIL(NKEYS(node),2);
   *intNode = newNode;
#if DEBUG
   printf("%d After split ",node->desc->totalLength);
   DBTreeIntNodePrint(tree,node,True);
   DBTreeIntNodePrint(tree,newNode,True);
   printf("\n");
#endif
   return True;
}

MorphDBError MorphDBErrorFromBMgrError(BlockMgrError error) {
   switch (error) {
      case BLOCKMGR_OK:
         return MORPHDB_OK;
      case BLOCKMGR_IO_ERR:
         return MORPHDB_IO_ERR;
      case BLOCKMGR_FILE_NOT_FOUND:
         return MORPHDB_FILE_NOT_FOUND;
      case BLOCKMGR_INVALID_MAGIC:
         return MORPHDB_INVALID_MAGIC;
      case BLOCKMGR_BLOCK_TOO_BIG:
         return MORPHDB_VALUE_TOO_BIG;
      case BLOCKMGR_INVALID_BLOCK_SIZE:
         return MORPHDB_INVALID_VALUE;
      case BLOCKMGR_NO_MEMORY:
         return MORPHDB_NO_MEMORY;

      default:
         return MORPHDB_OTHER_ERR;
   } 
   return MORPHDB_OTHER_ERR;
}

static uint8_t
DBTreeInsert(MorphDBDiskTree* tree,BTreeKey * key, BTreeValue *value,uint8_t insert,uint8_t set) {
   BlockMgrDiskPtr nullPtr;
   MorphDBIntNode* node = NULL;
   int i ;
   uint8_t newPtr;
   MorphDBIntNode* parent,*newNode;

   if (!DBTreeFindLeaf(tree,key,&node)) {
      // other error
      return False;
   }
   if (DBTreeNodeBinSearchEq(tree,node,key,&i)) {
      // it is found set
      if (!set) {
         SetDBTreeError(tree,MORPHDB_NOT_FOUND);   
         return False;
      }
      if (!BlockMgrDataWrite(tree->handle,value,PTR_DATA(node,i,TORDER(tree)),&newPtr)) {
         BErrToMErr(tree);
         return False;
      }
      if (newPtr) {
         return MorphDBIntNodeWrite(tree,node,&newPtr);
      }
      return True;
   } else {
      if (!insert) {
         // entry exists 
         SetDBTreeError(tree,MORPHDB_ENTRY_EXISTS);   
         return False;
      }
   }
   memset(&nullPtr,0,sizeof(nullPtr));
   if (NKEYS(node) < TORDER(tree)-1) {
      if (!DBTreeInsertToNode(tree,node,key,value,nullPtr,True,i)) {
         return False;
      }
      //return True;
      return DBTreeIncCount(tree);
   }
   // split node
   DBTreeInsertToNode(tree,node,key,value,nullPtr,False,i);
   //int i = BTreeFindIndexInNode(node,key);
   if (!DBTreeSplitNode(tree,node,&newNode)) {
      return False;
   }
   parent = node->parent;

   while (1) {
      //printf("parent %p , node %p newNode %p \n",parent,node,newNode);
      if (!parent) {
         if (!MorphDBIntNodeInit(tree,NULL,&parent)) {
            return False;
         }
         //tree->root = parent;
         ISLEAF(parent) = False;

         DBTreeInsertToIntNode(tree,parent,node,newNode,True);
         DBTreeSetRoot(tree,parent);
         if (!MorphDBIntNodeSetPtr(tree,parent,NKEYS(parent),newNode->nodeOffset)) {
            return False;
         }
         //parent->ptrs[NKEYS(parent)] = newNode->nodeOffset;
         parent->parent = NULL;
         node->parent = parent;
         newNode->parent = parent;
         //tree->root = parent;
         break;
      }

      if (DBTreeIsNodeFull(tree,parent)){
         // split parent    

         DBTreeInsertToIntNode(tree,parent,node,newNode,False);
         if (!DBTreeSplitNode(tree,parent,&newNode)) {
            return False;
         }
         node = parent;   
         //newNode->numKeys--;
         parent = newNode->parent;
         parent = node->parent;
      } else {
         DBTreeInsertToIntNode(tree,parent,node,newNode,True);
         break;
      }
   }

   return DBTreeIncCount(tree);
   //return True;
}

static uint8_t
DBTreeFind(MorphDBDiskTree* tree,BTreeKey* key,BTreeValue* outVal){
   MorphDBIntNode* node = NULL;
   int i;
   if (!DBTreeFindLeaf(tree,key,&node)) {
      return False;
   }
   if (!node) {
      return False;
   }
#if DEBUG
   printf("TreeFind Leaf ");
   DBTreeIntNodePrint(tree,node,True);
   printf("\n");
#endif
   if (!DBTreeNodeBinSearchEq(tree,node,key,&i)) {
      return False;
   }
   return BlockMgrDataRead(tree->handle,*PTR_DATA(node,i,TORDER(tree)),outVal); 
}

static uint8_t 
MorphDBIntNodeCopy(BlockMgrHandlePtr dbHandle,MorphDBIntNode* src,
                   MorphDBIntNode** outNode) {
   MorphDBIntNode *node = NULL;

   node = malloc(sizeof(MorphDBIntNode));
   if (!node) {
      SetDBTreeError(error,MORPHDB_NO_MEMORY);
      return False;
   }
   node->desc = malloc(src->desc->totalLength);
   if (!node->desc) {
      SetDBTreeError(error,MORPHDB_NO_MEMORY);
      return False;
   }
   
   memcpy(node->desc,src->buf,src->desc->totalLength);
   node->parent = NULL;
   node->nodeOffset = src->nodeOffset;
   node->buf = node->desc;
   node->keyTable = (uint32_t*)(node->buf + node->desc->keyPtrs[0]);
   node->ptrTable = (uint32_t*)(node->buf + node->desc->ptrPtrs[0]);
#if 0
   node = malloc(sizeof(MorphDBIntNode));
   if (!node) {
      return False;
   }
   node->diskNode = malloc(TINTNODE_SIZE(dbHandle));
   if (!node->diskNode) {
      free(node);
      return False;
   }
   memcpy(node->diskNode,src->diskNode,TINTNODE_SIZE(dbHandle));
   node->dNode = (MorphDBINode*) node->diskNode;
   node->parent = NULL;
   node->keys = (uint8_t*)(node->diskNode+sizeof(MorphDBINode)); 
   node->ptrs = (BlockMgrDiskPtr*)(node->keys+TKSIZE(dbHandle)*TORDER(dbHandle));
   node->nodeOffset = src->nodeOffset;
#endif
   *outNode = node;
   return True;
}

static uint8_t 
MorphDBBTreeCursorSetToStart(MorphDBBTreeCursor* cursor) {
   return MorphDBBTreeCursorSetPos(cursor,cursor->startKey);
}

static uint8_t 
MorphDBBTreeCursorSetPosInt(MorphDBBTreeCursor* cursor,BTreeKey key,uint8_t *curKeyChanged) {
   BlockMgrHandlePtr dbHandle = cursor->db;
   BTreeKey cur;
   BTreeKey newKey;
   MorphDBIntNode* node = NULL;

   if (key.data == NULL) {

      cursor->curIdx = 0;
      node = DBTreeGetRoot(dbHandle);
      while (!ISLEAF(node)) {
         if (!DBTreeLoadNode(dbHandle,*PTR_DATA(node,0,TORDER(dbHandle)),node,&node)) {
            MorphDBTreeSetError(tMorphDBBTreeError);
            return False;
         }
      }
   } else {
      if (!DBTreeFindLeaf(dbHandle,&key,&node)){
         MorphDBTreeSetError(tMorphDBBTreeError);
         return False;
      }
      cursor->curIdx = DBTreeNodeBinSearch(dbHandle,node,&key);
   }
   
   KEY_TO_BDATA(dbHandle,node,cursor->curIdx,&cur);
   if (!BTreeKeyCopy(dbHandle,&newKey,&cur,True)) {
      MorphDBTreeSetError(MORPHDB_NO_MEMORY);
      return False;
   }
   if (curKeyChanged) {
      if (BTreeKeyCmp(dbHandle,&cursor->curKey,&newKey)){
         *curKeyChanged = True;
      } else {
         *curKeyChanged = False;
      }
   }
   BinaryStrFree(&cursor->curKey);
   cursor->curKey = newKey;
   if (cursor->curNode) {
      MorphDBIntNodeFree(cursor->curNode);
   }
   if (!MorphDBIntNodeCopy(dbHandle,node,&cursor->curNode)) {
      MorphDBTreeSetError(MORPHDB_NO_MEMORY);
      return False;
   }
   return True;
}

uint8_t 
MorphDBBTreeCursorSetPos(MorphDBBTreeCursor* cursor,BTreeKey key) {
   return MorphDBBTreeCursorSetPosInt(cursor,key,NULL);
}

uint8_t 
MorphDBBTreeCursorInit(MorphDBDiskTree* db,MorphDBBTreeCursor** cursor) {
   MorphDBBTreeCursor* cur;

   cur = malloc(sizeof(MorphDBBTreeCursor));
   if (!cur) {
      SetDBTreeError(tree,MORPHDB_NO_MEMORY);
      return False;
   }
   memset(cur,0,sizeof(*cur));
   cur->startKey.data = NULL;
   cur->startKey.len = 0;

   cur->endKey.data = NULL;
   cur->endKey.len = 0;

   cur->db = db;
   cur->gen = db->gen;
   if (!MorphDBBTreeCursorSetToStart(cur)) {
      return False;
   }
   *cursor = cur;
   return True;
}

uint8_t 
MorphDBBTreeCursorSetStartKey(MorphDBBTreeCursor* cursor,BTreeKey key) {
   BinaryStrFree(&cursor->startKey);
   if (BinaryStrDup(&cursor->startKey,&key)) {
      MorphDBTreeSetError(MORPHDB_NO_MEMORY);
      return False;
   }
   if (!MorphDBBTreeCursorSetToStart(cursor)) {
      return False;
   }
   return True;
}

uint8_t 
MorphDBBTreeCursorSetEndKey(MorphDBBTreeCursor* cursor,BTreeKey key) {
   BinaryStrFree(&cursor->endKey);
   if (BinaryStrDup(&cursor->endKey,&key)) {
      MorphDBTreeSetError(MORPHDB_NO_MEMORY);
      return False;
   }
   return True;
}

uint8_t 
MorphDBBTreeCursorValue(MorphDBBTreeCursor* cursor,
                   BTreeKey* key,
                   BTreeValue* value) {
   BTreeKey cur;

   if (cursor->gen != cursor->db->gen) {
      if (!MorphDBBTreeCursorSetPos(cursor,cursor->curKey)) {
         return False;
      }
      cursor->gen = cursor->db->gen;
   }
   if (cursor->curIdx >= NKEYS(cursor->curNode)) {
      // no more data
      MorphDBTreeSetError(MORPHDB_CURSOR_END);
      return False;
   }
   KEY_TO_BDATA(cursor->db,cursor->curNode,cursor->curIdx,&cur);
   if (!BTreeKeyCopy(cursor->db,key,&cursor->curKey,True)) {
      MorphDBTreeSetError(MORPHDB_NO_MEMORY);
      return False;
   }

   if (!BlockMgrDataRead(cursor->db->handle,
                        *PTR_DATA(cursor->curNode,cursor->curIdx,TORDER(cursor->db)),value)) {
      BErrToMErr(cursor->db);
      return False;
   }
   return True;
}

uint8_t 
MorphDBBTreeCursorNext(MorphDBBTreeCursor* cursor) {
   uint8_t curKeyChanged=  False;
   BlockMgrHandlePtr db = cursor->db;
   MorphDBIntNode* node;
   BTreeKey cur;

   if (cursor->gen != db->gen) {
      if (!MorphDBBTreeCursorSetPosInt(cursor,cursor->curKey,&curKeyChanged)) {
         return False;
      }
      cursor->gen = db->gen;
   }
   if (curKeyChanged) {
      if (cursor->curIdx >= NKEYS(cursor->curNode)) {
         // no more data
         MorphDBTreeSetError(MORPHDB_CURSOR_END);
         return False;
      }
      // already moved to next position
      return True;
   }
   if ((cursor->curIdx >= (NKEYS(cursor->curNode)-1))) {
      if (BlockMgrIsNullPtr(&LNEXT(cursor->curNode))) {
         MorphDBTreeSetError(MORPHDB_NO_MEMORY);
         return False;
      }
      if (!DBTreeLoadNode(db,LNEXT(cursor->curNode),NULL,&node)) {
         MorphDBTreeSetError(tMorphDBBTreeError);
         return False;
      }
      // XXX 
      MorphDBIntNodeFree(cursor->curNode);
      if (!MorphDBIntNodeCopy(db,node,&cursor->curNode)) {
         MorphDBTreeSetError(MORPHDB_NO_MEMORY);
         return False;
      }
      //cursor->curNode = node;
      cursor->curIdx = 0;
   } else {
      cursor->curIdx++;
   }

   KEY_TO_BDATA(db,cursor->curNode,cursor->curIdx,&cur);
   
   if (cursor->endKey.data && BTreeKeyCmp(db,&cur,&cursor->endKey)>0) {
      MorphDBTreeSetError(MORPHDB_CURSOR_END);
      return False;
   }
   BinaryStrFree(&cursor->curKey);
   if (!BTreeKeyCopy(cursor->db,&cursor->curKey,&cur,True)) {
      MorphDBTreeSetError(MORPHDB_NO_MEMORY);
      return False;
   }
   return True;
}

static inline uint8_t 
MorphDBIntNodeSetPtr(BlockMgrHandlePtr tree,
                     MorphDBIntNode *node,
                     int pos,
                     BlockMgrDiskPtr ptr){
   uint32_t expandLen = 0;
   uint32_t curLength = node->desc->totalLength;
   uint32_t tOrder = TORDER(tree);
   void* tmp = NULL;

   assert(ptr.offset);
   if (!PTR_PTR(node,pos,tOrder)) {
      expandLen += PTR_SIZE(tOrder);
   
      tmp = realloc(node->desc,curLength+expandLen);
      if (!tmp) {
         SetDBTreeError(tree,MORPHDB_NO_MEMORY);
         return False;
      }
      if (tmp!=node->desc) {
         node->desc = tmp;
         node->keyPtrs = node->desc->keyPtrs;
         node->ptrPtrs = node->desc->ptrPtrs;
         node->buf = node->desc;
      }

      if (!PTR_PTR(node,pos,tOrder)) {
         PTR_PTR(node,pos,tOrder) = curLength;
         curLength += PTR_SIZE(tOrder);
      }
   }
   *(PTR_DATA(node,pos,tOrder)) = ptr;
   node->desc->totalLength = curLength;
   node->desc->actualDataLength = curLength;
   return True;
}

static uint8_t 
MorphDBIntNodeInsertKey(BlockMgrHandlePtr tree,
                        MorphDBIntNode *node,
                        BTreeKey key,
                        int pos,BlockMgrDiskPtr ptr){
   uint32_t expandLen = key.len;
   uint32_t nKeys = NKEYS(node);
   uint32_t curLength = node->desc->totalLength;
   void *tmp;
   uint32_t tOrder = TORDER(tree);
   int i;
   int j,k;

   //assert(ptr.offset);
   //printf("INSERTING KEY AT %lu\n",pos);
   //assert(curLength < 1000);
   assert(pos <= tOrder);
   if (NKEYS(node) == tOrder) {
      return False;
   }    

   if (!KEY_PTR(node,pos,tOrder)) {
      expandLen += KEY_PTR_SIZE(tOrder);
   }
   if (!PTR_PTR(node,pos,tOrder)) {
      expandLen += PTR_SIZE(tOrder);
   }

   if ((NKEYS(node) != pos) && !KEY_PTR(node,NKEYS(node),tOrder)) {
      expandLen += KEY_PTR_SIZE(tOrder);
   }

   if ((NKEYS(node)+1 != pos) && !PTR_PTR(node,NKEYS(node)+1,tOrder)) {
      expandLen += PTR_SIZE(tOrder);
   }
   tmp = realloc(node->desc,curLength+expandLen);
   if (!tmp) {
      SetDBTreeError(tree,MORPHDB_NO_MEMORY);
      return False;
   }
   if (tmp!=node->desc) {
      node->desc = tmp;
      node->keyPtrs = node->desc->keyPtrs;
      node->ptrPtrs = node->desc->ptrPtrs;
      node->buf = node->desc;
      node->keyTable = (uint32_t*)(node->buf + node->desc->keyPtrs[0]);
      node->ptrTable = (uint32_t*)(node->buf + node->desc->ptrPtrs[0]);
      
   }
   if (!KEY_PTR(node,pos,tOrder)) {
      KEY_PTR(node,pos,tOrder) = curLength;
      curLength += KEY_PTR_SIZE(tOrder);  
   }

   if ((NKEYS(node) != pos) && !KEY_PTR(node,NKEYS(node),tOrder)) {
      KEY_PTR(node,NKEYS(node),tOrder) = curLength;
      curLength += KEY_PTR_SIZE(tOrder);  
   }

   if (!PTR_PTR(node,pos,tOrder)) {
      PTR_PTR(node,pos,tOrder) = curLength;
      //printf("PTR_SIZE is allocated at %lu\n",curLength);
      curLength += PTR_SIZE(tOrder);
   }
   if ((NKEYS(node)+1 != pos) && !PTR_PTR(node,NKEYS(node)+1,tOrder)) {
      PTR_PTR(node,NKEYS(node)+1,tOrder) = curLength;
      //printf("PTR_SIZE is allocated at %lu\n",curLength);
      curLength += PTR_SIZE(tOrder);
   }

   //printf("PTR offset %lu : %lu \n",PTR_OFFSET(node,NKEYS(node)+1,tOrder),curLength+sizeof(BlockMgrDiskPtr)*((pos%PTR_PTR_LEN(tOrder)))); 
   *(PTR_DATA(node,NKEYS(node)+1,tOrder)) = *(PTR_DATA(node,NKEYS(node),tOrder));
   for (i=NKEYS(node);i>pos;i--) {
      //printf("Key offset %d , %u,%u\n",i,KEY_OFFSET(node,i,tOrder),KEY_OFFSET(node,i-1,tOrder));
      //assert(KEY_OFFSET(node,i-1,tOrder));
      KEY_OFFSET(node,i,tOrder) = KEY_OFFSET(node,i-1,tOrder);
      KEY_LEN(node,i,tOrder) = KEY_LEN(node,i-1,tOrder);
      //assert(!ISLEAF(node));
      *(PTR_DATA(node,i,tOrder)) = *(PTR_DATA(node,i-1,tOrder));
   } 
   KEY_OFFSET(node,pos,tOrder) = curLength;
   KEY_LEN(node,pos,tOrder) = key.len;
   *(PTR_DATA(node,pos,tOrder)) = ptr; 
   memcpy(node->buf+curLength,key.data,key.len);
   curLength += key.len;
   node->desc->totalLength = curLength;
   node->desc->actualDataLength += expandLen;
   NKEYS(node)++;
   return True;
}

void
MorphDBIntNodePrint(BlockMgrHandlePtr tree,MorphDBIntNode* node) {
   int i,j;
   BTreeKey key;
   
   printf("NKEYS %d \n",NKEYS(node));
   for(i=0;i<4;i++) {
      //printf("\t keyptr %u, ptr_ptr %u\n",node->keyPtrs[i],node->ptrPtrs[i]);
      for (j=0;j<KEY_PTR_LEN(TORDER(tree));j++) {
      }
   }
   for (i=0;i<NKEYS(node);i++) {
      KEY_TO_BDATA(tree,node,i,&key);
      printf("%d ",i);
      BTreeKeyPrint(tree,&key);
      printf(" ptr %lu ",_O(PTR_DATA(node,i,TORDER(tree))));
      printf("\n");
   }
}

void
DBIntNodeTest(char* fname) {
   int rv;
   MorphDBDiskTree *tree;
   BlockMgrDiskPtr ptr;
   BTreeKeyOps intOps;
   int i;
   int count=12;
   BTreeKey key,value,k,v,val;
   MorphDBIntNode* node;
   uint8_t newPtr;

   rv = MorphDBInitTreeHandle(fname,33,0,&tree);
   if (rv) {
      printf("Initializing tree failed\n");
      return;
   }
   intOps.cmp = BTreeKeyIntCmp;
   //intOps.free = BTreeKeyBStrFree;
   intOps.copy = BTreeKeyBStrCopy;
   intOps.print = BTreeKeyIntPrint;
   tree->keyOps = intOps;
   key.data = (void*)&i;
   key.len = sizeof(i);
   i = 1;
   _SO(&ptr,1);
   assert(MorphDBIntNodeInit(tree,NULL,&node));
   
   assert(MorphDBIntNodeInsertKey(tree,node,key,0,ptr));
   assert(MorphDBIntNodeWrite(tree,node,&newPtr)); 
   assert(BlockMgrDataRead(tree->handle,node->nodeOffset,&val));
   v.data = (void*)node->desc;
   v.len = node->desc->totalLength;
   assert(BStrEqual(&v,&val));
   MorphDBIntNodePrint(tree,node);
   printf("Adding second\n");
   i = 2;
   _SO(&ptr,2);
   assert(MorphDBIntNodeInsertKey(tree,node,key,0,ptr));
   assert(MorphDBIntNodeWrite(tree,node,&newPtr)); 
   assert(BlockMgrDataRead(tree->handle,node->nodeOffset,&val));
   v.data = (void*)node->desc;
   v.len = node->desc->totalLength;
   assert(BStrEqual(&v,&val));
   MorphDBIntNodePrint(tree,node);
   
   i = 3;
   _SO(&ptr,3);
   printf("Adding %d \n",i);
   assert(MorphDBIntNodeInsertKey(tree,node,key,0,ptr));
   assert(BlockMgrDataRead(tree->handle,node->nodeOffset,&val));
   v.data = (void*)node->desc;
   v.len = node->desc->totalLength;
   assert(BStrEqual(&v,&val));
   MorphDBIntNodePrint(tree,node);
   i = 4;
   _SO(&ptr,4);
   printf("Adding %d \n",i);
   assert(!MorphDBIntNodeInsertKey(tree,node,key,3,ptr));
   MorphDBIntNodePrint(tree,node);
}

void
MorphDBBTreeCursorFree(MorphDBBTreeCursor* cursor) {
   BinaryStrFree(&cursor->startKey);
   BinaryStrFree(&cursor->endKey);
   BinaryStrFree(&cursor->curKey);
   if (cursor->curNode) {
      MorphDBIntNodeFree(cursor->curNode);   
   }
   free(cursor);
}

uint8_t 
MorphDBBTreeSet(void* beData,BinaryStr key,BinaryStr value,uint8_t insert) {
   uint8_t rv = DBTreeInsert(beData,&key,&value,insert,True);
   //DBTreePrint(beData);
   //printf("PRINT ENDED \n");
   //fflush(stdout);
   //DBTreeCheck(beData);
   return rv;
}

uint8_t 
MorphDBBTreeInsert(void* beData,BinaryStr key,BinaryStr value) {
   return DBTreeInsert(beData,&key,&value,True,False);
}

uint8_t
MorphDBBTreeGet(void* beData,BTreeKey key,BTreeValue *value) {
   return DBTreeFind(beData,&key,value);
}

uint8_t MorphDBBTreeDelete(void* beData,BTreeKey key) {
   uint8_t rv = DBTreeDelete(beData,&key);
   //DBTreePrint(beData);
   //DBTreeCheck(beData);
   return rv;
}

void MorphDBBTreeClose(void* beData) {
   MorphDBTreeFree(beData);
}

uint8_t MorphDBBTreeSync(void* beData) {
   MorphDBDiskTree* tree=beData;
   uint8_t rv ;
   if (!DBTreeSetCount(tree)) {
      return False;
   }
   rv = BlockMgrDataSync(tree->handle);
   BErrToMErr(tree);
   return rv;
}

MorphDBError MorphDBBTreeGetLastError() {
   return tMorphDBBTreeError;
}

MorphDBError MorphDBBTreeGetLastMorphDBBTreeError2(void* handle) {
   return MorphDBBTreeGetLastError();
}

void MorphDBBTreeRemove(void* beData) {
   MorphDBTreeRemove(beData);
}

uint64_t MorphDBBTreeCount(void* beData) {
   return ((MorphDBDiskTree*)(beData))->count;
}

// cursor ops
uint8_t MorphDBBTreeWCursorSetEndKey(void* cursor,BTreeKey key) {
   return MorphDBBTreeCursorSetEndKey(cursor,key);
}

uint8_t MorphDBBTreeWCursorSetStartKey(void* cursor,BTreeKey key) {
   return MorphDBBTreeCursorSetStartKey(cursor,key);
}

uint8_t MorphDBBTreeWCursorNext(void* cursor) {
   return MorphDBBTreeCursorNext(cursor);
}

uint8_t MorphDBBTreeWCursorValue(void* cursor,BTreeKey* key,BTreeValue* value) {
   return MorphDBBTreeCursorValue(cursor,key,value);
}


uint8_t MorphDBBTreeWCursorSetPos(void* cursor,BTreeKey key) {
   return MorphDBBTreeCursorSetPos(cursor,key);
}

uint8_t MorphDBBTreeWCursorSetToStart(void* cursor){
   return MorphDBBTreeCursorSetToStart(cursor);
}

uint8_t MorphDBBTreeWCursorSetToEnd(void* cur) {
   MorphDBBTreeCursor* cursor = cur;
   return MorphDBBTreeCursorSetPos(cursor,cursor->endKey);
}

void MorphDBBTreeWCursorFree(void* cursor) {
   MorphDBBTreeCursorFree(cursor);
}

MorphDBError MorphDBBTreeWCursorGetLastError(void* cursor) {
   return MorphDBBTreeGetLastError();
}

uint8_t 
MorphDBBTreeWCursorInit(void* beData,void** cursor) {
   MorphDBBTreeCursor* cur;
   uint8_t rv;
   rv = MorphDBBTreeCursorInit(beData,&cur);
   if (rv) {
      *cursor = cur;
   } else {
      *cursor = NULL;
   }
   return rv; 
}

DBOps morphDBBTreeDBOps = {
   .set    = MorphDBBTreeSet,
   .insert = MorphDBBTreeInsert,
   .del    = MorphDBBTreeDelete,
   .remove = MorphDBBTreeRemove,
   .close  = MorphDBBTreeClose,
   .sync   = MorphDBBTreeSync,
   .get    = MorphDBBTreeGet,
   .count  = MorphDBBTreeCount,
   .getLastError= MorphDBBTreeGetLastMorphDBBTreeError2,
   .getLastErrorString = NULL,
   .cursorInit       = MorphDBBTreeWCursorInit,
};

DBCursorOps morphDBBTreeCursorOps = {
   .setEndKey        = MorphDBBTreeWCursorSetEndKey ,
   .setStartKey      = MorphDBBTreeWCursorSetStartKey,
   .next             = MorphDBBTreeWCursorNext,
   .prev             = NULL,
   .value            = MorphDBBTreeWCursorValue,
   .setPos           = MorphDBBTreeWCursorSetPos,
   .getLastError     = MorphDBBTreeWCursorGetLastError,
   .getLastErrorString = NULL,
   .setToStart       = MorphDBBTreeWCursorSetToStart,
   .free             = MorphDBBTreeWCursorFree,
    
};

MorphDBError MorphDBBTreeInit(char* name,int tOrder,uint64_t features,MorphDB** outDB){
   MorphDB *db;
   MorphDBDiskTree *tree;
   MorphDBError rv;

   db = malloc(sizeof(*db));
   if (!db) {
      return MORPHDB_NO_MEMORY;
   }
   if ((rv =MorphDBInitTreeHandle(name,tOrder,features,&tree))) {
      return rv;
   }
   db->beData = tree;
   db->dbOps = morphDBBTreeDBOps;
   db->cursorOps = morphDBBTreeCursorOps;
   *outDB= db;
   return MORPHDB_OK;
}

