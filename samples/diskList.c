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
#include  <stdio.h>
#include <assert.h>

#define MAGIC 0xabcdefab

/**
 * @file diskList.c
 *
 * Example usage of BlockMgr.
 */

#pragma pack(1)
/**
 * Super block of the disk list.
 */
typedef struct DiskListHeader {
   uint32_t magic; /*! Magic identifies the file has disk list. */
   BlockMgrDiskPtr head; /*! Head pointer of the list. */
   uint32_t numNodes; /*! Number of nodes in the list. */
} DiskListHeader;

/**
 * Ondisk representation of a list node.
 * On the disk both ptrs and data in the node
 * is stored together. Data follows immediately after
 * the DiskListNode structure.
 */
typedef struct DiskListNode {
   BlockMgrDiskPtr next;
   BlockMgrDiskPtr prev;
}DiskListNode;

#pragma pack()

/**
 * In-memory representation of the on-disk list.
 */
typedef struct DiskList {
   BlockMgrHandle* handle;
   DiskListHeader* header;
}DiskList;

/**
 * In-memory representation of a list node.
 * Ondisk the initial portion has DiskListNode and
 * the value of the node follows that.
 */
typedef struct ListNode {
   DiskListNode *nodePtr;
   BlockMgrDiskPtr curPtr;
   BinaryStr data;
}ListNode;

/**
 * Loads node from disk given the on-disk ptr.
 */
ListNode* ListLoadNode(DiskList* dList,BlockMgrDiskPtr ptr) {
   ListNode* node = malloc(sizeof(ListNode)); 
   BinaryStr val;
   if (!BlockMgrDataRead(dList->handle,ptr,&val)) {
      free(node);
      return NULL;  
   }
   node->nodePtr = (DiskListNode*)val.data;
   assert(node->nodePtr);
   node->data.data = (char*)(node->nodePtr+1);
   node->data.len  = val.len - sizeof(DiskListNode);
   node->curPtr = ptr;
   return node;
}

/**
 * Free the in-memory node.
 */
void ListNodeFree(ListNode* node) {
   free(node->nodePtr);
   free(node);
}

/**
 * Returns the pointer to first node.
 * In an empty list this will return header address.
 *
 */

BlockMgrDiskPtr GetFirst(DiskList* dList) {
   ListNode* node;
   BlockMgrDiskPtr ptr;
   node = ListLoadNode(dList,dList->header->head);
   ptr = node->nodePtr->next;
   ListNodeFree(node);
   return ptr;
}

/**
 * Returns the pointer to last node in the list.
 * In an empty list this will return header address.
 *
 */
BlockMgrDiskPtr GetLast(DiskList* dList) {
   ListNode* node;
   BlockMgrDiskPtr ptr;
   node = ListLoadNode(dList,dList->header->head);
   ptr = node->nodePtr->prev;
   ListNodeFree(node);
   return ptr;
}

/**
 * Writes node to disk also if the node gets new
 * address updates the previous and next nodes
 * to have the right address.
 *
 */
int ListNodeWrite(DiskList* dList,ListNode* node) {
   BinaryStr toVal;
   uint8_t newPtr;
   
   toVal.data = malloc(sizeof(DiskListNode)+node->data.len);
   toVal.len  = sizeof(DiskListNode)+node->data.len;
   assert(toVal.data);   
   memcpy(toVal.data,node->nodePtr,sizeof(*node->nodePtr));
   memcpy(toVal.data+sizeof(*node->nodePtr),node->data.data,node->data.len); 
   if (!BlockMgrDataWrite(dList->handle,&toVal,&node->curPtr,&newPtr)) {
      BinaryStrFree(&toVal);
      return -1;
   }
   BinaryStrFree(&toVal);
   if (newPtr) {
      ListNode* n;
      // update next node and prev node 
      if (!(n = ListLoadNode(dList,node->nodePtr->next))){

         return -1;
      }
      n->nodePtr->prev = node->curPtr;
      ListNodeWrite(dList,n);
      ListNodeFree(n);
      if (!(n = ListLoadNode(dList,node->nodePtr->prev))){

         return -1;
      }
      
      n->nodePtr->next = node->curPtr;
      ListNodeWrite(dList,n);
      ListNodeFree(n);
   }
   return 0;
}

/**
 * Allocate and initialize a ListNode with the given data.
 */

ListNode* ListNodeInit(BinaryStr data) {
   ListNode* node = malloc(sizeof(ListNode));   
   if (!node) {
      return NULL;
   }
   node->nodePtr = malloc(sizeof(DiskListNode)+data.len);
   if (!node->nodePtr) {
      free(node);
      return NULL;
   }
   node->data.data =((char*) (node->nodePtr))+sizeof(DiskListNode);
   node->data.len = data.len;
   memcpy(node->data.data,data.data ,data.len);
   return node;
}

/**
 * Adds a new node after the given node with the given binary data.
 */

int ListAddAfter(DiskList* dList,ListNode *node,BinaryStr data) {
   ListNode* newNode = ListNodeInit(data);
   assert(newNode);
   _SO(&newNode->curPtr,0);
   newNode->nodePtr->prev = node->curPtr; 
   newNode->nodePtr->next = node->nodePtr->next; 
   if (ListNodeWrite(dList,newNode)) {
      ListNodeFree(newNode);
      return -1;
   }
   node->nodePtr->next = node->curPtr;
   ListNodeFree(newNode);
   return 0;
  // node->nodePtr->next   = node->curPtr;
}

/**
 * Removes the given node from the list.
 */
int ListRemove(DiskList* dList,ListNode* node) {
   ListNode* n = ListLoadNode(dList,node->nodePtr->next);
   assert(n);
   n->nodePtr->prev = node->nodePtr->prev;
   if (ListNodeWrite(dList,n)) {
      ListNodeFree(n);
      return -1;
   }
   ListNodeFree(n);
   n = ListLoadNode(dList,node->nodePtr->prev);
   assert(n);
   n->nodePtr->next = node->nodePtr->next;
   if (ListNodeWrite(dList,n)) {
      ListNodeFree(n);
      return -1;
   }
   ListNodeFree(n);
   assert(BlockMgrDataFree(dList->handle,node->curPtr));
   return 0; 
}

/** 
 * Call back function type for list iterator.
 */
typedef int (*ListCbFn) (DiskList *dList,ListNode* node,void* args); 

/**
 * List iterator.
 *
 * Goes through all nodes in the list and calls the callback
 * function for each node.
 * If the call back returns non-zero iteration stops.
 */
int ListForAll(DiskList* dList,ListCbFn fn,void* args) {
   ListNode* node;
   int rv=0;
   node  = ListLoadNode(dList,GetFirst(dList));
   assert(node);
   while (_O(&node->curPtr) != _O(&dList->header->head)) {
      ListNode* tmp = node;
      rv = fn(dList,node,args);
      if (rv) {
         break;
      }
      node  = ListLoadNode(dList,node->nodePtr->next);
      assert(node);
      ListNodeFree(tmp);
   }
   return rv;
}


/**
 * Print a given node.
 */
int ListPrintFn(DiskList* dList,ListNode* node,void* args) {
   printf(" %d ",*(int*)node->data.data);
   return 0;
}

/**
 * Print the list.
 */
void
ListPrint(DiskList* dList) {
   ListForAll(dList,ListPrintFn,NULL);
}


typedef struct ListFindFnArgs {
   BinaryStr val;
   uint8_t found;
   BlockMgrDiskPtr ptr;
}ListFindFnArgs;

/** 
 * Call back function for finding a node.
 */
int ListFindFn(DiskList* dList,ListNode* node, void* args){
   ListFindFnArgs *findArgs = args;
   if (node->data.len != findArgs->val.len) {
      return 0;
   }
   if (memcmp(node->data.data,findArgs->val.data,node->data.len)) {
      return 0;
   }
   // value matches store the ptr
   findArgs->found = 1;
   findArgs->ptr = node->curPtr;
   return -1;
}

/**
 * Find a node which matches the given content and returns
 * the pointer to it if it is found.
 */
int
ListFind(DiskList* dList,BinaryStr val,BlockMgrDiskPtr *ptr) {
   ListFindFnArgs args;
   args.val = val;
   args.found = 0;
   ListForAll(dList,ListFindFn,&args);
   if (args.found) {
      *ptr = args.ptr;
      return 0;
   }
   // not found
   return -1;
}

/**
 * Formats a file into a on-disk list file on top of BlockMgr.
 */
int
ListFormat(char* fileName){
   BlockMgrHandle* handle;
   DiskListHeader header;
   BinaryStr str;
   DiskListNode head;
   uint8_t newPtr;

   if (BlockMgrFormatFile(fileName,0)){
      printf("BlockMgr format failed.\n");
      return -1;
   }
   if (BlockMgrInitHandle(fileName,0,&handle)) {
      printf("BlockMgr init failed.\n");
      return -1;
   }
   header.magic = MAGIC;
   _SO(&header.head,0);
   header.numNodes = 0;

   BlockMgrDiskPtr ptr;
   _SO(&head.next,0);
   _SO(&head.prev,0);
   // indicate this is new dat
   _SO(&ptr,0);
   str.data= (char*) &head;
   str.len = sizeof(head);
   BlockMgrDataWrite(handle,&str,&ptr,&newPtr) ;
   // new data has to be allocated
   assert(newPtr);
   // Circular linked list
   head.next = ptr;
   head.prev = ptr;

   BlockMgrDataWrite(handle,&str,&ptr,&newPtr) ;
   // should write to the same location
   assert(!newPtr);
   header.head = ptr;
   str.data = (char*)&header;
   str.len  = sizeof(header);
   // Set our super block
   assert(BlockMgrSetUserData(handle,&str));
   return 0;
}

/**
 * Initializes the in-memory list handle given filename.
 */
DiskList* ListInit(char* fileName) {
   DiskList* dList = malloc(sizeof(*dList));
   BinaryStr val;

   assert(dList);
   if (BlockMgrInitHandle(fileName,0,&dList->handle)) {
      ListFormat(fileName);
      if (BlockMgrInitHandle(fileName,0,&dList->handle)) {
         free(dList);
         return NULL;
      }
   }
   assert(BlockMgrReadUserData(dList->handle,&val));
   // verify whether header is valid
   assert(val.len == sizeof(DiskListHeader));
   dList->header = (DiskListHeader*) val.data;
   assert(dList->header->magic == MAGIC);
   return dList;
}

/**
 * CLI for various operations on list.
 * -  Following are the operations in CLI
 *       1. --> to add an element.
         2. --> to remove an element.
         3. --> to print list.
         4. --> to print this help message.
         5. --> Start transaction.
         6. --> Commit transaction.
         7. --> Abort transaction.
 *
 */

int main(int argc, char** argv) {
   BinaryStr val;
   int i=0;
   ListNode* node,*end;

   if (argc <2) {
      printf("Usage : %s <existing list filename or new filename to store disk list> \n",argv[0]);
      return -1;
   }
   DiskList* dList = ListInit(argv[1]);
   printf("DiskList %p\n",dList);
   assert(dList);
   //Adding to the end

   while(1){
      int opt;
      printf("<list: choose operation [1-4] 4 for help~>");
      scanf("%d",&opt);
      if (opt > 7) {
         printf("Invalid option.\n");
         opt = 4;
      } 
      if (opt == 4) {
         printf("1 --> to add an element.\n");
         printf("2 --> to remove an element.\n");
         printf("3 --> to print list\n");
         printf("4 --> to print this help message\n");
         printf("5 --> Start transaction .\n");
         printf("6 --> Commit transaction.\n");
         printf("7 --> Abort transaction.\n");
         continue;
      }
      if (opt == 1) {
         int fVal,val;
         BinaryStr s;
         BlockMgrDiskPtr ptr;
         printf("Enter element to add after, enter non-existing element to add to end of the list.\n");
         printf(":~");
         scanf("%d",&fVal);
         printf("Enter value to add.\n");
         printf(":~");
         scanf("%d",&val);
         s.data = (char*)&fVal; 
         s.len = sizeof(fVal);
         if (ListFind(dList,s,&ptr)) {
            ptr = GetLast(dList);
         }
         assert((node = ListLoadNode(dList,ptr)));
         s.data = (char*) &val; 
         s.len = sizeof(val);
         ListAddAfter(dList,node,s);

      }
      if (opt ==3) {
         ListPrint(dList);
         printf("\n");
      }
      if (opt == 2) {
         int fVal,val;
         BinaryStr s;
         BlockMgrDiskPtr ptr;
         printf("Enter value to remove.\n");
         printf(":~");
         scanf("%d",&val);
         s.data = (char*)&val; 
         s.len = sizeof(val);
         if (ListFind(dList,s,&ptr)) {
            printf("Value %d does not exists in the list.\n",val);
            continue;
         }
         assert((node = ListLoadNode(dList,ptr)));
         s.data =(char*) &val; 
         s.len = sizeof(val);
         ListRemove(dList,node);
      }
      if (opt == 5) {
         printf("Starting transaction.\n");
         BlockMgrTransactStart(dList->handle);
      }
      if (opt == 6) {
         printf("Committing transaction.\n");
         BlockMgrTransactCommit(dList->handle);
         BlockMgrTransactReplay(dList->handle);
      }

      if (opt == 7) {
         printf("Aborting transaction.\n");
         BlockMgrTransactAbort(dList->handle);
      }
   }
   return 0;
}
