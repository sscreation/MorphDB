
/*
    Copyright (C) 2013 Sridhar Valaguru <sridharnitt@gmail.com>

    This file is part of eXTendDB.

    eXTendDB is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    eXTendDB is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with eXTendDB.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "hash.h"
#include "list.h"
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

struct HashTable {
	HashFn hashFn;
	HashCmpFn cmpFn;
	uint32_t numBuckets;
	ListNode* bkts;
   uint64_t numRes;
   uint64_t numIns;
   uint32_t numNodes;
};

HashTable* 
HTInit(HashFn hashFn,HashCmpFn cmpFn, int numBuckets) {
	int i;
//	numBuckets = 1093;
	HashTable* ht = Malloc(sizeof(HashTable));
	ht->hashFn = hashFn;
	ht->cmpFn = cmpFn;
	ht->numBuckets = numBuckets;
	ht->bkts = Malloc(sizeof(ListNode)*ht->numBuckets);
   ht->numRes = 0;
   ht->numIns = 0;

	if (!ht->bkts) {
		return  NULL;
	}
	for (i=0;i<numBuckets;i++) {
		LIST_INIT(&ht->bkts[i]);
	}
	return ht;
}

int
HTInsert(HashTable* ht, void* toAdd,const void* key ,
		   uint32_t size,void** exist) {
	uint32_t hash = ht->hashFn(key,size);
	//printf("HASH VALUE 0x%x\n",hash);
	ListNode* bkt = &ht->bkts[hash%ht->numBuckets];
	ListNode* tmp = NULL;
	*exist = NULL;

   ht->numIns++;
	LIST_FOR_ALL(bkt,tmp) {
      ht->numRes++;
		if (ht->cmpFn(tmp->data,(void*)key)) {
			*exist = tmp->data;
			return EEXIST;
		}
	}
	tmp = Malloc(sizeof(ListNode));
	if (!tmp) {
		return ENOMEM;
	}
	LIST_INIT(tmp);
	tmp->data = toAdd;

	LIST_ADD_AFTER(tmp,bkt->prev);
   ht->numNodes++;
	return 0;
}

void*
HTFind(HashTable* ht,const void* key,uint32_t size){
	uint32_t hash = ht->hashFn(key,size);
	ListNode* bkt = &ht->bkts[hash%ht->numBuckets];
	ListNode* tmp = NULL;

	LIST_FOR_ALL(bkt,tmp) {
		if (ht->cmpFn(tmp->data,(void*)key)) {
			return tmp->data;
		}
	}
	
	return NULL;
}

void*
HTRemove(HashTable* ht,void* key,uint32_t size) {
	uint32_t hash = ht->hashFn(key,size);
	ListNode* bkt = &ht->bkts[hash%ht->numBuckets];
	ListNode* tmp = NULL;
	ListNode* found = NULL;
	void *data;

	LIST_FOR_ALL(bkt,tmp) {
		if (ht->cmpFn(tmp->data,(void*)key)) {
			found = tmp;
			break;
		}
	}

	if (!found) {
		return found;
	}
	LIST_REMOVE(bkt,found);
	data = found->data;
	free(found);
   ht->numNodes--;
	return data; 
}


void HTDestroy(HashTable* ht,HTDestroyCBFn cb) {
   int i;
   ListNode *bkt;
   ListNode *tmp,*next;
   for (i=0;i<ht->numBuckets;i++) {
      bkt = &ht->bkts[i];
      LIST_DESTROY(bkt,cb);
   }
   free(ht->bkts);
   free(ht);
}

int
HTForAll(HashTable* ht,HTForAllFn cb,void* args){
   int i;
   int rv;
   ListNode *bkt;
   ListNode *tmp;
   for (i=0;i<ht->numBuckets;i++) {
      bkt = &ht->bkts[i];
      LIST_FOR_ALL(bkt,tmp) {
         if ((rv=cb(tmp->data,args))) {
            return rv;
         }
      }
   }
   return 0;
}

/*
 * The following two algorithms are copied from
 * http://www.cse.yorku.ca/~oz/hash.html
 *
 * This is for ascii strings may not work well for unicode strings we are dealing
 * with now. Also strlen in necessary here.
 */

uint32_t SDBMStrHash(void* data,uint32_t len)
{
	uint32_t hash = 0;
	const uint8_t* str = data;
	int c,i;

	for (i=0;i<len;i++) {
		c = str[i];
		if ( str[i] ) {
			hash = c + (hash << 6) + (hash << 16) - hash;
		}
	}
	return hash;
}

uint32_t DJB2StrHash(const void *data,uint32_t len) 
{
	uint32_t hash = 5381;
	const uint8_t* str = data;
	int c,i;

	//printf("INHASH: ");
	for (i=0;i<len;i++) {
	//	printf("%c",str[i]);
		if (str[i]) {
			c = str[i];
			hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
		}
	}
	//printf("    hash value = %u \n",hash);
	return hash;
}

