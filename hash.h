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

#ifndef __HASH_H__
#define __HASH_H__
#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>


//#define Malloc malloc

static inline void* 
Malloc(size_t size) {
	void* tmp = malloc(size);
	if (tmp) {
		memset(tmp,0,size);
	}
	return tmp;
}


typedef enum HashSearchOp{
	HASH_ADD_CMP,
	HASH_SEARCH_CMP
}HashSearchOp;

typedef uint32_t (*HashFn)(const void* data,uint32_t size);
typedef uint8_t (*HashCmpFn)(void* fullData, void* searchData);
typedef void (*HTElemFn)(void* data);

struct HashTable;
typedef struct HashTable HashTable;


HashTable* HTInit(HashFn hashFn,HashCmpFn cmpFn, int numBuckets);
void* HTFind(HashTable* ht,const void* key,uint32_t size);
int HTInsert(HashTable* ht, void* toAdd,const void* key ,uint32_t size,void** exist);
void* HTRemove(HashTable* ht,void* key,uint32_t size);

/* Some useful hash functions */
uint32_t DJB2StrHash(const void *data,uint32_t len); 
typedef int (*HTForAllFn)(void* data,void* args);
typedef void (*HTDestroyCBFn) (void* data);
int HTForAll(HashTable* ht,HTForAllFn cb,void* args);
void HTDestroy(HashTable* ht,HTDestroyCBFn cb);

#endif
