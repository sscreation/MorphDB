#include "btreekeyops.h"
#include <assert.h>

void
BTreeKeyIntPrint(BTreeKey* key) {
   //assert(key->len);
   //assert((*(int*)key->data)< 2000);
   printf("%p %d",key->data,*((int*)key->data));
}

#define INTCMP(i1,i2) ((i1) > (i2) ? 1 : ((i1) == (i2) ? 0:-1))

int
BTreeKeyRangeCmp(BTreeKey* key1,BTreeKey* key2) {
   LongRange *r1 = (LongRange*)key1->data;
   LongRange *r2 = (LongRange*)key2->data;
   assert(key1->len == sizeof(LongRange));
   if (r1->start == r2->start ) {
      // Start of the range is unique
      return 0;
   }
   if ((r1->start < r2->start)  && (r1->end > r2->start)) {
      return 0;
   }
   return INTCMP(r1->start,r2->start);
}


int
BTreeKeyBStrCmp(BTreeKey* key1,BTreeKey* key2) {
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

int
BTreeKeyOidCmp(BTreeKey* k1,BTreeKey* k2) {
   uint32_t *key1=(uint32_t*)k1->data,*key2=(uint32_t*)k2->data;
   int i = INTCMP(key1[0],key2[0]);
   if (i) {
      return i;
   }
   i = INTCMP(key1[1],key2[1]);
   if (i) {
      return i;
   }
   i = INTCMP(key1[2],key2[2]);
   return i;
}

void
BTreeKeyBStrPrint(BTreeKey* key) {
   /*char *str = malloc(key->len*2+1);
   if (!str) {
      return;
   }*/
   int i;
   for (i=0;i<key->len;i++) {
      printf("%02x",(uint8_t)key->data[i]);
   }
   //printf("%s",str)
}

void
BTreeKeyStrPrint(BTreeKey* key) {
   /*char *str = malloc(key->len*2+1);
   if (!str) {
      return;
   }*/
   int i;
   for (i=0;i<key->len;i++) {
      printf("%c",(uint8_t)key->data[i]);
   }
   //printf("%s",str)
}

uint8_t
BTreeKeyBStrCopy(BTreeKey* dst,BTreeKey* src,uint8_t new){
   if (!new) {
      assert(src->len <= dst->len);
      memcpy(dst->data,src->data,src->len);
      dst->len = src->len;
      /*BTreeKey k1;
      uint32_t *i = (uint32_t*) src->data;
      k1 = *dst;
      assert(i[0]);


      //assert(!memcmp(dst->data,src->data,src->len));
      //assert(k1.data == dst->data);*/
      return True;
   }
   dst->data = malloc(src->len);
   if (!dst->data) {
      assert(0);
      return False;
   }
   assert(src->len);
   memcpy(dst->data,src->data,src->len);
   //assert(!memcmp(dst->data,src->data,src->len));
   dst->len = src->len;
//   printf("After copying : ");BTreeKeyIntPrint(dst);printf(" ");BTreeKeyIntPrint(src);printf("\n");
   return True;
}

void
BTreeKeyBStrFree(BTreeKey* key) {
  // printf("Freeing key ");BTreeKeyIntPrint(key);printf("\n");
   free(key->data);
   key->data = NULL;
   key->len  = 0;
}

int
BTreeKeyIntCmp(BTreeKey* k1,BTreeKey* k2) {
   int key1,key2;
   key1 = *(int*)k1->data;
   key2 = *(int*)k2->data;
   return INTCMP(key1,key2);
   /*printf("%d %d\n",key1,key2);
   if (key1 < key2) {
      return -1;
   }
   if (key1 > key2) {
      return 1;
   }
   return 0;*/
}
