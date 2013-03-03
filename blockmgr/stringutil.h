#ifndef __STRING_UTIL_H__
#define __STRING_UTIL_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <stddef.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>

#define False 0
#define True 1

#define STR(a) ((a)->ptr)
#define StrToBStr(str,bStr) (bStr)->data = (char*)str; (bStr)->len = strlen(str);
#define BStrData(b) (char*)(b)->data

#define BStrCopy(dst,src) \
   memcpy((dst)->data,(src)->data,(src)->len);\
   (dst)->len = (src)->len;

typedef struct BinaryStr{
   char* data;
   uint32_t len;
} BinaryStr;

typedef struct String {
   uint32_t len;
   uint32_t curLen;
   char* ptr;
} String;

static inline int
StrInit(String* str){
   str->len =0;
   str->curLen =0;
   str->ptr = NULL;
   return 0;
}

static inline int
StrAppend(String* str, const char* toAppend) {
   while (*toAppend) {
      if (str->len == str->curLen) {
         char *tmp = (char*)malloc(str->len+128);
         if (!tmp) {
            printf("Error: No memory.\n");
            return ENOMEM;
         } 
         memcpy(tmp,str->ptr,str->curLen);
         str->len = str->len+128;
         free(str->ptr);
         str->ptr = tmp;
         str->ptr[str->curLen] = '\0';
      }
      str->ptr[str->curLen++] = *toAppend;
      toAppend++;
   }
   str->ptr[str->curLen] = '\0';
   return 0; 
}

static inline int
StrAppendMulti(String* str, int numStr,...) {
   va_list ap;
   va_start(ap,numStr);
   int i =0;
   int rv;

   for (i=0;i<numStr;i++) {
      char* toAppend = va_arg(ap,char*);
      if ((rv=StrAppend(str,toAppend))) {
         return rv;
      }
   }
   va_end(ap);
   return 0;
}

static inline int
StrAppendFmt(String* str, const char* fmtStr,...) {
   char resultStr[4096];
   va_list ap;
   int numChars;
   int rv;
   va_start(ap,fmtStr);
   numChars = vsnprintf(resultStr,sizeof(resultStr),fmtStr,ap);
   if ((rv= StrAppend(str,resultStr))) {
      return rv;
   }
   va_end(ap);
   if (numChars > sizeof(resultStr)) {
      return 1;
   }
   return 0; 
}

static inline void
StrFree(String *str) {
   free(str->ptr);
   str->len = -1;
   str->curLen = -1;
}


static inline uint8_t BStrEqual(BinaryStr* val1,BinaryStr* val2) {
   if (val1->len != val2->len) {
      return False;
   }
   return !memcmp(val1->data,val2->data,val1->len);
}

static inline int
BinaryStrDup(BinaryStr* dst,BinaryStr* src) {
   dst->data = malloc(src->len);
   if (!dst->data) {
      return -1;
   }
   dst->len = src->len;
   memcpy(dst->data,src->data,src->len);
   return 0;
}

static inline void
BinaryStrFree(BinaryStr* key) {
   free((void*)key->data);
   memset(key,0,sizeof(*key));
}

static inline void BinaryStrPrint(BinaryStr* k) {
   int i;
   for (i=0;i<k->len;i++) {
      printf("%02x ",(uint8_t)k->data[i]);
   }
   printf("\n");
}

#endif 
