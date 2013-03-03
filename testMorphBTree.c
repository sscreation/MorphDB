#include "morphdb.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

double UTime();

void GenerateStr(char* str, int len) {
   int i;
   for (i=0;i<len;i++) {
      str[i] = 'a'+(i%26);
   }
   str[i] = '\0';
   printf("%s\n",str);
}

int InitRand(char* fileName,int kSize);
int GetKey(char* buf, int *len);

int FnTest(MorphDB* db) {
   int a=100,v=1000;
   BTreeValue key,value,n;
   printf("Inserting key %d:%d\n",a,v);
   key.data = (char*)&a;
   key.len = sizeof(a);
   value.data = (char*)&v;
   value.len = sizeof(v);
   assert(MorphDBSet(db,key,value,True));
   assert(MorphDBGet(db,key,&n));
   printf("Got value %d\n",*(int*)n.data);
   assert(v == *(int*)n.data);
   BinaryStrFree(&n);

   a=5; v=5*a;
   assert(MorphDBSet(db,key,value,True));
   assert(MorphDBGet(db,key,&n));
   printf("Got value %d\n",*(int*)n.data);
   assert(v == *(int*)n.data);
   BinaryStrFree(&n);

   a=7; v=5*a;
   assert(MorphDBSet(db,key,value,True));
   assert(MorphDBGet(db,key,&n));
   printf("Got value %d\n",*(int*)n.data);
   assert(v == *(int*)n.data);
   BinaryStrFree(&n);

   a=79; v=5*a;
   assert(MorphDBSet(db,key,value,True));
   assert(MorphDBGet(db,key,&n));
   printf("Got value %d\n",*(int*)n.data);
   assert(v == *(int*)n.data);
   BinaryStrFree(&n);

   MorphDBCursor* cursor;
   assert(MorphDBCursorInit(db,&cursor));
   
   do {
      BTreeValue key,value;
      MorphDBCursorValue(cursor,&key,&value);
      printf("Cursor key : %d value : %d\n",*(int*)key.data,*(int*)value.data);
      BinaryStrFree(&key);
      BinaryStrFree(&value);
   }while(MorphDBCursorNext(cursor));
   a = 7;
   printf("Jumping to key %d\n",a);
   assert(MorphDBCursorSetPos(cursor,key));
   printf("Cursor iteration after jumping.\n");
   do {
      BTreeValue key,value;
      MorphDBCursorValue(cursor,&key,&value);
      printf("Cursor key : %d value : %d\n",*(int*)key.data,*(int*)value.data);
      BinaryStrFree(&key);
      BinaryStrFree(&value);
   }while(MorphDBCursorNext(cursor));
   a = 79;
   printf("Setting end key %d .\n",a);
   MorphDBCursorSetEndKey(cursor,key);
   a = 7; 
   printf("Setting start to  key %d .\n",a);
   MorphDBCursorSetStartKey(cursor,key);
   printf("Range cursor .\n");
   do {
      BTreeValue key,value;
      MorphDBCursorValue(cursor,&key,&value);
      printf("Cursor key : %d value : %d\n",*(int*)key.data,*(int*)value.data);
      BinaryStrFree(&key);
      BinaryStrFree(&value);
   }while(MorphDBCursorNext(cursor));
   MorphDBCursorFree(cursor);
   printf("Number of objects in the db %lu.\n",MorphDBCount(db));

   printf("Deleting key %d.\n",a);
   MorphDBDelete(db,key);
   assert(!MorphDBGet(db,key,&n));
   printf("Number of objects in the db %lu.\n",MorphDBCount(db));
   MorphDBFree(db);
   return 0;
}

int main(int argc, char** argv) {
   int i=1;   
   int j=i*10;
   int rv;
   int len = 0;
   int count = 1000*1000;
   MorphDB* db;
   BinaryStr key;
   BinaryStr val;
   BinaryStr val2;
   char *data = NULL;

   if (argc < 3) {
      printf("Usage: %s <dbName> <strlen> [count]\n",argv[0]);
      return -1;
   }
   len = atoi(argv[2]);
   if (argc > 3) {
      count = atoi(argv[3]);
   }
   data = malloc(len+1);
   assert(data);

   key.data = (char*) &i;
   key.len  = sizeof(int);
   GenerateStr(data,len);
   val.data = data;
   val.len  = len;
   val = key;
   (rv = MorphDBBTreeInit(argv[1],35,0,&db));
   printf("%d \n",rv);
   
   assert(!rv);
   free(data);
   return FnTest(db);
   double t1,t2;
   assert(!InitRand("../benchmark/100MBRand",20));
   t1= UTime();
   key.data = malloc(1024);
   //key.data = (char*)&i;
   for (i=0;i< count;i++) {
      GetKey(key.data,&key.len);
      //printf("key %d: %s\n",key.len,key.data);
      assert(MorphDBSet(db,key,val,True));
      if (i%10000 == 0) {
         printf("Prog %d\n",i);
      }
   }
   t2=UTime();
   printf("For insertion %lf \n",t2-t1);
   return 0;
   for(i=0;i<count;i++) {
      assert(MorphDBGet(db,key,&val));
      assert(!memcmp(val.data,key.data,val.len));
      BinaryStrFree(&val);
   }
   for (i=0;i< count;i++) {
      //printf("Trying to delete %d\n",i);
      assert(MorphDBDelete(db,key));
      if (i%10000 == 0) {
         printf("Del %d\n",i);
      }
   }
   MorphDBFree(db);
   return 0;
}
