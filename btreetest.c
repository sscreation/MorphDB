#include "morphdb.h"
#include <assert.h>

int main() {
   int i=1;   
   int j=i*10;
   int rv;

   MorphDB* db;

   BinaryStr key;
   BinaryStr val;
   BinaryStr val2;

   key.data = (char*) &i;
   key.len  = sizeof(int);
   val.data = (char*) &j;
   val.len  = sizeof(int);
   assert(!(rv = MorphDBBTreeInit("test",3,0,&db)));
   assert(MorphDBSet(db,key,val,True));
   assert(MorphDBGet(db,key,&val2));
   assert(!memcmp(val.data,val2.data,val.len));
   printf("%d\n",*(int*)val2.data);
   j += 10;
   BinaryStrFree(&val2);
   assert(MorphDBSet(db,key,val,True));
   assert(MorphDBGet(db,key,&val2));
   assert(!memcmp(val.data,val2.data,val.len));
   printf("%d\n",*(int*)val2.data);

   BinaryStrFree(&val2);
   assert(MorphDBDelete(db,key));
   assert(!MorphDBGet(db,key,&val2));
   MorphDBFree(db);
   return 0;
}
