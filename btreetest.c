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
