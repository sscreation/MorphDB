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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

static int fd;
char* buf;
static int keySize;
static int keyIndex;

int InitRand(char* fileName,int kSize) {
   fd = open(fileName,O_RDONLY);
   keySize = kSize;
   assert(fd > 0);
   return 0;
}

int GetKey(char* buf, int *len) {
   if (fd) {
      *len = read(fd,buf,keySize);
   } else {
      *len =snprintf(buf,1024,"KEY in hex %x",keyIndex);
      keyIndex++;
   }
   return 0;
}

int GetRandKey(char* buf,int* len) {
   if (fd) {
      *len = pread(fd,buf,keySize,(rand()*keySize)% 80*1024*1024);
   } else {
      *len = (int)snprintf(buf,1024,"KEY in hex %x",rand());
   }
   return 0;
}
