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

#ifndef __MMDB_KEY_VAL_LINUX_H__
#define __MMDB_KEY_VAL_LINUX_H__

/*#include <stdint.h>
#include "stringutil.h"
#include "list.h"
#include "hash.h"
#include "mmdbdef.h"*/

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <limits.h>

typedef struct BlockMgrFileHandle {
   char name[NAME_MAX];
   int fd;
   uint64_t error;
   int osError;
}BlockMgrFileHandle;

#endif
