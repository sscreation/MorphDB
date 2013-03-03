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
