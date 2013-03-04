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

#ifndef __BMGRERROR_DEF_H__
#define __BMGRERROR_DEF_H__

#define True 1
#define False 0

typedef enum BlockMgrError {
   BLOCKMGR_OK=0,
   BLOCKMGR_IO_ERR,
   BLOCKMGR_FILE_NOT_FOUND,
   BLOCKMGR_INVALID_MAGIC,
   BLOCKMGR_INT_ERROR,
   BLOCKMGR_BLOCK_TOO_BIG,
   BLOCKMGR_DATA_TOO_BIG,
   BLOCKMGR_INVALID_BLOCK_SIZE,
   BLOCKMGR_NO_MEMORY,
   BLOCKMGR_TRANSAC_EXISTS,
   BLOCKMGR_NO_TRANSAC,
   BLOCKMGR_TRANSAC_REPLAY_FAILED,
   BLOCKMGR_TRANSAC_NOT_COMMITTED,
   BLOCKMGR_CHECKSUM_ERR,
   BLOCKMGR_CORRUPT_DATA,
   BLOCKMGR_LOCK_FAILED,
} BlockMgrError;


#endif
