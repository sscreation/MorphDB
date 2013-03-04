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

#ifndef __PLATFORMOPS_H__
#define __PLATFORMOPS_H__

#include "platform_linux.h"
#include "platform_windows.h"
typedef void* RWLock;

RWLock PlatformRWLockInit();
int PlatformRWGetRLock(RWLock lock);
int PlatformRWGetWLock(RWLock lock);
int PlatformRWTryWLock(RWLock lock);
int PlatformRWTryRLock(RWLock lock);
int PlatformRWUnlock(RWLock lock);
void PlatformRWLockFree(RWLock lock);

#endif
