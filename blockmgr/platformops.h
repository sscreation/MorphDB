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
