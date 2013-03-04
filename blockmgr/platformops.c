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

#include "platformops.h"
#include <stdlib.h>
RWLock PlatformRWLockInit() {
#ifndef WIN32
   pthread_rwlock_t *lock = malloc(sizeof(pthread_rwlock_t));
   if (!lock) {
      return NULL;
   }
   if(pthread_rwlock_init(lock,NULL)) {
      return NULL;
   }
   return lock;
#else
#endif
}

int PlatformRWGetRLock(RWLock lock) {
#ifndef WIN32
   pthread_rwlock_t *l = lock;
   return pthread_rwlock_rdlock(l);
#else
#endif
}

int PlatformRWGetWLock(RWLock lock){
#ifndef WIN32
   pthread_rwlock_t *l = lock;
   return pthread_rwlock_wrlock(l);
#else
#endif
}

int PlatformRWTryWLock(RWLock lock) {
#ifndef WIN32
   pthread_rwlock_t *l = lock;
   return pthread_rwlock_trywrlock(l);
#else
#endif
}

int PlatformRWTryRLock(RWLock lock) {
#ifndef WIN32
   pthread_rwlock_t *l = lock;
   return pthread_rwlock_tryrdlock(l);
#else
#endif
}

int PlatformRWUnlock(RWLock lock) {
#ifndef WIN32
   pthread_rwlock_t *l = lock;
   return pthread_rwlock_unlock(l);
#else
#endif
}

void PlatformRWLockFree(RWLock lock) {
#ifndef WIN32
   pthread_rwlock_t *l = lock;
   pthread_rwlock_destroy(l);
   free(l);
#else
#endif
}
