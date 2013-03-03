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
