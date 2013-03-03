#include "morphdb.h"

static MorphDBError __thread tMorphDBError;

typedef struct MorphDBErrDesc {
   MorphDBError errnum;
   char* errString;
}MorphDBErrDesc;

static MorphDBErrDesc errDesc[]= {
};

MorphDBError MorphDBGetError() {
   return tMorphDBError;
}

void MorphDBSetError(MorphDBError error) {
   tMorphDBError = error;
}

inline uint8_t MorphDBSet(MorphDB* db,BinaryStr key,BinaryStr value,uint8_t insert) {
   MorphDBSetError(MORPHDB_OK);
   if (db->dbOps.set) {
      return db->dbOps.set(db->beData,key,value,insert);
   } else {
      MorphDBSetError(MORPHDB_NOT_IMPLEMENTED);
      return False;
   }
}

void MorphDBSetKeyOps(MorphDB* db,BTreeKeyOps keyOps) {
   db->dbOps.setKeyOps(db->beData,keyOps);
}

uint64_t MorphDBCount(MorphDB* db) {
   if (db->dbOps.count){
      return db->dbOps.count(db->beData);
   }
   MorphDBSetError(MORPHDB_NOT_IMPLEMENTED);
   return 0;
}

inline uint8_t MorphDBInsert(MorphDB* db,BinaryStr key,BinaryStr value) {
   MorphDBSetError(MORPHDB_OK);
   if (db->dbOps.insert) {
      return db->dbOps.insert(db->beData,key,value);
   } else {
      MorphDBSetError(MORPHDB_NOT_IMPLEMENTED);
      return False;
   }
}

inline uint8_t MorphDBDelete(MorphDB* db,BinaryStr key) {
   MorphDBSetError(MORPHDB_OK);
   if (db->dbOps.del) {
      return db->dbOps.del(db->beData,key);
   } else {
      MorphDBSetError(MORPHDB_NOT_IMPLEMENTED);
      return False;
   }
}

inline uint8_t MorphDBCursorInit(MorphDB* db,MorphDBCursor** cursor) {
   MorphDBSetError(MORPHDB_OK);
   if (db->dbOps.cursorInit) {
      uint8_t rv;
      MorphDBCursor* cur = malloc(sizeof(*cur));
      if (!cur) {
         MorphDBSetError(MORPHDB_NO_MEMORY);
         return False;
      }
      rv = db->dbOps.cursorInit(db->beData,&cur->beData);
      if (!rv) {
         free(cur);
         return rv;
      }
      assert(sizeof(cur->cursorOps) == sizeof(db->cursorOps));
      cur->cursorOps = db->cursorOps;
      *cursor = cur;
      return rv;
   } else {
      MorphDBSetError(MORPHDB_NOT_IMPLEMENTED);
      return False;
   }
}

inline uint8_t MorphDBGet(MorphDB* db,BinaryStr key,BinaryStr* value) {
   MorphDBSetError(MORPHDB_OK);
   if (db->dbOps.get) {
      return db->dbOps.get(db->beData,key,value);
   } else {
      MorphDBSetError(MORPHDB_NOT_IMPLEMENTED);
      return False;
   }
}

inline void MorphDBRemove(MorphDB* db) {
   MorphDBSetError(MORPHDB_OK);
   if (db->dbOps.remove) {
      db->dbOps.remove(db->beData);
   } else {
      MorphDBSetError(MORPHDB_NOT_IMPLEMENTED);
   }
   free(db);
}

void MorphDBFree(MorphDB* db) {
   MorphDBSetError(MORPHDB_OK);
   if (db->dbOps.close) {
      db->dbOps.close(db->beData);
   } else {
      MorphDBSetError(MORPHDB_NOT_IMPLEMENTED);
   }
   free(db);
}

MorphDBError MorphDBGetLastError(MorphDB* db){
   if (db->error != MORPHDB_OK) {
      return db->error;
   }
   if (db->dbOps.set) {
      return db->dbOps.getLastError(db->beData);
   } else {
      return MORPHDB_NOT_IMPLEMENTED;
   }
}

const char* MorphDBGetLastErrorString(MorphDB* db) {
   MorphDBSetError(MORPHDB_OK);
   if (db->dbOps.set) {
      return db->dbOps.getLastErrorString(db->beData);
   } else {
      return NULL;
   }
}

uint8_t MorphDBCursorSetEndKey(MorphDBCursor* cursor,BTreeKey key) {
   return cursor->cursorOps.setEndKey(cursor->beData,key);
}

uint8_t MorphDBCursorSetStartKey(MorphDBCursor* cursor,BTreeKey key) {
   return cursor->cursorOps.setStartKey(cursor->beData,key);
}
uint8_t MorphDBCursorNext(MorphDBCursor* cursor) {
   return cursor->cursorOps.next(cursor->beData);
}
uint8_t MorphDBCursorValue(MorphDBCursor* cursor,BTreeKey* key,BTreeValue* value) {
   return cursor->cursorOps.value(cursor->beData,key,value);
}
uint8_t MorphDBCursorSetPos(MorphDBCursor* cursor,BTreeKey key) {
   return cursor->cursorOps.setPos(cursor->beData,key);
}
uint8_t MorphDBCursorPrev(MorphDBCursor* cursor) {
   if (!cursor->cursorOps.prev) {
      MorphDBSetError(MORPHDB_NOT_IMPLEMENTED);
      return False;
   }
   return cursor->cursorOps.prev(cursor->beData);
}
   
uint8_t MorphDBCursorSetToStart(MorphDBCursor* cursor) {
   return cursor->cursorOps.setToStart(cursor->beData);
}

uint8_t MorphDBCursorSetToEnd(MorphDBCursor* cursor) {
   return cursor->cursorOps.setToEnd(cursor->beData);
}

void MorphDBCursorFree(MorphDBCursor* cursor) {
   cursor->cursorOps.free(cursor->beData);
   free(cursor);
}
