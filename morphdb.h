#ifndef __MORPH_DB_H__
#define __MORPH_DB_H__

#include <stdint.h>
#include "stringutil.h"
#include "btreekeyops.h"
#include <assert.h>

typedef enum MorphDBError {
   MORPHDB_OK=0,            /*!< Success */  
   MORPHDB_IO_ERR=1,        /*!< Read or write system call failed. */
   MORPHDB_INT_ERROR=2,     /*!< Unexpected internal error*/
   MORPHDB_NO_MEMORY=3,     /*!< No memory. */
   MORPHDB_CURSOR_END=4,    /*!< End of cursor reached. */
   MORPHDB_FILE_NOT_FOUND=5,/*!< File not found. */
   MORPHDB_INVALID_MAGIC=6, /*!< Invalid magic in the file. */
   MORPHDB_ENTRY_EXISTS=7,  /*!< Entry already exists. */
   MORPHDB_NOT_FOUND=8,    /*!< Entry not found. */
   MORPHDB_INVALID_VALUE=9,/*!< Invalid input value. */
   MORPHDB_VALUE_TOO_BIG=10, /*!< Data value too big. */
   MORPHDB_NOT_IMPLEMENTED=11, /*!< Function not implemented. */
   MORPHDB_INVALID_HEADER=12,  /*!< Invalid header information in file. */
   MORPHDB_INVALID_VERSION=13, /*!< Invalid version in file. */
   MORPHDB_OTHER_ERR=14,      /*!< Other error than any of the above. */
}MorphDBError;

typedef uint8_t (*DBSet)(void* beData,BinaryStr key,BinaryStr value,uint8_t insert);
typedef uint8_t (*DBInsert)(void* beData,BinaryStr key,BinaryStr value);
typedef uint8_t (*DBDelete)(void* beData,BinaryStr key);
typedef uint8_t (*DBCursorInit)(void* beData,void** cursor);
typedef uint8_t (*DBGet)(void* beData,BinaryStr key,BinaryStr* value);
typedef void (*DBRemove)(void* beData);
typedef uint8_t (*DBSync) (void* beData);
typedef void (*DBClose) (void* beData);
typedef MorphDBError (*DBGetLastError) (void* beData);
typedef const char* (*DBGetLastErrorString) (void* beData);
typedef void (*DBSetKeyOps) (void* beData,BTreeKeyOps keyOps);
typedef uint64_t (*DBCount)(void* beData);

// cursor ops
typedef uint8_t (*DBCursorSetEndKey)(void* cursor,BTreeKey key);
typedef uint8_t (*DBCursorSetStartKey)(void* cursor,BTreeKey key);
typedef uint8_t (*DBCursorNext)(void* cursor);
typedef uint8_t (*DBCursorValue)(void* cursor,BTreeKey* key,BTreeValue* value);
typedef uint8_t (*DBCursorSetPos)(void* cursor,BTreeKey key);
typedef uint8_t (*DBCursorPrev)(void* cursor);
typedef uint8_t (*DBCursorSetToStart)(void* cursor);
typedef uint8_t (*DBCursorSetToEnd)(void* cursor);
typedef void (*DBCursorFree)(void* cursor);
typedef MorphDBError (*DBCursorGetLastError) (void* cursor);
typedef const char* (*DBCursorGetLastErrorString) (void* cursor);

typedef struct DBOps {
   DBSet          set;
   DBInsert       insert;
   DBDelete       del;
   DBRemove       remove;
   DBClose        close;
   DBCursorInit   cursorInit;
   DBGet          get;
   DBSync         sync;
   DBSetKeyOps    setKeyOps;
   DBCount        count;
   DBGetLastError getLastError;
   DBGetLastErrorString getLastErrorString;
} DBOps;

typedef struct DBCursorOps {
   DBCursorSetEndKey   setEndKey;
   DBCursorSetStartKey setStartKey;
   DBCursorNext        next;
   DBCursorPrev        prev;
   DBCursorValue       value;
   DBCursorSetPos      setPos;
   DBCursorGetLastError getLastError;
   DBCursorGetLastErrorString getLastErrorString;
   DBCursorFree        free;
   DBCursorSetToEnd    setToEnd;
   DBCursorSetToStart  setToStart;
}DBCursorOps;

/**
 * Internal representation of MorphDB.
 * Do not interpret the fields they are subjected to change.
 */
typedef struct MorphDB {
   char beName[50];
   void* beData;
   MorphDBError error;
   DBOps dbOps;
   DBCursorOps cursorOps;
} MorphDB;

typedef struct MorphDBCursor {
   void* beData;
   MorphDBError error;
   DBCursorOps cursorOps;
}MorphDBCursor;

/* Module initialization functions.
 */

/**
 * @file -- morphdb.h
 *
 * MorphDB is an unified interface for different implementaiton of key - value pairs.
 *
 * Initialize MorphDB handle with any of the available
 * implementation functions and use the same interface
 * for different implementations like on-disk B+Tree or
 * in-memory SkipList implementation.
 *
 * Currently 2 implementations are done.
 *   MorphDBBTree -- On-disk key value pair implemenation. Uses B+Tree datastructure on top of file.
 *   @see MorphDBBTreeInit 
 *   SkipList     -- In-memory key value pair implementation . Uses SkipList datastructure.
 *   @see SkipListMDBInit
 */

/**
 * Initialization functions.
 */

/**
 * \brief Initialize MorphDB Btree backend on top of the filename
 * given.
 *
 * If the file does not exist file is created and formatted with BlockMgr and MorphDBBTree format.
 *
 * \param [in] name -- Name of the file.
 * \param [in] tOrder -- Order of the B+Tree on-disk. 3-35
 * \param [in] features -- XORed features value.
 *  Current valid values or
 *  ((uint64_t)0x1) -- ENABLE CHECK SUM
 *  features is used only when new data base is created.
 *
 * \param [out] db    -- Output MorphDB handle to be used in subsequent calls.
 *
 */
MorphDBError MorphDBBTreeInit(char* name,int tOrder,uint64_t features,MorphDB** db);

/**
 * \brief Initialize MorphDB SkipList backend on top of the filename
 * given.
 * \param [in] name -- Name of the db currently unused.
 * \param [in] tOrder -- Maximum levels of SkipList.
 * \param [out] outDB    -- Output MorphDB handle to be used in subsequent calls.
 */
MorphDBError SkipListMDBInit(char* name,int maxLevels,MorphDB** outDB);


/** 
 * MorphDB Unified interfaces.
 */

/**
 * \brief Sets value corresponding to the given.
 *
 * If insert value is 0 then call fails with MORPHDB_NOT_FOUND if the key is not found in db.
 *
 * If insert value is 1 and key is not found new value
 * with the key and value is inserted.
 *
 * \param [in] db --  MorphDB Handle.
 * \param [in] key -- Key to search for
 * \param [in] value -- Value to set for the given key.
 * \param [in] insert -- If 1 is given key is inserted if not found.
 *
 * @return True on Success False on failure with appropriate error set.
 *
 */
uint8_t MorphDBSet(MorphDB* db,BinaryStr key,BinaryStr value,uint8_t insert);


/**
 * \brief Inserts a new key-value pair to the db.
 *  
 *  If value is already existing fails with MORPHDB_ENTRY_EXISTS error.
 *
 * \param [in] db --  MorphDB Handle.
 * \param [in] key -- Key to search for
 * \param [in] value -- Value to set for the given key.
 *
 * @return True on Success False on failure with appropriate error set.
 *
 */
uint8_t MorphDBInsert(MorphDB* db,BinaryStr key,BinaryStr value);

/**
 * \brief Deletes given key-value from the db.
 * \param [in] db --  MorphDB Handle.
 * \param [in] key -- Key to delete.
 *  
 * @return True on Success False on failure with appropriate error set.
 *
 */
uint8_t MorphDBDelete(MorphDB* db,BinaryStr key);

/**
 * \brief Get value corresponding the key.
 *
 * Returns value corresponding to the key if key is found.
 * \param [in] db --  MorphDB Handle.
 * \param [in] key -- Key to search for.
 * \param [out] value -- Output value corresponding to the key.
 *  
 * @return True on Success False on failure with appropriate error set.
 * @note On success val->data will be malloced user is responsible in freeing it.
 *
 */
uint8_t MorphDBGet(MorphDB* db,BinaryStr key,BinaryStr* value);

/**
 * \brief Initializes a cursor to traverse through key-value pairs.
 * \param [in] db --  MorphDB Handle.
 * \param [out] cursor -- Output cursor.
 * @return True on Success False on failure with appropriate error set.
 * @see MorphDBCursorFree
 * @note if success cursor should be freed with MorphDBCursorFree.
 *
 */
uint8_t MorphDBCursorInit(MorphDB* db,MorphDBCursor** cursor);

/**
 * \brief Set key ops to be used by the db.
 * \param [in] db --  MorphDB Handle.
 * \param [in] keyOps -- Input keyops structure.
 *
 */
void MorphDBSetKeyOps(MorphDB* db,BTreeKeyOps keyOps);

/**
 * \brief Returns the number key-value pair in the DB.
 *
 * 0 is returned and MORPHDB_NOT_IMPLEMENTED error is set
 * if the function is not implemented.
 */

uint64_t MorphDBCount(MorphDB* db);
/**
 * \brief Removes the given database also frees resources used by it.
 *
 * Removes the backend file also incase ondisk database.
 * \param [in] db -- MorphDB handle.
 */

void MorphDBRemove(MorphDB* db);
/**
 * \brief Frees the resources used by db.
 * @note Handle becomes invalid after this call.
 */
void MorphDBFree(MorphDB* db);
/**
 * \brief Get last error in morphdb call for the calling thread.
 */
MorphDBError MorphDBGetLastError(MorphDB* db);

/**
 * \brief Returns const string corresponding to last error.
 * @note this is const string and not malloced do not free it.
 */
const char* MorphDBGetLastErrorString(MorphDB* db);

/**
 * \brief Move cursor to the next key-value pair.
 *
 * \param [in] cursor -- Cursor to handle returned by MorphDBCursorInit
 *
 * @return True on Success False on failure with appropriate error set.
 *
 * If end of cursor is reached MORPHDB_CURSOR_END is set with return value False.
 * Other error values are set in case of other errors.
 */
uint8_t MorphDBCursorNext(MorphDBCursor* cursor);

/**
 * \brief Get key and value from the current cursor position.
 * \param [in] cursor -- Cursor to handle returned by MorphDBCursorInit
 * \param [out] key   -- Output key. malloced data should be freed.
 * \param [out] value -- Output value.
 * 
 * @return True on Success False on failure with appropriate error set.
 *
 * @note if the call is successful data in key/value fields
 * will be malloced has to be freed by the user using
 * BinaryStrFree after use.
 */
uint8_t MorphDBCursorValue(MorphDBCursor* cursor,BTreeKey* key,BTreeValue* value);

/**
 * \brief Sets the current position of the cursor to the given key.
 * \param [in] cursor -- Cursor to handle returned by MorphDBCursorInit
 * \param [in] key   -- Key to jump to.
 *
 * If the key is not found next value greater than the key will be the current value.
 * @return True on Success False on failure with appropriate error set.
 */
uint8_t MorphDBCursorSetPos(MorphDBCursor* cursor,BTreeKey key);

/**
 * \brief Sets starting key of the cursor to the given key.
 * This function will also set the cursor position to the
 * key. This will be used by MorphDBCursorSetToStart 
 *
 * \param [in] cursor -- Cursor to handle returned by MorphDBCursorInit
 * \param [in] key   -- Key to jump to.
 * @return True on Success False on failure with appropriate error set.
 */
uint8_t MorphDBCursorSetStartKey(MorphDBCursor* cursor,BTreeKey key);

/**
 * \brief Sets the last key of the cursor.
 * If keys greater than this key cursorNext will fail 
 * with MORPHDB_CURSOR_END.
 * Using MorphDBCursorSetStartKey and MorphDBCursorSetEndKey range search can be done.
 * \param [in] cursor -- Cursor to handle returned by MorphDBCursorInit
 * \param [in] key   --  Last key to stop the traversal
 * @return True on Success False on failure with appropriate error set.
 */
uint8_t MorphDBCursorSetEndKey(MorphDBCursor* cursor,BTreeKey key);

/**
 * \brief Sets the current position of cursor to start.
 *
 * @return True on Success False on failure with appropriate error set.
 */
uint8_t MorphDBCursorSetToStart(MorphDBCursor* cursor);

/**
 * \brief Frees the resource used by the cursor.
 * Cursor becomes invalid after this point.
 */
void MorphDBCursorFree(MorphDBCursor* cursor);

#endif
