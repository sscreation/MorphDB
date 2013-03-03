#ifndef __MMDB_KEY_VAL_H__
#define __MMDB_KEY_VAL_H__

#include <stdint.h>
#include "stringutil.h"
//#include "hash.h"
#include "blockmgrerror.h"
#define _O(a) (((uint64_t)(a)->offset))
#define _SO(a,of) ((a)->offset = (of)); 
#define BlockMgrIsNullPtr(ptr) ((ptr)->offset ==0)
#define CEIL(a,b) ((a)%(b) == 0 ? (a)/(b) : ((a)/(b))+1)

typedef BinaryStr BTreeValue;
#pragma pack(1)
// Ondisk structures go here.

/**
 * Pointer to an on-disk datastructure.
 * Dont interpret these fields they are for
 * internal purposes of BlockMgr will change
 */ 

typedef struct BlockMgrDiskPtr {
   uint64_t blockSize:4;
   uint64_t unused:4;
   uint64_t offset:56;
} BlockMgrDiskPtr;

#define BLOCKMGR_FEATURE_CHECKSUM ((uint64_t )0x1)

#pragma pack()
/**
 * @file blockmgr.h
 *
 * Defines all block manager interfaces.
 *
 * Block manager is an innovative library which manages on-disk
 * blocks inside a file and provides a very simple interface to be used
 * for variety of on-disk datastructures. 
 *
 * Complex data structures like B+Tree or a SkipList database on-disk
 * could be written without worrying block management aspects like  
 *      - Finding a free block in the file 
 *      - Freeing a used block 
 *      - Expanding existing block to accommodate the datastructure 
 *      etc.
 *
 * Provides transaction support which complies with ACID for durability
 * and consistency of the data.
 *
 * @example diskList.c
 * diskList.c is an example of using  BlockMgr to implement an on-disk linked list.
 * 
 * It uses all the features of BlockMgr.
 * Compile and run the program and perform various operations on linked list using CLI
 *         - add an element
 *         - remove an element 
 *         - print the list.
 *         - start a transaction.
 *         - commit transaction.
 *         - abort transaction
 */

struct BlockMgrTransactHandle;

/**
 * Opaque handle to BlockMgr
 */
typedef struct BlockMgrHandle BlockMgrHandle;
/**
 * \brief Initializes BlockMgrHandle given a filename.
 * 
 * Initializes block manager handle which has to be
 * passed in all subsequent calls.
 *
 * @param[in] fileName  -- Name of the file. File should
 *                         be existing and should be able
 *                         to open for read/write.
 *                         For a new file use BlockMgrFormatFile before using InitHandle.
 * @param[in]     mode  -- For future puporses, not used as of now.
 *                         Always pass 0 to this value, in future when this parameter is
 *                         going to be used 0 will be equivalent to its current behavior.
 * @param[out] outHandle -- Pointer to the variable going to
 *                          be holding BlockMgrHandle.
 *
 * \returns BLOCKMGR_OK on success. Or other BlockMgrError on failure.
 *  BLOCKMGR_INVALID_MAGIC -- if file is not formatted with BlockMgrFormatFile or corrupt.
 *  @see BlockMgrFormatFile,BlockMgrFreeHandle
 */

BlockMgrError BlockMgrInitHandle(char* fileName,int mode,BlockMgrHandle ** outHandle);

/**
 * \brief Formats a given file with BlockMgr format.
 * 
 * Initializes given file with BlockMgr format and datastructures.
 * \warning The contents of input file will be deleted unconditionally.
 *
 * BlockMgrFormatFile has to be called before first time calling
 * BlockMgrInitHandle.
 *
 * @param[in] fileName -- Name of the file.
 * @param[in] features -- One or more features Bitwise ORed 
 *                        BLOCKMGR_FEATURE_* 
 *                        BLOCKMGR_FEATURE_CHECKSUM --> enables checksum check for the data.
 * @param[in] keySize  -- Always pass 0.
 *
 * \returns BLOCKMGR_OK on success. Or other BlockMgrError on failure.
 */

BlockMgrError BlockMgrFormatFile(char* fileName,uint64_t features);

/**
 * \brief Set user data for the file.
 *
 * User data is the entry point to the users of BlockMgr. 
 * This could be considered as superblock of the user data structure.
 * For e.g. 
 * In a B+Tree implementation using BlockMgr, this value will contain
 * the reference to root node and tree order etc.
 *
 * A reference to the value is stored in BlockMgr header and will
 * automatically be referenced during BlockMgrReadUserData.
 *
 * It can be any structure of any size.
 * See the example for the usage.
 *
 * @param[in] handle -- Handle to BlockMgr
 * @param[in] value  -- Uinterpreted user data as binary string .
 *
 * @return True on Success False on failure with appropriate error set.
 *
 */
uint8_t BlockMgrSetUserData(BlockMgrHandle* handle,BTreeValue* value);
/**
 *
 * \brief Reads user data into val.
 *
 * @see BlockMgrSetUserData for more details about userdata.
 * @param[in] handle -- Handle to BlockMgr
 * @param[out] val   -- Output binary string holding the user data.
 *
 * @return True on Success False on failure with appropriate error set.
 * On success `val` will hold the user data binary string read from disk.
 * @note val->data is malloc ed  should be freed by the user.
 */
uint8_t BlockMgrReadUserData(BlockMgrHandle* handle,BTreeValue* val);
/**
 *
 * \brief Frees the handle after use.
 *
 * @param[in]  handle -- Handle to BlockMgr
 * After this function handle becomes invalid to use.
 * Any use after this can cause crashes.
 */
void BlockMgrFreeHandle(BlockMgrHandle* handle);

/**
 *
 * \brief Frees the handle after use and removes the file.
 *
 * @param[in]  handle -- Handle to BlockMgr
 * After this function handle becomes invalid to use.
 * Any use after this can cause crashes.
 */
void BlockMgrRemove(BlockMgrHandle* handle);

/**
 * \brief Returns last error happened in BlockMgr module call.
 *
 * This is a thread local storage.
 * Each thread will have its own copy.
 * But subsequent call to BlockMgr from same thread will overwrite
 * the error value.
 */
BlockMgrError BlockMgrGetLastError();

/**
 * \brief Reads data which starts from the offset given by ptr.
 *
 * Reads the data whose starting point is at the offset given by ptr.
 * Data could be in one block or multiple blocks, the call will
 * read all the links and stitch together the data and return.
 * If checksum is enabled. Checksum of all chunks forming the data will
 * be verified if the checksum in all chunks match the checksum of the
 * whole data only then data is returned.
 *
 * @param[in]   handle  -- Handle to BlockMgr
 * @param[in]   ptr     -- Pointer to the start of the data.
 * @param[out]  val     -- Holds the data as binary string in case of success.
 * @return True on Success False on failure with appropriate error set.
 * @note On success val->data will be malloced user is responsible in freeing it.
 */
uint8_t BlockMgrDataRead(BlockMgrHandle* handle,BlockMgrDiskPtr ptr,BTreeValue *val);
/**
 * \brief Writes data which starts from whose old offset given by ptr.
 *
 * Writes the data starting point is at the offset given by ptr.
 * - New data 
 *   If the data is new i.e offset field of ptr is set to zero.
 *   set a new block(s) will be allocated and the data will be written to it.
 *   ptr will hold the pointer to the newly written data.

 * - Existing data
 *   i.e. Data was already in the file and previous start location is in
 *   ptr
 *   - Data Expansion
 *       In case the data is expanding i.e data size has increased from
 *       previous values. A new link will be created for the new portion
 *       of data incase existing links do not have enough space for the
 *       new data.
 *
 *       If link count has reached NUM_MAX_LINKS then entirely new location
 *       will be found for the data and written ptr will be modified to hold
 *       the new location.
 *       Old data blocks will be freed and will be invalid to use after this. 
 *   - Data shrinking
 *       If data size has become lesser than its previous version and if
 *       any of the link is redundant i.e the link/chunk does not store
 *       any part of the data will be freed.
 * Pointer returned by Write will be unique.
 *
 * @param[in]   handle  -- Handle to BlockMgr
 * @param[in]   val     -- Holds the data as binary string in case of success.
 * @param[in/out]ptr     -- Pointer to the start of the data.
 *                         Incase of new data offset field of ptr will be set to 0.
 *                         use _SO macro to set the offset.
 *                         ptr will hold the new start offset of the data after write.
 * @param[out] newPtr    -- Boolean value indicating if the new pointer is returned in ptr.
 *
 * @return True on Success False on failure with appropriate error set.
 *
 * @note Incase a new pointer is returned user is responsible for storing 
 * the references.If a new pointer is returned old ptr becomes invalid.
 * If the refrence is lost the block will be leaked.
 */
uint8_t BlockMgrDataWrite(BlockMgrHandle* handle,BTreeValue* val,BlockMgrDiskPtr * ptr,uint8_t* newPtr);

/** 
 * \brief Frees a data block whose starting address is startPtr.
 *
 * @param[in]   handle -- Handle to BlockMgr
 * @param[in]   startPtr -- Starting address of the data.
 * @return True on Success False on failure with appropriate error set.
 *
 */
uint8_t BlockMgrDataFree(BlockMgrHandle* handle,BlockMgrDiskPtr startPtr);

/**
 * \brief Syncs all buffered contents to disk.
 *
 * Note there is no buffering by BlockMgr this sync will commit
 * OS buffers to the disk.
 * similar to fsync.
 */
uint8_t BlockMgrDataSync(BlockMgrHandle* handle);
double UTime();

/*
 *Transaction related functions.
 */

/**
 * \brief Start a transaction for the BlockMgr specified by handle.
 *
 * All writes after this point will go to the journal file till
 * transaction is aborted or committed.
 * There can be only one transaction active at a time.
 * Reads will still return new data from the journal/main file.
 *
 * @param[in]  handle -- Handle to BlockMgr
 * @return True on Success False on failure with appropriate error set.
 * @note Journal file contents are not synced to disk until commit.
 */
uint8_t BlockMgrTransactStart(BlockMgrHandle* handle);

/**
 * \brief Aborts the current transaction.
 *
 * All changes done after the BlockMgrTransactStart will be discarded.
 * And the database will be set to its previous state.
 *
 * @param[in]  handle -- Handle to BlockMgr
 * @return True on Success False on failure with appropriate error set.
 */
uint8_t BlockMgrTransactAbort(BlockMgrHandle* handle);
/**
 * \brief Commits the current transaction to disk.
 *
 * This marks the end of the transaction all the changes will be written
 * to journal file. And journal file contents will be synced to disk.
 * If all data is synced successfully to disk all the data in the journal
 * can be replayed.
 * And data will be automatically recovered from a OS/application crash 
 * during next BlockMgrInitHandle.
 *
 * Any application/OS crash after TransactStart and before TransactCommit
 * will cause all changes to be discarded.
 *
 * @param[in]  handle -- Handle to BlockMgr
 * @return True on Success False on failure with appropriate error set.
 */
uint8_t BlockMgrTransactCommit(BlockMgrHandle* handle);


/**
 * \brief Replays the journal file contents to the main file.
 *
 * This function should be called after BlockMgrTransactCommit.
 * Any write after BlockMgrTransactCommit will also cause the
 * journal to be replayed.
 * If successfully replayed the journal file will be removed.
 * @param[in]  handle -- Handle to BlockMgr
 * @return True on Success False on failure with appropriate error set.
 */
uint8_t BlockMgrTransactReplay(BlockMgrHandle* handle);

uint32_t BlockMgrDiskPtrHashFn(const void* data,uint32_t size);
#endif
