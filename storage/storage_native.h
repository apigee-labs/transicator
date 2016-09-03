#ifndef GO_LEVELDB_NATIVE_H
#define GO_LEVELDB_NATIVE_H
#endif

#include <leveldb/c.h>

/* These have to match constants in storage_convert.go.
   These go into just four bits so you only get 0-15! */
#define KEY_VERSION 1
#define STRING_KEY  1
#define TXID_KEY    5
#define INDEX_KEY   10

#define COMPARATOR_NAME "TRANSICATOR-V1"

typedef struct {
  leveldb_t* db;
  leveldb_cache_t* cache;
} GoDb;

/*
 * One-time init of comparators and stuff like that. Should be run using "once".
 */
extern void go_db_init();

/*
 * Do all the work around creating options and column families and opening
 * the database.
 */
extern char* go_db_open(
  const char* directory,
  size_t cacheSize,
  GoDb** h);

/*
 * Close the database.
 */
extern void go_db_close(GoDb* h);

/*
 * Wrapper around leveldb_get because it's a pain to cast to and from char* in
 * go code itself.
 */
extern char* go_db_get(
    leveldb_t* db,
    const leveldb_readoptions_t* options,
    const void* key, size_t keylen,
    size_t* vallen,
    char** errptr);

/* Do wrapper for leveldb_put */
extern void go_db_put(
    leveldb_t* db,
    const leveldb_writeoptions_t* options,
    const void* key, size_t keylen,
    const void* val, size_t vallen,
    char** errptr);

/* Do wrapper for leveldb_delete */
extern void go_db_delete(
    leveldb_t* db,
    const leveldb_writeoptions_t* options,
    const void* key, size_t keylen,
    char** errptr);

/* Do wrapper for leveldbdb_seek */
extern void go_db_iter_seek(leveldb_iterator_t* it,
    const void* k, size_t klen);

/*
 * Wrapper for internal comparator to facilitate testing from Go.
 */
extern int go_compare_bytes(
  void* state,
  const void* a, size_t alen,
  const void* b, size_t blen);
