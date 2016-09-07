#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_native.h"

static leveldb_comparator_t* Comparator;

char* go_db_get(
    leveldb_t* db,
    const leveldb_readoptions_t* options,
    const void* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  return leveldb_get(db, options, (const char*)key, keylen, vallen, errptr);
}

void go_db_put(
    leveldb_t* db,
    const leveldb_writeoptions_t* options,
    const void* key, size_t keylen,
    const void* val, size_t vallen,
    char** errptr) {
  leveldb_put(db, options, (const char*)key, keylen,
              (const char*)val, vallen, errptr);
}

void go_db_delete(
    leveldb_t* db,
    const leveldb_writeoptions_t* options,
    const void* key, size_t keylen,
    char** errptr) {
  leveldb_delete(db, options, (const char*)key, keylen, errptr);
}

void go_db_iter_seek(leveldb_iterator_t* it,
    const void* k, size_t klen) {
  leveldb_iter_seek(it, (const char*)k, klen);
}

static int compare_string_key(
  void* c,
  const char* a, size_t alen,
  const char* b, size_t blen)
{
  if ((alen < 1) || (blen < 1)) { return 0; }

  if ((alen == 1) && (blen > 1)) {
    return -1;
  }
  if ((alen > 1) && (blen == 1)) {
    return 1;
  }

  // Keys are null-terminated
  return strcmp(a + 1, b + 1);
}

static int compare_index_key(
  void* c,
  const char* a, size_t alen,
  const char* b, size_t blen)
{
  if ((alen < 1) || (blen < 1)) { return 0; }

  char* ap = (char*)a + 1;
  char* bp = (char*)b + 1;

  int cmp = strcmp(ap, bp);
  if (cmp == 0) {
    ap += strlen(ap) + 1;
    bp += strlen(bp) + 1;

    long long* lsna = (long long*)ap;
    long long* lsnb = (long long*)bp;

    if (*lsna < *lsnb) {
      return -1;
    }
    if (*lsna > *lsnb) {
      return 1;
    }

    ap += 8;
    bp += 8;

    int* indexa = (int*)ap;
    int* indexb = (int*)bp;

    if (*indexa < *indexb) {
      return -1;
    }
    if (*indexa > *indexb) {
      return 1;
    }
    return 0;
  }
  return cmp;
}

static int go_compare_bytes_impl(
  void* state,
  const char* a, size_t alen,
  const char* b, size_t blen)
{
  if ((alen < 1) || (blen < 1)) { return 0; }

  // Do something reasonable if versions do not match
  int vers1 = (a[0] >> 4) & 0xf;
  int vers2 = (b[0] >> 4) & 0xf;
  if ((vers1 != KEY_VERSION) || (vers2 != KEY_VERSION)) { return vers1 + 100; }

  // If types don't match, then just compare them
  int type1 = a[0] & 0xf;
  int type2 = b[0] & 0xf;

  if (type1 < type2) {
    return -1;
  }
  if (type1 > type2) {
    return 1;
  }

  switch (type1) {
  case STRING_KEY:
    return compare_string_key(state, a, alen, b, blen);
  case INDEX_KEY:
    return compare_index_key(state, a, alen, b, blen);
  default:
    return 999;
  }
}

int go_compare_bytes(
  void* state,
  const void* a, size_t alen,
  const void* b, size_t blen) {
    return go_compare_bytes_impl(state, a, alen, b, blen);
}

static const char* comparator_name(void* v) {
  return COMPARATOR_NAME;
}

void go_db_init() {
  Comparator = leveldb_comparator_create(
    NULL, NULL, go_compare_bytes_impl, comparator_name);
}

char* go_db_open(
  const char* directory,
  size_t cacheSize,
  GoDb** ret)
{
  leveldb_options_t* mainOptions;
  leveldb_cache_t* cache;
  char* err = NULL;
  leveldb_t* db;

  cache = leveldb_cache_create_lru(cacheSize);

  mainOptions = leveldb_options_create();
  leveldb_options_set_create_if_missing(mainOptions, 1);
  leveldb_options_set_comparator(mainOptions, Comparator);
  leveldb_options_set_cache(mainOptions, cache);

  db = leveldb_open(mainOptions, directory, &err);

  leveldb_options_destroy(mainOptions);

  if (err == NULL) {
    GoDb* h = (GoDb*)malloc(sizeof(GoDb));
    h->db = db;
    h->cache = cache;
    *ret = h;
  }

  return err;
}

void go_db_close(GoDb* h)
{
  leveldb_close(h->db);
  leveldb_cache_destroy(h->cache);
}
