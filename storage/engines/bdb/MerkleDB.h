#ifndef MERKLEDB_H
#define MERKLEDB_H

#include <db.h>
#include <pthread.h>
#include "gen-cpp/StorageEngine.h"
#include "TQueue.h"

#define MERKLEDB_HASH_FUNC MHASH_TIGER128
#define MERKLEDB_HASH_WIDTH 128

#define is_leaf(keyd) ((keyd)->size > 0 and (((char *)(keyd)->data)[(keyd)->size - 1]) == 0)

//MerkleNode

using namespace std;

namespace SCADS {

  typedef int MerkleHash;

  typedef struct {
    int offset; //|MerkleNode.key| - |MerkleNode.parent.key| (i.e. suffix length)
    MerkleHash digest; //hash (of data for leaf, children's digests if interior node)
  } MerkleNode;

  class MerkleDB {
  public:
    MerkleDB(const NameSpace& ns, DB_ENV* db_env);

  public:
    int flush_flag;
    DB * dbp; //Merkle trie database
    static DB * qdb; // queue database for running syncs

  private:
    pthread_mutex_t sync_lock;
    DB * pup; //Pending update database
    DB * aly; //Set of updates to apply
    DB * cld; //Secondary index (key) -> (children);

  public:
    int enqueue(DBT * key, DBT * data);
    int flushp();
    void examine(DBT * key);
    void close();
    //debug methods
    u_int32_t prefix_length(DBT * key1, DBT * key2);
    void print_tree();
    void print_children(DBT *key);
    int dbt_cmp(DBT * db1, DBT * db2);
    void queue_children(DBT *key,TQueue<DBT>*);
    void apply_children(DBT *key,
			void(*to_apply)(void*,void*,void*),
			void* apply_arg);

  private:
    MerkleNode parent(MerkleNode * node);
    MerkleNode get(DBT * key);
    int insert(DBT * key, MerkleHash hash);
    int recalculate(DBT * key, DBT * data, DBC * cursorp);
    DBT parent(DBT * key, MerkleNode * node);
  };

}

#endif // MERKLEDB_H
