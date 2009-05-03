#ifndef MERKLEDB_H
#define MERKLEDB_H

#include <db.h>
#include <pthread.h>
#include "gen-cpp/Storage.h"

using namespace std;

namespace SCADS {

typedef int MerkleHash;

typedef struct {
  int offset; //|MerkleNode.key| - |MerkleNode.parent.key| (i.e. suffix length)
  MerkleHash digest; //hash (of data for leaf, children's digests if interior node)
} MerkleNode;

class MerkleDB {
public:
  MerkleDB(const NameSpace& ns, DB_ENV* db_env, const char* env_dir);

 private:
	pthread_mutex_t sync_lock;
  DB * dbp; //Merkle trie database
  DB * pup; //Pending update database
	DB * aly; //Set of updates to apply
	DB * cld; //Secondary index (key) -> (children);
	
 public:
  int enqueue(DBT * key, DBT * data);
  void flushp();
  void examine(DBT * key);
  void close();
  //debug methods
  u_int32_t prefix_length(DBT * key1, DBT * key2);
  void print_tree();
	void print_children(DBT *key);
	
 private:
  MerkleNode parent(MerkleNode * node);
  MerkleNode get(DBT * key);
  int insert(DBT * key, MerkleHash hash);
  void update(DBT * key, MerkleHash hash);
  DBT parent(DBT * key, MerkleNode * node);
  int dbt_equal(DBT * db1, DBT * db2);
};

}

#endif // MERKLEDB_H

