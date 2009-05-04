#ifndef MERKLEDB_H
#define MERKLEDB_H

#include <db.h>
#include <pthread.h>
#include "gen-cpp/Storage.h"

#define MERKLEDB_HASH_FUNC MHASH_TIGER128
#define MERKLEDB_HASH_WIDTH 128

//MerkleNode

using namespace std;

namespace SCADS {

typedef int MerkleHash;


typedef struct {
  int offset; //|MerkleNode.key| - |MerkleNode.parent.key| (i.e. suffix length)
  MerkleHash digest; //hash (of data for leaf, children's digests if interior node)
	MerkleHash data_digest;
} MerkleNode;

class MerkleDB {
public:
  MerkleDB(const NameSpace& ns, DB_ENV* db_env, const char* env_dir);

 public:
	int flush_flag;
	int type;
	
 private:
	pthread_mutex_t sync_lock;
  DB * dbp; //Merkle trie database
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
	
 private:
  MerkleNode parent(MerkleNode * node);
  MerkleNode get(DBT * key);
  int insert(DBT * key, MerkleHash hash);
	int recalculate(DBT * key, DBT * data, DBC * cursorp);
  DBT parent(DBT * key, MerkleNode * node);
  int dbt_equal(DBT * db1, DBT * db2);
};

}

#endif // MERKLEDB_H

