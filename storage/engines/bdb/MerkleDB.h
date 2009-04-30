#ifndef MERKLEDB_H
#define MERKLEDB_H

#include <db.h>
#include <pthread.h>
//#include "gen-cpp/Storage.h"

typedef int MerkleHash;

typedef struct {
  int suffix_length; //|MerkleNode.key| - |MerkleNode.parent.key|
  MerkleHash digest; //hash (of data for leaf, children's digests if interior node)
} MerkleNode;

class MerkleDB {

 private:
	pthread_mutex_t sync_lock;
  DB * dbp; //Merkle trie database
  DB * pup; //Pending update database
	DB * aly;//set of updates to apply
  
 private:
  void enqueue(DBT * key, DBT * data);
	void flushp();

 public:
  MerkleNode parent(MerkleNode * node);
  MerkleNode get(DBT * key);
	void insert(DBT * key, DBT * data);
	void update_hash(DBT * key, MerkleHash hash);
  void toDBT(MerkleNode *m, DBT *dbt);
  void close();
  MerkleDB();

};

#endif // MERKLEDB_H

