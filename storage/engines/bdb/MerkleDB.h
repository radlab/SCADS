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
  
 public:
  void enqueue(DBT * key, DBT * data);
	void flushp();
	void examine(DBT * key);
	void close();
	int direct_get(DB_TXN *txnid, DBT *key, DBT *data, u_int32_t flags);
  MerkleDB();
	
 private:
  MerkleNode parent(MerkleNode * node);
  MerkleNode get(DBT * key);
	void insert(DBT * key, DBT * data);
	void update_hash(DBT * key, MerkleHash hash);
	
		
  DBT dbtize(MerkleNode *m);
	MerkleNode * merklize(DBT *dbt);
};

#endif // MERKLEDB_H

