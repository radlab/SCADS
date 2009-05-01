#ifndef MERKLEDB_H
#define MERKLEDB_H

#include <db.h>
#include <pthread.h>
//#include "gen-cpp/Storage.h"

typedef int MerkleHash;

typedef struct {
  int offset; //|MerkleNode.key| - |MerkleNode.parent.key| (i.e. suffix length)
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
  MerkleDB();
	//debug methods
	u_int32_t prefix_length(DBT * key1, DBT * key2);
	int direct_get(DB_TXN *txnid, DBT *key, DBT *data, u_int32_t flags);
	
 private:
  MerkleNode parent(MerkleNode * node);
  MerkleNode get(DBT * key);
	int insert(DBT * key, DBT * data);
	void update(DBT * key, MerkleHash hash);
	DBT parent(DBT * key, MerkleNode * node);
		
  DBT dbtize(MerkleNode *m);
};

#endif // MERKLEDB_H

