#ifndef MERKLEDB_H
#define MERKLEDB_H

#include <db.h>
//#include "gen-cpp/Storage.h"

typedef int MerkleHash;

typedef struct {
  int suffix_length; //|MerkleNode.key| - |MerkleNode.parent.key|
  MerkleHash digest; //hash (of data for leaf, children's digests if interior node)
} MerkleNode;

class MerkleDB {

 private:
  DB * dbp;//Merkle trie database
  DB * pup;//Pending update database
  
 private:
  void put(DBT * key, DBT * data);
	void flush();

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

