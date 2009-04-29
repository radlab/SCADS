#ifndef MERKLEDB_H
#define MERKLEDB_H

#include <db.h>
//#include "gen-cpp/Storage.h"

typedef struct {
  int suffix_length; //|MerkleNode.key| - |MerkleNode.parent.key|
  int digest;//hash (of data for leaf, children's digests if interior node)
} MerkleNode;

typedef int MerkleHash;

class MerkleDB {

 private:
  DB * dbp;//Merkle trie database
  DB * pup;//Pending update database
  
 private:
  MerkleNode parent(MerkleNode * node);
  
 public:
  void schedule(DBT * key, DBT * data);
  MerkleNode get(DBT * key);
  void MerkleDB::flush();
  void toDBT(MerkleNode *m, DBT *dbt);
  void close();
  MerkleDB();

};

#endif // MERKLEDB_H

