#include "MerkleDB.h"

#include <iostream>

MerkleDB::MerkleDB() {
	//TODO: We'll need a different merkledb for each namespace.  Ditto for the pending queue (unless we put more info in struct)
  char *dbp_filename = "merkledb.db";
  char *pup_filename = "pupdb.db";
	char *aly_filename = "alydb.db";
  
  /* Initialize the DB handles */
  db_create(&dbp, NULL, 0);
  db_create(&pup, NULL, 0);
	db_create(&aly, NULL, 0);

  /* Now open the databases */
  dbp->open(dbp, NULL, dbp_filename, NULL, DB_BTREE, DB_CREATE, 0);
  pup->open(pup, NULL, pup_filename, NULL, DB_BTREE, DB_CREATE, 0);
	aly->open(aly, NULL, aly_filename, NULL, DB_BTREE, DB_CREATE, 0);
  //TODO: Define sorting order on aly & pup database, longest keys first
  //TODO: Create secondary database to give parent->children mapping

	pthread_mutex_init(&sync_lock, NULL);

  /** Create root node for dbp, if it doesn't exist **/
  DBT key, data;
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  key.size = 0;//Not necessary (since already memset to zero), but let's be explicit
  
  int ret;
  ret = dbp->get(dbp, NULL, &key, &data, 0);
  if (ret == DB_NOTFOUND) {
    MerkleNode root;
    root.suffix_length = 0;
    root.digest = 100;//Canary for debugging, overwritten in regular operation.
    data.data = &root;
    data.size = sizeof(MerkleNode);
    dbp->put(dbp, NULL, &key, &data, DB_NOOVERWRITE);
  } else {
    printf("found root: \n");
  }
}

//Adds key->hash(data) to pending update queue
void MerkleDB::put(DBT * key, DBT * data) {
  MerkleHash hash = (MerkleHash)rand; //TODO: hash(data);
  DBT h;
  memset(&h, 0, sizeof(DBT));
  h.data = &hash;
  h.size = sizeof(MerkleHash);
  pup->put(pup, NULL, key, &h, 0);
}

//Clear the pending update queue
void MerkleDB::flushp() {
  DBC *cursorp;
  int ret;
  DBT key, data;
	pthread_mutex_lock(&sync_lock);
	//Turn pending queue into the apply queue, turn (empty) apply queue into pending queue.
	DB * tmp = aly;
	apply = pup;
	pup = tmp;
	aly->cursor(aly, NULL, &cursorp, 0);
	while (cursorp->get(cursorp, &key, &data, DB_NEXT)) {  //TODO: proper check of ret code
   	insert(&key, &data);
	}
	pthread_mutex_unlock(&sync_lock);
}

//Take key,hash pair and insert into patricia-merkle trie db
void MerkleDB::insert(DBT * key, DBT * mnode) {
	int ret;
	DBC *cursorp;
  dbp->cursor(dbp, NULL, &cursorp, 0);

	DBT skey, sdata;
	memcpy(&skey, key, sizeof(DBT));
	memset(&sdata, 0, sizeof(DBT));

  ret = cursorp->get(cursorp, &skey, &sdata, DB_SET_RANGE);
	/*
		if node exists
			update it's hash
			add it's parent to pending
		else
			make a new node
			find it's longest common prefix, this is its parent
			
			if the parent node isn't in the db, then we have to split an edge {
				find the longest prefix for which a node does exist, this is the old parent
				make a node for the (new) parent
				make the old parent node the parent of the new parent node
				find the (solitary) child of the old parent node, make the new parent it's parent
			}
			set the new nodes parent
			add the new nodes parent to pending
		end
	*/
}

//hashes the supplied value with the hashes of the children on key
void MerkleDB::update_hash(DBT * key, MerkleHash hash) {
	;
}

void MerkleDB::close() {
  if (dbp != NULL) {
    dbp->close(dbp, 0);
  } else {
    printf("dbp is null");
  }
  if (pup != NULL) {
    pup->close(pup, 0);
  } else {
    printf("pup is null");
  }
}

void MerkleDB::toDBT(MerkleNode * m, DBT *dbt) {
	dbt->data = m;
  dbt->size = sizeof(MerkleNode);
}

int main( int argc, char** argv )
{
  MerkleDB * merkle = new MerkleDB();
  merkle->close();
  return 0;
}
