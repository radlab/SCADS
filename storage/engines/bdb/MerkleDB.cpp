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
		printf("MerkleDB(): inserting root\n");
    MerkleNode root;
    root.suffix_length = 0;
    root.digest = 100;//Canary for debugging, overwritten in regular operation.
    data.data = &root;
    data.size = sizeof(MerkleNode);
    dbp->put(dbp, NULL, &key, &data, DB_NOOVERWRITE);
  } else {
    printf("MerkleDB(): found root: \n");
  }
}

//Adds key->hash(data) to pending update queue
void MerkleDB::enqueue(DBT * key, DBT * data) {
	int rdm = rand() % 1000;
	std::cout << "enqueue(\"" << std::string((char*)key->data,key->size) << "\",\"" << std::string((char*)data->data,data->size) << "\") with hash:";
	printf("%i\n", rdm);
  MerkleHash hash = (MerkleHash)(rdm); //TODO: hash(data);
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
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	pthread_mutex_lock(&sync_lock);
	//Turn pending queue into the apply queue, turn (empty) apply queue into pending queue.
	DB * tmp = aly;
	aly = pup;
	pup = tmp;
	aly->cursor(aly, NULL, &cursorp, 0);
	while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
   	insert(&key, &data);
	}
	pthread_mutex_unlock(&sync_lock);
}

//Take key,hash pair and insert into patricia-merkle trie db
void MerkleDB::insert(DBT * key, DBT * hash) {
	int ret;
	DBC *cursorp;
  dbp->cursor(dbp, NULL, &cursorp, 0);

	DBT ckey, cdata;	//key & data under cursor
	memcpy(&ckey, key, sizeof(DBT));
	memset(&cdata, 0, sizeof(DBT));

	MerkleNode m;
	memset(&m, 0, sizeof(MerkleNode));
	if (hash->size != sizeof(MerkleHash)) {
		//TODO: throw exception?
	}
	memcpy(&(m.digest), hash->data, sizeof(MerkleHash));
	m.suffix_length = 10;	//canary value
	
	DBT data;
	memset(&data, 0, sizeof(DBT));
	data.data = &m;
	data.size = sizeof(MerkleNode);
	
	dbp->put(dbp, NULL, key, &data, 0); //TODO: debug code, testing pending queue operation

	/*
	  ret = cursorp->get(cursorp, &skey, &sdata, DB_SET_RANGE);
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

DBT MerkleDB::dbtize(MerkleNode *m) {
	DBT d;
	memset(&d, 0, sizeof(DBT));
	d.data = m;
	d.size = sizeof(MerkleNode);
	return d;
}

MerkleNode * MerkleDB::merklize(DBT *dbt) {
	if (dbt->size != sizeof(MerkleNode)) {
		//TODO: Throw exception?
	}
	return (MerkleNode *)(dbt->data);
}

void MerkleDB::examine(DBT * key) {
	DBT data;
	memset(&data, 0, sizeof(DBT));
	std::cout << "examine(" << std::string((char*)key->data,key->size) << ") => ";
	int ret;
	ret = dbp->get(dbp, NULL, key, &data, 0);
	if (0 == ret) {
		MerkleNode * m = merklize(&data);
		printf("MerkleNode<digest:%i, suffix_length:%i>\n", m->digest, m->suffix_length);		
	} else if (DB_NOTFOUND == ret) {
		std::cout << "nil\n";
	} else {
		printf("error: %s\n", db_strerror(ret));
	}
}

int MerkleDB::direct_get(DB_TXN *txnid, DBT *key, DBT *data, u_int32_t flags) {
	return dbp->get(dbp, txnid, key, data, flags);
}

int main( int argc, char** argv )
{
	std::cout << "Initialize MerkleDB\n";
  MerkleDB * merkle = new MerkleDB();
	DBT key, data;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
  int ret;
	
	std::cout << "Look for root node via direct_get\n";
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.size = 0;	//root node
	ret = merkle->direct_get(NULL, &key, &data, 0);
	if (0 == ret) {
		printf("found\n");		
	} else if (DB_NOTFOUND == ret) {
		std::cout << "not found\n";
	} else {
		printf("error: %s\n", db_strerror(ret));
	}
	std::cout << "Look for root node via examine\n";
	merkle->examine(&key);
	
	
	std::cout << "Schedule update of key1 & key2\n";
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = (char *)"key1";
	key.size = 5;
	data.data = (char *)"data1";
	data.size = 5;
	merkle->enqueue(&key, &data);
	
	memset(&data, 0, sizeof(DBT));
	DBT key2;
	memset(&key2, 0, sizeof(DBT));
	key2.data = (char *)"key2";
	key2.size = 5;
	data.data = (char *)"data2";
	data.size = 5;
	merkle->enqueue(&key2, &data);
	
	
	std::cout << "Look for key1 & key2 node via examine\n";
	merkle->examine(&key);
	merkle->examine(&key2);
	
	std::cout << "flushing\n";
	merkle->flushp();
	
	std::cout << "Look for key1 and key2 node via examine\n";
	merkle->examine(&key);
	merkle->examine(&key2);
	
	
  merkle->close();
  return 0;
}
