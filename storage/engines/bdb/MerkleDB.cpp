#include "MerkleDB.h"

#include <iostream>

MerkleDB::MerkleDB() {
  //DB *dbp;//Instance variables
  //DB *pup;
  char *dbp_filename = "merkledb.db";
  char *pup_filename = "pupdb.db";
  
  /* Initialize the DB handle */
  db_create(&dbp, NULL, 0);
  db_create(&pup, NULL, 0);

  /* Now open the database */
  dbp->open(dbp, NULL, dbp_filename, NULL, DB_BTREE, DB_CREATE, 0);
  dbp->open(pup, NULL, pup_filename, NULL, DB_BTREE, DB_CREATE, 0);
  //TODO: Define sorting order on pup database, longest keys first
  //TODO: Create secondary database to give parent->children mapping
  
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

//Adds key->value pair to pending update queue
void MerkleDB::schedule(DBT * key, DBT * data) {
  MerkleHash hash = (MerkleHash)rand; //TODO: hash(data);
  DBT h;
  memset(&h, 0, sizeof(DBT));
  h.data = &hash;
  h.size = sizeof(MerkleHash);
  //pup->put(pup, NULL, &key, &h, 0);
}

//Clear out the pending update queue
void MerkleDB::flush() {
  DBC *cursorp;
  pup->cursor(pup, NULL, &cursorp, 0);
  int ret;
  DBT key, data;
  while (cursorp->get(cursorp, &key, &data, DB_NEXT)) {
    update(&key, &data);
  }
}

void MerkleDB::update(DBT * key, DBT * data) {
  int ret;
  ret = cursorp->get(cursorp, &key, &data, DB_SET_RANGE);
  if (ret == DB_NOTFOUND) {
    printf("could not find prefix\n");
    //Key shares no prefix, attaches directly to root.
    //dbp->put(dbp, NULL, &key, &data, 0);
  } else {
    printf("found %s\n", key.data);
  }
  if (cursorp != NULL) {
    cursorp->close(cursorp);
  }
  //TODO: main insertion logic
  //Add parent to pending update queue
  //schedule(&pkey, &pdata);
}

MerkleNode MerkleDB::get(DBT *key, DBT *data) {
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

void MerkleDB::toDBT(MerkleNode *m, DBT *dbt) {
  dbt.data = m;
  dbt.size = sizeof(MerkleNode);
}

int main( int argc, char** argv )
{
  MerkleDB * merkle = new MerkleDB();
  merkle->put("abc\0", 4, "abc", 3);
  merkle->get("abc", 3);
  merkle->close();
  return 0;
}
