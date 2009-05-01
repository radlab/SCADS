#include "MerkleDB.h"

#include <iostream>

#define dbt_equal(dp1, dp2) (((dp1)->size == (dp2)->size) and (memcmp(&((dp1)->data), &((dp2)->data), (dp1)->size) == 0))
#define dbt_string(dp) std::string((char*)(dp)->data,(dp)->size)
#define min(i1, i2) ((i1) < (i2) ? (i1) : (i2))
#define max(i1, i2) ((i1) > (i2) ? (i1) : (i2))
#define return_with_error(error) std::cout << db_strerror(error); cursorp->close(cursorp); return error;
#define return_with_success() cursorp->close(cursorp); return 0;

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
    root.offset = 0;
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
	std::cout << "enqueue(\"" << dbt_string(key) << "\",\"" << dbt_string(key) << "\") with hash:" << rdm << "\n";
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

u_int32_t MerkleDB::prefix_length(DBT * key1, DBT * key2) {
	u_int32_t max = min(key1->size, key2->size);
	int i = 0;
	while ((i < max) and (((char *)key1->data)[i] == ((char *)key2->data)[i])) {
		i++;
	}
	return i;
}

//Take key,hash pair and insert into patricia-merkle trie db
int MerkleDB::insert(DBT * key, DBT * hash) {
	std::cout << "insert(\"" << dbt_string(key) << "\", " << *((int *)hash->data) << ")\n";
	int ret;
	DBC *cursorp;
  dbp->cursor(dbp, NULL, &cursorp, 0);

	DBT ckey, cdata;	//key & data under cursor
	memcpy(&ckey, key, sizeof(DBT));
	memset(&cdata, 0, sizeof(DBT));
	
	/* Update hash entry for "key".  We may need to do some manipulations of the tree 
	 * so we use cursors to get as close as possible. 
	 */
	ret = cursorp->get(cursorp, &ckey, &cdata, DB_SET_RANGE);
	if (0 == ret) {	//success
		if (dbt_equal(key, &ckey)) {
			/* A node for this exact key exists.  Update it and we're done */
			//TODO: update(&ckey, &m.digest)
			//TODO: add parent to pending (with data with length zero)
			return_with_success();
		}
		
		/* 	We're inserting a new node, so we need to find out where this node belongs
		*		(i.e. who his parent is going to be)  The parent of our node is the longest
		*		common prefix of the elements that sort lexically on either side of the key.
		*/
		DBT leftk, rightk;
		memset(&leftk, 0, sizeof(DBT));
		memset(&rightk, 0, sizeof(DBT));
		rightk = ckey;
		ret = cursorp->get(cursorp, &ckey, &cdata, DB_PREV);
		if (DB_NOTFOUND == ret) {
			//We are the leftmost key, so we can't share a prefix on that side.
			leftk.size = 0;
		} else if (0 == ret) { //succeeded
			leftk = ckey;
		} else {
			return_with_error(ret);
		}
	
		/*	At this point, we have the prefixes we share with our left and right neighbors.
		* 	The longest prefix is our parent node (this parent node may or may not yet exist)
		*   (side effect of patricia trie and lexical sorting)
		*/
		u_int32_t prefixl = max(prefix_length(key, &leftk), prefix_length(key, &rightk));
		DBT parentk;
		memset(&parentk, 0, sizeof(DBT));
		parentk.data = key->data;
		parentk.size = prefixl; //Take inserted key, truncate to prefix_length

		ckey.data = parentk.data;
		ckey.size = parentk.size;
		
		/* Let's see if our parent already exists.  If not, we'll create it. */
		ret = cursorp->get(cursorp, &ckey, &cdata, DB_SET_RANGE);
		if (0 == ret) {
			if (!dbt_equal(&ckey, &parentk)) {
				/* Parent node doesn't exist, so create it */
				DBT parentd;
				MerkleNode parentn;
				memset(&parentd, 0, sizeof(DBT));
				memset(&parentn, 0, sizeof(MerkleNode));
				parentd.data = &parentn;
				parentd.size = sizeof(MerkleNode);
				
				/*	Since our parent doesn't exist (as a node) and the empty string represents the root
				*		There's some edge that our parent falls on (since we're a patricia trie).  Thus we
				*   need to split an edge.  At this point, the cursor is conveniently pointed at the
				*		node whose up-edge we're splitting.  This node will be a child of our new parent
				*		and his old parent will be our parent's parent.
				*/
				MerkleNode * sibling = (MerkleNode *)(&cdata.data);
				DBT grandparentk = parent(&ckey, sibling);
				int parentk_suffix_len = grandparentk.size - parentk.size;
					
				sibling->offset = (sibling->offset - parentk_suffix_len);
				ret = cursorp->put(cursorp, &ckey, &cdata, DB_CURRENT); //Sibling is under cursor.
				return_with_error(ret);
				
				parentn.offset = parentk_suffix_len;
				ret = cursorp->put(cursorp, &parentk, &parentd, 0);
				return_with_error(ret);
			}
			/* Our parent is guaranteed to exist now. (Parent node either existed, or we created it)  */
			MerkleNode * selfn = (MerkleNode *)(&cdata.data); //.size of data items are always sizeof(MerkleNode)
			selfn->offset = key->size - parentk.size;
			ckey.data = key->data;
			ckey.size = key->size;
			ret = cursorp->put(cursorp, &ckey, &cdata, 0);
			return_with_error(ret);
		} else if (DB_NOTFOUND == ret) {	//impossible
			std::cerr << "Inconceivable!\n";
			exit(1);
		} else {
			return_with_error(ret);
		}
	} else if (DB_NOTFOUND == ret) {
			std::cerr << "Inconceivable!\n";
			exit(1);	
	} else {
		return_with_error(ret);
	}
	
	
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
void MerkleDB::update(DBT * key, MerkleHash hash) {
	//TODO: implement
	//cursorp->put(...., DB_CURRENT)
	std::cout << "update(" << dbt_string(key) << ", " << hash << ") //update hash\n";
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

DBT MerkleDB::parent(DBT * key, MerkleNode * node) {
	DBT parentk;
	memset(&parentk, 0, sizeof(DBT));
	parentk.data = key->data;
	parentk.size = (key->size) - (node->offset);
	return parentk;
}

DBT MerkleDB::dbtize(MerkleNode *m) {
	DBT d;
	memset(&d, 0, sizeof(DBT));
	d.data = m;
	d.size = sizeof(MerkleNode);
	return d;
}


void MerkleDB::examine(DBT * key) {
	DBT data;
	memset(&data, 0, sizeof(DBT));
	std::cout << "examine(" << dbt_string(key) << ") => ";
	int ret;
	ret = dbp->get(dbp, NULL, key, &data, 0);
	if (0 == ret) {
		MerkleNode * m =  (MerkleNode *)((&data)->data);;
		printf("MerkleNode<digest:%i, offset:%i>\n", m->digest, m->offset);		
	} else if (DB_NOTFOUND == ret) {
		std::cout << "nil\n";
	} else {
		printf("error: %s\n", db_strerror(ret));
	}
}

int MerkleDB::direct_get(DB_TXN *txnid, DBT *key, DBT *data, u_int32_t flags) {
	return dbp->get(dbp, txnid, key, data, flags);
}

int test_macro() {
	DBT db1, db2, db3, db4;
		
	memset(&db1, 0, sizeof(DBT));
	memset(&db2, 0, sizeof(DBT));
	memset(&db3, 0, sizeof(DBT));
	memset(&db4, 0, sizeof(DBT));
	
	db1.data = (char *)"value1";
	db1.size = 6;
	
	db2.data = (char *)"value2";
	db2.size = 6;
	
	db3.data = (char *)"value1";
	db3.size = 5;
	
	db4.data = (char *)"value1";
	db4.size = 6;
	
	if (dbt_equal(&db1, &db2) || dbt_equal(&db1,&db3) || !dbt_equal(&db1,&db4)) {
		printf("Failed test_macro\n");
	} else {
		printf("Succeeded test_macro\n");
	}
}

int test_pending() {
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
	
}


int test_prefix() {
	MerkleDB * merkle = new MerkleDB();
	DBT db1, db2, db3, db4,db5;
		
	memset(&db1, 0, sizeof(DBT));
	memset(&db2, 0, sizeof(DBT));
	memset(&db3, 0, sizeof(DBT));
	memset(&db4, 0, sizeof(DBT));
	memset(&db5, 0, sizeof(DBT));
		
	db1.data = (char *)"";
	db1.size = 0;
	
	db2.data = (char *)"abcdefg";
	db2.size = 7;
	
	db3.data = (char *)"abczzz";
	db3.size = 6;
	
	db4.data = (char *)"abcdefghijklmn";
	db4.size = 14;
	
	db5.data = (char *)"zzzzzz";
	db5.size = 6;
		
	std::cout << merkle->prefix_length(&db1, &db2)<<"\n";
	std::cout << merkle->prefix_length(&db2, &db3)<<"\n";
	std::cout << merkle->prefix_length(&db2, &db4)<<"\n";
	std::cout << merkle->prefix_length(&db2, &db5)<<"\n";
}
int main( int argc, char** argv )
{
	test_macro();
	test_pending();
	//test_prefix();
	return 0;
}
