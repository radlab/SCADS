#include "MerkleDB.h"

#include <iostream>

#define dbt_equal(dp1, dp2) (((dp1)->size == (dp2)->size) and (memcmp(&((dp1)->data), &((dp2)->data), (dp1)->size) == 0))
#define dbt_string(dp) std::string((char*)(dp)->data,(dp)->size)
#define min(i1, i2) ((i1) < (i2) ? (i1) : (i2))
#define max(i1, i2) ((i1) > (i2) ? (i1) : (i2))
#define return_with_error(error) std::cout << db_strerror(error) << "\n"; cursorp->close(cursorp); return error;
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
	std::cout << "enqueue(\"" << dbt_string(key) << "\",\"" << dbt_string(data) << "\") with hash:" << rdm << "\n";
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
   	insert(&key, *((MerkleHash *)(data.data)));
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
int MerkleDB::insert(DBT * key, MerkleHash hash) {
	std::cout << "insert(\"" << dbt_string(key) << "\", " << hash << ")\n";
	int ret;
	DBC *cursorp;
  dbp->cursor(dbp, NULL, &cursorp, 0);

	DBT ckey, cdata;	//key & data under cursor
	memcpy(&ckey, key, sizeof(DBT));
	memset(&cdata, 0, sizeof(DBT));
	
	DBT leftk, rightk;	//keys to the left and right (lexically) of the input key
	memset(&leftk, 0, sizeof(DBT));
	memset(&rightk, 0, sizeof(DBT));
	
	//position the cursor at the key (or to the right of the key, if first insertion)
	ret = cursorp->get(cursorp, &ckey, &cdata, DB_SET_RANGE);
	if (ret == 0) {
		if (dbt_equal(key, &ckey)) {
			/* A node for this exact key exists.  Update it and we're done */
			//TODO: update(&ckey, &m.digest)
			//TODO: add parent to pending (with data with length zero)
			return_with_success();
		} else {
			//Key doesn't exist, cursor is now at key on right-hand side of insertion point.
			rightk = ckey;
		}
	} else if (DB_NOTFOUND == ret) {
		//key is all the way to the right, no right-side neighbor
		rightk.size = 0;
	} else {
		return_with_error(ret);
	}
	
	/*	No exact match exists (or we would have returned from this procedure call already),
	* 	so we're inserting a new node.  We need to find out where this node belongs (i.e. who
	* 	his parent is going to be)  We're constructing a patricia trie, so the parent of the
	*   new key is the longest prefix it shares with it's right & left-hand neighbors. (exercise
	*   left to reader) At this point, we have the right neighbors, so we need the left neighbor
	*   to calculate where our new node integrates into the tree. 
	*/
	
	//We're going to have to create (at least) on additional node, so set up data structures for them
	DBT newd;
	MerkleNode newn;
	memset(&newd, 0, sizeof(DBT));
	memset(&newn, 0, sizeof(MerkleNode));
	newd.size = sizeof(MerkleNode);
	
	//Find the left-side neighbor
	ret = cursorp->get(cursorp, &ckey, &cdata, DB_PREV);
	if (DB_NOTFOUND == ret) {
		std::cerr << "Inconceivable! The root node sorts first! It should be here!\n";
		exit(1);
	} else if (0 == ret) {
		leftk = ckey;
	} else {
		return_with_error(ret);
	}
	
	//The longest prefix our new node shares with it's neighbors will be its new parent
	u_int32_t prefixl = max(prefix_length(key, &leftk), prefix_length(key, &rightk));

	//prefixl may be equal to length of the key.  In this case, the inserted node 
	//(conceptually) has an "empty string" edge from its parent.  As an optimization,
	//in this case, we just merge the data of the child with the parent and don't
	//actually create a distinct node for the child.
	DBT parentk;
	memset(&parentk, 0, sizeof(DBT));
	parentk.data = key->data;
	parentk.size = prefixl; //Take inserted key, truncate to prefix_length 
	
	/* configure the cursor to see if the parent node exists (or position ourselves to create it) */
	ckey.data = parentk.data;
	ckey.size = parentk.size;
	ret = cursorp->get(cursorp, &ckey, &cdata, DB_SET_RANGE);
	if (0 == ret) {
		//check if parent node exists
		if (not dbt_equal(&ckey, &parentk)) { 
			// No node exists, so we have to split an edge.  Conveniently, the node
			// immediately after our parent is the child whose parent edge we need
			// to split (he'll become our new sibling) and the cursor is now pointing at him.
			// Since we're already using the word "parent," we'll call our siblings' parent
			// "estranged_parent"
			DBT estranged_parentk = parent(&ckey, (MerkleNode *)(cdata.data));
			
			//TODO: Consider the impact of the order of these pointer updates on the "children_of(key)" index 
			
			//Need to update our siblings parent pointer
			(((MerkleNode *)(cdata.data))->offset) = ckey.size - parentk.size;
			ret = cursorp->put(cursorp, &ckey, &cdata, DB_CURRENT);
			if (ret != 0) { return_with_error(ret); }
			
			//Now we need to insert the node that splits the edge
			ckey.data = parentk.data;
			ckey.size = parentk.size;
			cdata.data = &newn;
			cdata.size = sizeof(MerkleNode);
			newn.offset = parentk.size - estranged_parentk.size;
			//We don't need to set the hash here, since that will happen naturally when we start walking up the tree
			ret = cursorp->put(cursorp, &ckey, &cdata, DB_KEYFIRST);
			if (ret != 0) { return_with_error(ret); }
		}
		/* parent is guaranteed to exist now. (Parent node either existed, or we created it)  */
		if (parentk.size != key->size) {
			//The inserted node has a non-empty string suffix relative to it's common prefix, so we
			//need to add a node for it.
			ckey.data = key->data;
			ckey.size = key->size;
			cdata.data = &newn;
			memset(&newn, 0, sizeof(MerkleNode));
			newn.offset = key->size - parentk.size;
			newn.digest = hash;
			ret = cursorp->put(cursorp, &ckey, &cdata, DB_KEYFIRST);
			if (ret != 0) { return_with_error(ret); }
		}
		//update_hash(key)
		//add parent to apply_set
	} else if (DB_NOTFOUND == ret) {
		//Should be impossible, our parent is a prefix of us and if we can't find it directly, there
		//must be some key containing it as a sole prefix (which would lexically sort after it).
		std::cerr << "Inconceivable!\n";
		exit(1);
	} else {
		return_with_error(ret);
	}
	return_with_success();
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
void MerkleDB::print_tree() {
	DBC *cursorp;
	DBT key, data;
	int ret;

	/* Get a cursor */
	dbp->cursor(dbp, NULL, &cursorp, 0); 

	/* Initialize our DBTs. */
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	/* Iterate over the database, retrieving each record in turn. */
	MerkleNode * mn;
	int i;
	while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
		int suffix = ((MerkleNode *)data.data)->offset;
		int prefix = key.size - suffix;
		for (i = 0; i < prefix; i++) {
			std::cout << " ";
		}
		char * sstart = ((char *)(key.data)+prefix);
		std::cout << std::string(sstart,suffix) << "\n";
	}
	if (ret != DB_NOTFOUND) {
		std::cout << "DONE\n";
	}

	/* Cursors must be closed */
	if (cursorp != NULL) {
	    cursorp->close(cursorp);
	}
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
	DBT key, key2, data;
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
	memset(&key2, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = (char *)"key1";
	key.size = 4;
	data.data = (char *)"data1";
	data.size = 5;
	merkle->enqueue(&key, &data);

	memset(&data, 0, sizeof(DBT));
	key2.data = (char *)"key2";
	key2.size = 4;
	data.data = (char *)"data2";
	data.size = 5;
	merkle->enqueue(&key2, &data);
	
	data.data = (char *)"data3";
	data.size = 5;
	merkle->enqueue(&key, &data);
	
	std::cout << "Look for keys via examine\n";
	merkle->examine(&key);
	merkle->examine(&key2);
	
	std::cout << "flushing\n";
	merkle->flushp();
	
	std::cout << "Look for keys node via examine\n";
	merkle->examine(&key);
	merkle->examine(&key2);
	
	data.data = (char *)"data4";
	data.size = 5;
	merkle->enqueue(&key, &data);
	
	std::cout << "flushing\n";
	merkle->flushp();
	
	std::cout << "Look for keys node via examine\n";
	merkle->examine(&key);
	merkle->examine(&key2);
	
	merkle->print_tree();
	
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
