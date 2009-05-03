// MerkleDB File

#include "MerkleDB.h"

#include <iostream>
#include <string.h>
#include <stdlib.h>
#include "mhash.h"

#define dbt_string(dbt) std::string((char*)(dbt)->data,(dbt)->size)
#define min(i1, i2) ((i1) < (i2) ? (i1) : (i2))
#define max(i1, i2) ((i1) > (i2) ? (i1) : (i2))
#define return_with_error(error) std::cout << db_strerror(error) << "\n"; return error;
#define return_with_success() return 0;
#define close_if_not_null(db) if ((db) != NULL) { (db)->close((db), 0); }
#define cleanup_after_bdb() cursorp->close(cursorp); while (data_ptrs.size() > 0) { free(data_ptrs.back()); data_ptrs.pop_back(); }
#define print_hex(buf, len) for (int i = 0; i < (len); i++) { printf("%x%x", (0xF0 & (((char *)buf)[i]) >> 4), (0x0F & (((char *)buf)[i]))); }

using namespace std;
using namespace SCADS;

int cmp_longest_first(DB *dbp, const DBT *a, const DBT *b) {
  if (a->size > b->size) {
    return -1;
  } else if (a->size == b->size) {
		return memcmp(a->data, b->data, a->size);
  } else {
    return 1;
  }
}

//Used by children index.  Each node add a pointer from its parent to itself
//TODO: Need to make sure tombstoned nodes aren't included in hashing! 
int child_extractor(DB *dbp, const DBT *pkey, const DBT *pdata, DBT *ikey) {
	if (pkey->size != 0) {
		//memset(ikey, 0, sizeof(DBT));
		// TODO: Example code does above, even though ikey is parameter.  Unclear how this'll work with 
		// multi-threaded environment (i.e. DB_DBT_MALLOC).  *Seems* to work without it.
		//		http://www.oracle.com/technology/documentation/berkeley-db/db/gsg/C/keyCreator.html
		ikey->data = pkey->data;
		ikey->size = (pkey->size - ((MerkleNode *)pdata->data)->offset);
		return 0;
	} else {
		//make sure root node (who is own parent) doesn't add child link to itself 
		return DB_DONOTINDEX;	
	}
}

//TODO: Write destructor that closes db

MerkleDB::MerkleDB(const string& ns, DB_ENV* db_env, const char* env_dir) {
  //TODO: We'll need a different merkledb for each namespace.  Ditto for the pending queue (unless we put more info in struct)
  char filebuf[10+ns.length()];
  int ret;
	
  // open the tree env
  u_int32_t env_flags = 0;
  u_int32_t gb;
  DB_ENV* tree_env;

  ret = db_env_create(&tree_env, 0);
  if (ret != 0) {
    fprintf(stderr, "Error creating tree_env handle: %s\n", db_strerror(ret));
    exit(-1);
  }

  env_flags = 
    DB_CREATE |     /* If the environment does not exist, create it. */
    DB_INIT_MPOOL|  /* Initialize the in-memory cache. */
    DB_PRIVATE;
  
  ret = db_env->open(tree_env,   /* DB_ENV ptr */
		     env_dir,    /* env home directory */
		     env_flags,  /* Open flags */
		     0);         /* File mode (default) */
  if (ret != 0) {
    fprintf(stderr, "Environment open failed: %s\n", db_strerror(ret));
    exit(-1);
  }

  /* Initialize the DB handles */
  //TODO: Add error checking here and elsewhere
  db_create(&dbp, tree_env, 0);
  db_create(&pup, db_env, 0);
  db_create(&aly, db_env, 0);

  //Create index (secondary database) to give parent->children mapping
  db_create(&cld, tree_env, 0);
  cld->set_flags(cld, DB_DUPSORT);

  //Set sorting functions for queues
  pup->set_bt_compare(pup, cmp_longest_first);
  aly->set_bt_compare(aly, cmp_longest_first);

  /* Now open the databases */
  sprintf(filebuf,"%s_merkledb.bdb",ns.c_str());
  dbp->open(dbp, NULL, filebuf, NULL, DB_BTREE, DB_CREATE, 0);
  sprintf(filebuf,"%s_pendingq.bdb",ns.c_str());
  pup->open(pup, NULL, filebuf, NULL, DB_BTREE, DB_CREATE | DB_THREAD, 0);
  sprintf(filebuf,"%s_applyq.bdb",ns.c_str());
  aly->open(aly, NULL, filebuf, NULL, DB_BTREE, DB_CREATE | DB_THREAD, 0);
  sprintf(filebuf, "%s_chldix.bdb", ns.c_str());
	//Create child index
  cld->open(cld, NULL, filebuf, NULL, DB_BTREE, DB_CREATE, 0);
  dbp->associate(dbp, NULL, cld, child_extractor, 0);
	
  pthread_mutex_init(&sync_lock, NULL);

  /** Create root node for dbp, if it doesn't exist **/
  DBT key, data;
  memset(&key, 0, sizeof(DBT));
	key.flags = DB_DBT_MALLOC;
  memset(&data, 0, sizeof(DBT));
	data.flags = DB_DBT_MALLOC;
  key.size = 0;//Not necessary (since already memset to zero), but let's be explicit
  
  ret = dbp->get(dbp, NULL, &key, &data, 0);
  if (ret == DB_NOTFOUND) {
    MerkleNode root;
    root.offset = 0;
    root.digest = 0;//Canary for debugging, overwritten in regular operation.
    data.data = &root;
    data.size = sizeof(MerkleNode);
    dbp->put(dbp, NULL, &key, &data, DB_NOOVERWRITE);
  } else {
  	free(key.data);
		free(data.data);
	}
}

//Adds key->hash(data) to pending update queue
int MerkleDB::enqueue(DBT * key, DBT * data) {
  int ret;
  DBT h;
  MHASH td;
	
	memset(&h, 0, sizeof(DBT));
	//h.flags = DB_DBT_MALLOC; //We're not currently 'get()-ing' here
	
	td = mhash_init(MERKLEDB_HASH_FUNC);
	if (td == MHASH_FAILED) {
		std::cerr << "HASH Failed";
		return -1;
	}
	mhash(td, (unsigned char *)key->data, key->size);
	mhash(td, (unsigned char *)data->data, data->size);
	//TODO: use mhash_end instead.  Not clear if we need to 'free' mhash_end, so playing safe.
	h.data = malloc(MERKLEDB_HASH_WIDTH / 8);
	mhash_deinit(td, h.data);	

	//TODO: clean this up, don't need to be passing MerkleHashs all over and we should be using the full hash
	h.size = sizeof(MerkleHash);
  MerkleHash hash = *((MerkleHash *)(h.data));
  std::cout << "enqueue(\"" << dbt_string(key) << "\",\"" << dbt_string(data) << "\") with hash:\t";
	print_hex(h.data, h.size);
	std::cout << "\n";
	
	ret = pup->put(pup, NULL, key, &h, 0);
	free(h.data);
	if (ret != 0) { return_with_error(ret); }
	return_with_success();
}

//Clear the pending update queue
void MerkleDB::flushp() {
  DBC *cursorp;
  int ret;
  DBT key, data;
  memset(&key, 0, sizeof(DBT));
	key.flags = DB_DBT_MALLOC;
  memset(&data, 0, sizeof(DBT));
	data.flags = DB_DBT_MALLOC;
  pthread_mutex_lock(&sync_lock);
  //Turn pending queue into the apply queue, turn (empty) apply queue into pending queue.
  DB * tmp = aly;
  aly = pup;
  pup = tmp;
  aly->cursor(aly, NULL, &cursorp, 0);
  while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    insert(&key, *((MerkleHash *)(data.data)));
		free(key.data);
		free(data.data);
    print_tree();
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
	
	//This code is only executed by a single thread, but BDB requires us
	//to be thread-safe since we're in a multi-threaded environment, so we
	//need to keep track of all the data buffers cursor->get returns to us
	vector<void *> data_ptrs;

  DBC *cursorp;
  dbp->cursor(dbp, NULL, &cursorp, 0);

  DBT ckey, cdata;	//key & data under cursor
  memcpy(&ckey, key, sizeof(DBT));
	ckey.flags = DB_DBT_MALLOC;
  memset(&cdata, 0, sizeof(DBT));
	cdata.flags = DB_DBT_MALLOC;
	
  DBT leftk, rightk;	//keys to the left and right (lexically) of the input key
  memset(&leftk, 0, sizeof(DBT));
	leftk.flags = DB_DBT_MALLOC;
  memset(&rightk, 0, sizeof(DBT));
	rightk.flags = DB_DBT_MALLOC;
	
	//TODO: Check code coverage during tests
  //position the cursor at the key (or to the right of the key, if first insertion)
  ret = cursorp->get(cursorp, &ckey, &cdata, DB_SET_RANGE);
  if (ret == 0) {
		data_ptrs.push_back(ckey.data);
		data_ptrs.push_back(cdata.data);
    
		if (dbt_equal(key, &ckey)) {
      /* A node for this exact key exists.  Update it and we're done */
			recalculate(key, &cdata, hash, cursorp);
      //TODO: update(&ckey, &m.digest...?)
      //TODO: add parent to pending (with data with length zero)
			cleanup_after_bdb(); 
      return_with_success();
    } else {
      //Key doesn't exist, cursor is now at key on right-hand side of insertion point.
      rightk = ckey;
    }
  } else if (DB_NOTFOUND == ret) {
    //key is all the way to the right, no right-side neighbor
    rightk.size = 0;
  } else {
		cleanup_after_bdb();
    return_with_error(ret);
  }
	
  /*	No exact match exists (or we would have returned from this procedure call already),
   * 	so we're inserting a new node.  We need to find out where this node belongs (i.e. who
   * 	his parent is going to be)  We're constructing a patricia trie, so the parent of the
   *   new key is the longest prefix it shares with it's right & left-hand neighbors. (exercise
   *   left to reader) At this point, we have the right neighbor, so we need the left neighbor. 
   */
	
  //We're going to have to create (at least) on additional node, so set up data structures for them
  DBT newd;
  MerkleNode newn;
  memset(&newd, 0, sizeof(DBT));
	newd.flags = DB_DBT_MALLOC;
  memset(&newn, 0, sizeof(MerkleNode));
  newd.size = sizeof(MerkleNode);
	
  //Find the left-side neighbor
  ret = cursorp->get(cursorp, &ckey, &cdata, DB_PREV);
  if (DB_NOTFOUND == ret) {
    std::cerr << "Inconceivable! The root node sorts first! It should be here!\n";
		return_with_error(ret);
  } else if (0 == ret) {
		data_ptrs.push_back(ckey.data);
		data_ptrs.push_back(cdata.data);
    leftk = ckey;
  } else {
		cleanup_after_bdb();
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
	parentk.flags = DB_DBT_MALLOC;
  parentk.data = key->data;
  parentk.size = prefixl; //Take inserted key, truncate to prefix_length 
	
  /* configure the cursor to see if the parent node exists (or position ourselves to create it) */
  ckey.data = parentk.data;
  ckey.size = parentk.size;
  ret = cursorp->get(cursorp, &ckey, &cdata, DB_SET_RANGE);
  if (0 == ret) {
		data_ptrs.push_back(ckey.data);
		data_ptrs.push_back(cdata.data);
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
		/* We just need to check if we should coalesce with our parent, or actually add ourselves */
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
		//Now we reposition the cursor, and update the new nodes hash
		ret = cursorp->get(cursorp, key, &cdata, DB_SET);
    if (ret != 0) { return_with_error(ret); }
		recalculate(key, &cdata, hash, cursorp);
  } else {
		if (ret == DB_NOTFOUND) {
			//Should be impossible, our parent is a prefix of us and if we can't find it directly, there
	    //must be some key containing it as a sole prefix (which would lexically sort after it).
			std::cerr << "Inconceivable!\n";
		}
		cleanup_after_bdb();
    return_with_error(ret);
  }
	cleanup_after_bdb();
  return_with_success();
}

//rehash the targeted key and 
int MerkleDB::recalculate(DBT * key, DBT * data, MerkleHash hash, DBC * cursorp) {
	int ret;
	MHASH td;
	
	/* Data structures for key's children */
	DBT childk, childd; /* Used to return the primary key and data */
	memset(&childk, 0, sizeof(DBT));
	memset(&childd, 0, sizeof(DBT));
	childk.flags = DB_DBT_MALLOC;
	childd.flags = DB_DBT_MALLOC;

	//Setup the hash function
	td = mhash_init(MERKLEDB_HASH_FUNC);
	if (td == MHASH_FAILED) {
		std::cerr << "HASH Failed";
		return -1;	//TODO: We're returning error codes from both mhash and bdb...
	}
	
	//Hash in the data for this node (and it's hash, if necessary)
	mhash(td, (unsigned char *)key->data, key->size);
	if (hash != NULL) {
		mhash(td, &hash, sizeof(MerkleHash));	//TODO: Switch away from MerkleHash
	}
	
	//Hash in the children (in lexical order) //TODO: Confirm lexical ordering
	DBC *childc;
	cld->cursor(cld, NULL, &childc, 0);
	MerkleNode * childn;
	ret = childc->pget(childc, key, &childk, &childd, DB_SET);
	if (ret == 0) {
		do {
			mhash(td, (unsigned char *)childk.data, childk.size);
			childn = (MerkleNode *)childd.data;
			mhash(td, (unsigned char *)&(childn->digest), sizeof(MerkleHash));
			free(childk.data);
			free(childd.data);
		} while ((ret = childc->pget(childc, key, &childk, &childd, DB_NEXT_DUP)) == 0);
	}
	childc->close(childc);
	
	void * digest = malloc(MERKLEDB_HASH_WIDTH / 8);
	//TODO: use mhash_end instead.  Not clear if we need to 'free' mhash_end, so playing safe.
	mhash_deinit(td, digest);
	MerkleNode * mn = (MerkleNode *)data->data; //TODO: Note, we're changing the state of the data DBT passed in, icky.
	mn->digest = *((int *)digest);	//TODO: We're only using a little bit of the hash.. decide which way to go.
	free(digest);
	
	//Put back into the main database
	ret = cursorp->put(cursorp, key, data, DB_KEYFIRST);
	if (ret != 0) { return_with_error(ret); }
	
	//Now put this nodes parent into the apply queue to recurse up the tree
	//TODO: Following code is hanging on aly->put (since we've already got an open cursor in flushp?)
	/*DBT parentk;
	if (key->size > 0) {
		parentk = parent(key, mn);
		std::cout << "putting: " << dbt_string(&parentk) << "\n";
		ret = aly->put(aly, NULL, &parentk, data, DB_NOOVERWRITE);
		if (ret != 0) { return_with_error(ret); } 
	}
	*/
	
  //TODO: implement has and add parent to pending queue
  std::cout << "update(" << dbt_string(key) << ", ";
	print_hex(&hash, sizeof(MerkleHash));

	return_with_success();
}

void MerkleDB::print_tree() {
  DBC *cursorp;
  DBT key, data;
  int ret;

  /* Get a cursor */
  dbp->cursor(dbp, NULL, &cursorp, 0); 

  /* Initialize our DBTs. */
  memset(&key, 0, sizeof(DBT));
	key.flags = DB_DBT_MALLOC;
  memset(&data, 0, sizeof(DBT));
	data.flags = DB_DBT_MALLOC;

  /* Iterate over the database, retrieving each record in turn. */
  MerkleNode * mn;
  int i;
  while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    int suffix = ((MerkleNode *)data.data)->offset;
    int prefix = key.size - suffix;
    for (i = 0; i < prefix; i++) {
			if (i == (prefix - 1)) {
				std::cout << "|";
			} else {
				std::cout << " ";
			}
    }
    char * sstart = ((char *)(key.data)+prefix);
    std::cout << std::string(sstart,suffix); 
		DBT parentk = parent(&key, (MerkleNode *)(data.data));
		std::cout << "-->(" << dbt_string(&parentk) << ")";
		print_children(&key);
    std::cout << "                                  ";
		MerkleNode * m = (MerkleNode *)data.data;
		print_hex(&(m->digest), sizeof(MerkleHash));
    std::cout << ";\n";
		free(key.data);
		free(data.data);	//TODO: Memory Leak??
  }
  if (ret != DB_NOTFOUND) {
    std::cout << "DONE\n";
  }

  /* Cursors must be closed */
  if (cursorp != NULL) {
    cursorp->close(cursorp);
  }
  std::cout << "\n";
}

void MerkleDB::print_children(DBT *key) {
	DBT pkey, pdata; /* Used to return the primary key and data */
	int ret;
	
	memset(&pkey, 0, sizeof(DBT));
	pkey.flags = DB_DBT_MALLOC;
	memset(&pdata, 0, sizeof(DBT));
	pdata.flags = DB_DBT_MALLOC;
	DBC *cursorp;
	cld->cursor(cld, NULL, &cursorp, 0);
	
	ret = cursorp->pget(cursorp, key, &pkey, &pdata, DB_SET);
	if (ret == 0) {
		std::cout << "-->[";
		do {
			std::cout << "(";
			std::cout << dbt_string(&pkey);
			std::cout << ")";
			free(pkey.data);
			free(pdata.data);
		} while ((ret = cursorp->pget(cursorp, key, &pkey, &pdata, DB_NEXT_DUP)) == 0);
		std::cout << "]";
	}
	cursorp->close(cursorp);
}

void MerkleDB::close() {
	close_if_not_null(dbp);
	close_if_not_null(aly);
	close_if_not_null(pup);
	close_if_not_null(cld);
}

DBT MerkleDB::parent(DBT * key, MerkleNode * node) {
  DBT parentk;
  memset(&parentk, 0, sizeof(DBT));
	parentk.flags = DB_DBT_MALLOC;
  parentk.data = key->data;
  parentk.size = (key->size) - (node->offset);
  return parentk;
}

void MerkleDB::examine(DBT * key) {
  DBT data;
  memset(&data, 0, sizeof(DBT));
	data.flags = DB_DBT_MALLOC;
  std::cout << "examine(" << dbt_string(key) << ") => ";
  int ret;
  ret = dbp->get(dbp, NULL, key, &data, 0);
  if (0 == ret) {
    MerkleNode * m =  (MerkleNode *)((&data)->data);;
    printf("MerkleNode<digest:%i, offset:%i>\n", m->digest, m->offset);		
		free(data.data);
  } else if (DB_NOTFOUND == ret) {
    std::cout << "nil\n";
  } else {
    printf("error: %s\n", db_strerror(ret));
  }
}

int MerkleDB::dbt_equal(DBT * db1, DBT * db2) {
  bool c1 = ((db1)->size) == ((db2)->size);
  int c2 = memcmp(((db1)->data), ((db2)->data), db1->size);
  return (c1 and (c2 == 0));
}

#ifdef MERKLETEST
int test_macro(DB_ENV* db_env) {
  //DBT db1, db2, db3, db4;
  //	
  //memset(&db1, 0, sizeof(DBT));
  //memset(&db2, 0, sizeof(DBT));
  //memset(&db3, 0, sizeof(DBT));
  //memset(&db4, 0, sizeof(DBT));
  //
  //db1.data = (char *)"value1";
  //db1.size = 6;
  //
  //db2.data = (char *)"value2";
  //db2.size = 6;
  //
  //db3.data = (char *)"value1";
  //db3.size = 5;
  //
  //db4.data = (char *)"value1";
  //db4.size = 6;
  //
  //if (dbt_equal(&db1, &db2) || dbt_equal(&db1,&db3) || !dbt_equal(&db1,&db4)) {
  //	printf("Failed test_macro\n");
  //} else {
  //	printf("Succeeded test_macro\n");
  //}
}

int test_pending(DB_ENV* db_env) {
  std::cout << "Initialize MerkleDB\n";

  MerkleDB * merkle = new MerkleDB("testns",db_env,".");
  DBT key, key2, data;
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  int ret;
	
  //std::cout << "Look for root node via direct_get\n";
  //memset(&key, 0, sizeof(DBT));
  //memset(&data, 0, sizeof(DBT));
  //key.size = 0;	//root node
  //ret = merkle->direct_get(NULL, &key, &data, 0);
  //if (0 == ret) {
  //  printf("found\n");		
  //} else if (DB_NOTFOUND == ret) {
  //  std::cout << "not found\n";
  //} else {
  //  printf("error: %s\n", db_strerror(ret));
  //}
  std::cout << "Look for root node via examine\n";
  merkle->examine(&key);
	
  std::cout << "Schedule update of key1 & key2\n";
  memset(&key, 0, sizeof(DBT));
  memset(&key2, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  key.data = (char *)"user_1234";
  key.size = 8;
  data.data = (char *)"data1";
  data.size = 5;
  merkle->enqueue(&key, &data);
  key.data = (char *)"abcdefgh";
  key.size = 8;
  merkle->enqueue(&key, &data);
  key.data = (char *)"abcdjekl";
  key.size = 8;
  merkle->enqueue(&key, &data);
  key.data = (char *)"bbcdjekl";
  key.size = 8;
  merkle->enqueue(&key, &data);
  key.data = (char *)"bbcxxxxx";
  key.size = 8;
  merkle->enqueue(&key, &data);
  key.data = (char *)"bbcxxxyy";
  key.size = 8;
  merkle->enqueue(&key, &data);
  key.data = (char *)"bbcxxxxz";
  key.size = 8;
  merkle->enqueue(&key, &data);

  memset(&data, 0, sizeof(DBT));
  key2.data = (char *)"user_234";
  key2.size = 7;
  data.data = (char *)"data2";
  data.size = 5;
  merkle->enqueue(&key2, &data);
	
  data.data = (char *)"data3";
  data.size = 5;
  merkle->enqueue(&key, &data);
	
  std::cout << "Look for keys via examine\n";
  merkle->examine(&key);
  merkle->examine(&key2);
	
  merkle->flushp();

	key.data = (char *)"bbc";
	key.size = 3;
	std::cout << "printing_children of: " << dbt_string(&key) << "\n";
	merkle->print_children(&key);
	std::cout << "\n"	;
  //std::cout << "Look for keys node via examine\n";
  //merkle->examine(&key);
  //merkle->examine(&key2);
  //
  //data.data = (char *)"data4";
  //data.size = 5;
  //merkle->enqueue(&key, &data);
  //
  //std::cout << "flushing\n";
  //merkle->flushp();
  //
  //std::cout << "Look for keys node via examine\n";
  //merkle->examine(&key);
  //merkle->examine(&key2);
  //
  //merkle->print_tree();
	
  merkle->close();
	
}


int test_prefix(DB_ENV* db_env) {
  MerkleDB * merkle = new MerkleDB("testns",db_env,".");
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
	u_int32_t env_flags = 0;
  int ret;
  u_int32_t gb;
  DB_ENV* db_env;

  ret = db_env_create(&db_env, 0);
  if (ret != 0) {
    fprintf(stderr, "Error creating env handle: %s\n", db_strerror(ret));
    exit(-1);
  }

  env_flags = 
    DB_CREATE |     /* If the environment does not exist, create it. */
    DB_INIT_LOCK |  /* Multiple threads might write */
    DB_INIT_MPOOL|  /* Initialize the in-memory cache. */
    DB_THREAD |
    DB_PRIVATE;
  
  ret = db_env->set_lk_detect(db_env,DB_LOCK_DEFAULT);

  ret = db_env->open(db_env,      /* DB_ENV ptr */
		     ".",    /* env home directory */
		     env_flags,  /* Open flags */
		     0);         /* File mode (default) */
  if (ret != 0) {
    fprintf(stderr, "Environment open failed: %s\n", db_strerror(ret));
    exit(-1);
  }
  test_macro(db_env);
  test_pending(db_env);
  //test_prefix();
  return 0;
}
#endif
