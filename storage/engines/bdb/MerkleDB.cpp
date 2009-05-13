// MerkleDB File

#include "MerkleDB.h"

#include <iostream>
#include <string.h>
#include <stdlib.h>
#include "mhash.h"
#include <sys/time.h>
#include <assert.h>

#define min(i1, i2) ((i1) < (i2) ? (i1) : (i2))
#define max(i1, i2) ((i1) > (i2) ? (i1) : (i2))
#define return_with_error(error) std::cout << db_strerror(error) << "\n"; return error;
#define return_with_success() return 0;
#define close_if_not_null(db) if ((db) != NULL) { (db)->close((db), 0); }
#define cleanup_after_bdb() cursorp->close(cursorp); while (data_ptrs.size() > 0) { free(data_ptrs.back()); data_ptrs.pop_back(); }
#define print_hex(buf, len) for (int i = (len) - 1; i >= 0 ; i--) { printf("%X%X", (0x0F & (((char *)buf)[i]) >> 4), (0x0F & (((char *)buf)[i])));}

#define prepare_timer() struct timeval start_time, end_time, diff_time;
#define start_timer() gettimeofday(&start_time,NULL)
#define end_timer() { \
    gettimeofday(&end_time,NULL); \
    timersub(&end_time,&start_time,&diff_time); \
    printf("%ld.%.6ld\n", diff_time.tv_sec, diff_time.tv_usec); \
  }

//#define is_leaf(keyd) false
#define dbt_string(dbt) std::string((char*)(dbt)->data,(dbt)->size)

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

int cmp_shortest_first(DB *dbp, const DBT *a, const DBT *b) {
	if (a->size < b->size) {
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

MerkleDB::MerkleDB(const string& ns, DB_ENV* db_env) :
  flush_flag(0)
{
	
  //TODO: We'll need a different merkledb for each namespace.  Ditto for the pending queue (unless we put more info in struct)
  char filebuf[10+ns.length()];
  int ret;

  if (qdb == NULL) { // it's static so only init it once
    db_create(&qdb, db_env, 0);
    qdb->open(qdb, NULL, "merkle_queue",NULL, DB_RECNO, DB_CREATE , 0);
  }

  /* Initialize the DB handles */
  //TODO: Add error checking here and elsewhere
  db_create(&dbp, db_env, 0);
  db_create(&pup, db_env, 0);
  db_create(&aly, db_env, 0);

  //Create index (secondary database) to give parent->children mapping
  db_create(&cld, db_env, 0);
  cld->set_flags(cld, DB_DUPSORT);

  //Set sorting functions for queues
  pup->set_bt_compare(pup, cmp_longest_first);
  aly->set_bt_compare(aly, cmp_longest_first);
	//cld->set_bt_compare(cld, cmp_shortest_first);

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

DB* MerkleDB::qdb = NULL;

//Adds key->hash(data) to pending update queue
//NOTE! Enqueue only takes null-terminated strings
int MerkleDB::enqueue(DBT * key, DBT * data) {
  int ret;
  DBT h;
  MHASH td;
	
  memset(&h, 0, sizeof(DBT));
  //h.flags = DB_DBT_MALLOC; //We're not currently 'get()-ing' here
	
	//TODO: HACK: enqueue accepts both null and non-null terminated keys, but the MerkleDB needs null-terminated strings.
	//If our string ends in the null character, we're fine. [Hack #1]
	bool c_str = ((((char *)key->data)[key->size - 1] == '\0') ? true : false);
	
  td = mhash_init(MERKLEDB_HASH_FUNC);
  if (td == MHASH_FAILED) {
    std::cerr << "HASH Failed";
    return -1;
  }
	//TODO: HACK: We only hash in the non-null part of the key for uniformity between pascal & c-strings[Hack #1]
  mhash(td, (unsigned char *)key->data, key->size - (c_str ? 1 : 0));
  mhash(td, (unsigned char *)data->data, data->size);
 
  h.data = malloc(MERKLEDB_HASH_WIDTH / 8);
  mhash_deinit(td, h.data);	//TODO: use mhash_end instead.  Not clear if we need to 'free' mhash_end, so playing safe.

	//TODO: HACK: If we don't have a c-style string, we increase the size of the data item to one larger than it's
	//"supposed" to be.  This is just a flag that lets flushp know we're dealing with a pascal string. :()**** [Hack #1]
  h.size = sizeof(MerkleHash)+(c_str ? 0 : 1);
  MerkleHash hash = *((MerkleHash *)(h.data));
  //std::cout << "enqueue(\"" << dbt_string(key) << "\",\"" << dbt_string(data) << "\") with hash:\t";
  //print_hex(h.data, h.size);
  //std::cout << "\n";
	
	//TODO: HACK: If we're dealing with a pascal string, we increase the size of the key by one to make space for the 
	//null terminator.  We don't set it now for efficiency reasons.  Instead we repair it when we dequeue in flushp
	//BDB will read one extra (garbage) byte past the end of the string into the database, but we'll overwrite it
	//before using it. [Hack #1]
	if (!c_str) { key->size += 1; }	
  ret = pup->put(pup, NULL, key, &h, 0);
	//std::cout << "pup->put(";
	//std::cout << key->size << "\t:\t";
	//print_hex(&key, key->size);
	//std::cout << ")"<<endl;
	if (!c_str) { key->size -= 1; }
  free(h.data);
  if (ret != 0) { return_with_error(ret); }
  return_with_success();
}

//Clear the pending update queue
int MerkleDB::flushp() {
  DBT key, data;
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  data.flags = DB_DBT_MALLOC;
  key.flags = DB_DBT_MALLOC;
	
  pthread_mutex_lock(&sync_lock);

  //Swap pending and apply queues
  DB * tmp = aly;
  aly = pup;
  pup = tmp;

  int ret = 0;
  DBC *cursorp;
  aly->cursor(aly, NULL, &cursorp, 0);
  while ((ret = cursorp->get(cursorp, &key, &data, DB_FIRST)) == 0) {
		bool repair = ((data.size == sizeof(MerkleHash)) ? false : true);
		if (repair) {
			((char *)key.data)[key.size - 1] = '\0'; //Force last character to null for null-termination [Hack #1]
		}
		ret = cursorp->del(cursorp, 0); //TODO: Switch to a transactional store for these queues, not crash safe here
    if (ret != 0) { 
      pthread_mutex_unlock(&sync_lock);
      return_with_error(ret);
    }
    cursorp->close(cursorp);
    insert(&key, *((MerkleHash *)(data.data)));
    free(key.data);
    free(data.data);
#ifdef DEBUG
    //print_tree();
#endif
    aly->cursor(aly, NULL, &cursorp, 0);
  }
  if (ret != DB_NOTFOUND) { 
    pthread_mutex_unlock(&sync_lock);
    return_with_error(ret);
  } else {
    pthread_mutex_unlock(&sync_lock);
    return_with_success();
  }
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
  //std::cout << "insert(\"" << dbt_string(key) << "\", ";
  //print_hex(&hash, sizeof(MerkleHash));
  //std::cout << ")\n";
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
      ((MerkleNode *)cdata.data)->digest = hash;
      recalculate(&ckey, &cdata, cursorp);
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
    //TODO: We need to do something besides DB_PREV in next call to find left neighbor
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
	
  //Find the left-side neighbor (if cursor failed finding right, DB_PREV will give us DB_LAST (which is correct))
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

	//We only support C-style null terminated keys, so prefixl must be shorter than key.size
	if (prefixl == key->size) {
		std::cerr << "Inconceivable! : Key length shouldn't be able to be same!\n ";
		exit(-1);
	}
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
      newn.digest = 0;
      //We don't need to set the hash here, since that will happen naturally when we start walking up the tree
      ret = cursorp->put(cursorp, &ckey, &cdata, DB_KEYFIRST);
      if (ret != 0) { return_with_error(ret); }
    }
    /* parent is guaranteed to exist now. (Parent node either existed, or we created it)  */
    /* We should never have the same key as our parent (TODO: redundant check) */
		if (parentk.size == key->size) { 
			std:cerr << "Inconceivable! : Parent length shouldn't be able to be same!\n";
			exit(-1);
		}
    //Add a node for this key
		ckey.data = key->data;
    ckey.size = key->size;
    cdata.data = &newn;
    memset(&newn, 0, sizeof(MerkleNode));
    newn.offset = key->size - parentk.size;
    newn.digest = hash;
    ret = cursorp->put(cursorp, &ckey, &cdata, DB_KEYFIRST);
    if (ret != 0) { return_with_error(ret); }
    recalculate(key, &cdata, cursorp);
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

//rehash the targeted key and add it's parent to aly queue
int MerkleDB::recalculate(DBT * key, DBT * data, DBC * cursorp) {
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
	
  MerkleNode * mn = ((MerkleNode *)data->data);
	if (is_leaf(key)) {
		//Hash in the data for this node
  	mhash(td, (unsigned char *)key->data, key->size - 1); //HACK: We don't hash in the null terminator [Hack #1]
  	mhash(td, (unsigned char *)&(mn->digest), sizeof(MerkleHash));
	} else {
		//Hash in the children (in lexical order) //TODO: Confirm lexical ordering
  	DBC *childc;
  	MerkleNode * childn;
		cld->cursor(cld, NULL, &childc, 0);
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
	}
	
  void * digest = malloc(MERKLEDB_HASH_WIDTH / 8);
  //TODO: use mhash_end instead.  Not clear if we need to 'free' mhash_end, so playing safe.
  mhash_deinit(td, digest);
  mn = (MerkleNode *)data->data; //TODO: Note, we're changing the state of the data DBT passed in, icky.
  mn->digest = *((int *)digest);	//TODO: We're only using a little bit of the hash.. decide which way to go.
	
  //Put back into the main database
  ret = cursorp->put(cursorp, key, data, DB_KEYFIRST);
  free(digest);
  if (ret != 0) { return_with_error(ret); }
	
  //Now put this nodes parent into the apply queue to recurse up the tree
  DBT parentk, parentd;
  memset(&parentk, 0, sizeof(DBT));
  memset(&parentd, 0, sizeof(DBT));
  parentk.flags = DB_DBT_MALLOC;
  parentd.flags = DB_DBT_MALLOC;
  if (key->size > 0) {	//The root is its own parent, but don't add it.
    parentk = parent(key, mn);
    //std::cout << "putting: " << dbt_string(&parentk) << "\n";
		
    //We need to get the parent's data node to add it to the apply queue correctly
    //TODO: Alter apply queue so it differentiates between sets and recurse-ups (and insert, tec)
    ret = cursorp->get(cursorp, &parentk, &parentd, DB_SET);
    if (ret != 0) { return_with_error(ret); }
		
    DBT mh;
    memset(&mh, 0, sizeof(DBT));
    mh.data = &(((MerkleNode *)parentd.data)->digest);
		mh.size = sizeof(MerkleHash);	//See [Hack #1]
		
    ret = aly->put(aly, NULL, &parentk, &mh, DB_NOOVERWRITE);
    free(parentd.data);
    if (ret == DB_KEYEXIST) {
      //			std::cout << "Already existed. nm.\n";
    } else if (ret != 0) {
      return_with_error(ret);
    }
  }
	
  //TODO: implement has and add parent to pending queue
  //std::cout << "update(" << dbt_string(key) << ", ";
  //print_hex(&mn->data_digest, sizeof(MerkleHash));
  //std::cout << "\n";
	
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
  while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    //std::cout << "(key: "<<string((char*)key.data,key.size)<<") ";
    MerkleNode * m = (MerkleNode *)data.data;
    print_hex(&(m->digest), sizeof(MerkleHash));
    std::cout << " ";
    int suffix = ((MerkleNode *)data.data)->offset;
    int prefix = key.size - suffix;
    for (int i = 0; i < prefix; i++) {
      if (i == (prefix-1)) {
				std::cout << "|";
      } else {
				std::cout << " ";
      }
    }
    char * sstart = ((char *)(key.data)+prefix);
    std::cout << std::string(sstart,suffix) << "\n"; 
    //DBT parentk = parent(&key, (MerkleNode *)(data.data));
    //std::cout << "-->(" << dbt_string(&parentk) << ")";
    //print_children(&key);
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

void MerkleDB::
apply_children(DBT *key,
	       void(*to_apply)(void*,void*,void*),
	       void* apply_arg) {
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
    do {
      (*to_apply)(apply_arg,&pkey,&pdata);
      free(pkey.data);
      free(pdata.data);
    } while ((ret = cursorp->pget(cursorp, key, &pkey, &pdata, DB_NEXT_DUP)) == 0);
  }
  cursorp->close(cursorp);
}

void MerkleDB::queue_children(DBT *key, TQueue<DBT>* q) {
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
    do {
#ifdef DEBUG
      //std::cout << "Queuing child: "<<dbt_string(&pkey)<<endl;
#endif
      q->enqueue(pkey);
      //free(pdata.data);  TODO: this?
    } while ((ret = cursorp->pget(cursorp, key, &pkey, &pdata, DB_NEXT_DUP)) == 0);
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
    MerkleNode * m =  (MerkleNode *)((&data)->data);
    printf("MerkleNode<digest:%i, offset:%i>\n", m->digest, m->offset);		
    free(data.data);
  } else if (DB_NOTFOUND == ret) {
    std::cout << "nil\n";
  } else {
    printf("error: %s\n", db_strerror(ret));
  }
}

//TODO: Make this return a boolean
int MerkleDB::dbt_equal(DBT * db1, DBT * db2) {
	//We need to avoid doing memcmp if size is 0
	if (db1->size == 0) { return (db2->size == 0); }
	if (db2->size == 0) { return (db1->size == 0); }
//  int c2 = memcmp(((db1)->data), ((db2)->data), db1->size);
// Need short circuit evaluation in case size is zero and data undefined
  return (((db1)->size) == ((db2)->size) and (memcmp(((db1)->data), ((db2)->data), db1->size) == 0));
}

//Return the lexical comparison of these dbts
int MerkleDB::dbt_cmp(DBT * db1, DBT * db2) {
	if (db1->size == 0) { return (db2->size == 0 ? 0 : -1); }
	if (db2->size == 0) { return (db1->size == 0 ? 0 : 1); }
	int prefixlen = (db1->size < db2->size ? db1->size : db2->size);
	int c = memcmp(db1->data, db2->data, prefixlen);
	if (c != 0) {
		return c;
	} else {
		if (db1->size == db2->size) {
			return 0;
		} else if (db1->size > db2->size) {
			return 1;
		} else {
			return -1;
		}
	}
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

  MerkleDB * merkle = new MerkleDB("testns",db_env);
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
  merkle->print_tree();
	
  merkle->close();
}


int test_prefix(DB_ENV* db_env) {
  MerkleDB * merkle = new MerkleDB("testns",db_env);
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

int test_pending2(DB_ENV* db_env) {
  std::cout << "Initialize MerkleDB\n";

  MerkleDB * merkle = new MerkleDB("testns",db_env);
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
  key.data = (char *)"bbcdjekl";
  key.size = 8;
  merkle->enqueue(&key, &data);
  key.data = (char *)"bbcxxxxx";
  key.size = 8;
  merkle->enqueue(&key, &data);
  key.data = (char *)"abcdefgh";
  key.size = 8;
  merkle->enqueue(&key, &data);
  key.data = (char *)"abcdjekl";
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
  key.data = (char *)"bbcxxxyy";
  key.size = 8;
  merkle->enqueue(&key, &data);
  key.data = (char *)"bbcxxxxz";
  key.size = 8;
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
  merkle->print_tree();
	
  merkle->close();
}


void build_random_tree(MerkleDB * mdb, int rows, int key_seed, int data_seed, int out_std = 0) {
	char keybuf[128];
	DBT key;
	memset(&key, 0, sizeof(DBT));
	key.flags = DB_DBT_MALLOC;
	key.data = keybuf;
		
	char databuf[2048];
	DBT data;
	memset(&data, 0, sizeof(DBT));
	data.flags = DB_DBT_MALLOC;
	data.data = databuf;
 
	//Make data
	srand(data_seed);
	int datalen = 1024;
	//std::cout << "data:" << datalen << endl;
	//change 10 keys to make data different
	int pos;
	for (int k = 0; k < datalen; k++) {
		databuf[k] = ((char) ((rand() % 24) + 65));
	}
	databuf[datalen] = '\0';
	data.size = datalen;


	int keylen;
	for (int i = 0; i < rows; i++) {
		//Make key
		srand(key_seed+i);
		keylen = (rand() % 64) + 1;
		//std::cout << "key:" << keylen << ",";
		for (int j = 0; j < keylen; j++) {
			keybuf[j] = ((char) ((rand() % 24) + 65));
		} 
		keybuf[keylen] = '\0';
		key.size = keylen;
		if (out_std != 0) { (out_std > 0 ? std::cout : std::cerr) << i << "\t" << keybuf << "\t"; }

		//Mix up the data just a little bit
		srand(data_seed + i);
		for (int k = 0; k < 5; k++) {
			databuf[(rand() % datalen)] = ((char) ((rand() % 24) + 65));
		}
		if (out_std != 0) { (out_std > 0 ? std::cout : std::cerr) << "\t" << databuf << endl; }
		
		mdb->enqueue(&key, &data);
	}
	mdb->flushp();
}

void random_order_insertion_test(MerkleDB * merkle, int seed, bool c_style) {
  std::cout << "build_tree(" << merkle << ", " << seed << ")\n";
  srand(seed);
  vector<std::string> keys;
  vector<std::string> data;
  keys.push_back("deefafdfebe");
  keys.push_back("ccaacdcbddbc");
  keys.push_back("cedfccdfbdccffbbbfccd");
  keys.push_back("ffaeceaefffebfcbb");
  keys.push_back("cbdcaaf");
  keys.push_back("ceda");
  keys.push_back("dedaccedfff");
  keys.push_back("efacac");
  keys.push_back("addabdaacafccd");
  keys.push_back("edbdf");
	
  data.push_back("1");
  data.push_back("2");
  data.push_back("3");
  data.push_back("4");
  data.push_back("5");
  data.push_back("6");
  data.push_back("7");
  data.push_back("8");
  data.push_back("9");
  data.push_back("10");
	
  unsigned int i;
  int max;
  char * key;
  char * datum;
  while (keys.size() > 0) {
    max = keys.size();
    i = rand() % max;
    key = (char *)keys[i].c_str();
    datum = (char *)data[i].c_str();
    //std::cout << key << " , " << datum << "\n";
    DBT keyd, datad;
    memset(&keyd, 0, sizeof(DBT));
    memset(&datad, 0, sizeof(DBT));
    keyd.data = key;
    keyd.size = strlen(key)+(c_style ? 1 : 0);
    datad.data = datum;
    datad.size = strlen(datum);
		
    merkle->enqueue(&keyd, &datad);
    keys.erase(keys.begin()+i);
    data.erase(data.begin()+i);
  }
  merkle->flushp();
  //merkle->print_tree();
}

void test_print_hex() {
	unsigned int i = 0xFFFFFFFF;
	for (unsigned int j = 0; j < i; j++) {
		print_hex(&j, sizeof(int));
		std::cout << "\n";
	}
}

void pascal_and_c_test(MerkleDB * mdb) {
  char testkey[] = "abcdef";
	char testdata[] = "data";
	DBT key, data;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = testkey;
	key.size = strlen(testkey);
	data.data = testdata;
	data.size = strlen(testdata);
	
	mdb->enqueue(&key, &data);
	mdb->flushp();
	
	key.size = strlen(testkey) + 1;
	testkey[3] = ((char) (rand() % ('z' - '0' + 1) + '0')); 
	mdb->enqueue(&key, &data);
	mdb->flushp();
}

void linear_scan_test(MerkleDB * mdb1, MerkleDB * mdb2) {
	DBC * c1;
	DBC * c2;
	DB * db1 = (mdb1->dbp);
	DB * db2 = (mdb2->dbp);
	db1->cursor(db1, NULL, &c1, 0);
	db2->cursor(db2, NULL, &c2, 0);
	
	DBT k1, k2;
	DBT d1, d2;
	int ret1, ret2;
	memset(&k1 , 0, sizeof(DBT));
	memset(&k2 , 0, sizeof(DBT));
	memset(&d1 , 0, sizeof(DBT));
	memset(&d2 , 0, sizeof(DBT));
	
	k1.flags = DB_DBT_MALLOC;
	k2.flags = DB_DBT_MALLOC;
	d1.flags = DB_DBT_MALLOC;
	d2.flags = DB_DBT_MALLOC;
	
	k1.size = 0;
	k2.size = 0;
	
	void * t1;
	void * t2;
	void * t3;
	void * t4;
	
	ret1 = c1->get(c1, &k1, &d1, DB_FIRST);
	ret2 = c2->get(c2, &k2, &d2, DB_FIRST);
}

void dfs_with_pruning_test(MerkleDB * mdb1, MerkleDB * mdb2) {
	DBC * c1;
	DBC * c2;
	DB * db1 = (mdb1->dbp);
	DB * db2 = (mdb2->dbp);
	db1->cursor(db1, NULL, &c1, 0);
	db2->cursor(db2, NULL, &c2, 0);
	
	DBT k1, k2;
	DBT d1, d2;
	int ret1, ret2;
	memset(&k1 , 0, sizeof(DBT));
	memset(&k2 , 0, sizeof(DBT));
	memset(&d1 , 0, sizeof(DBT));
	memset(&d2 , 0, sizeof(DBT));
	
	k1.flags = DB_DBT_MALLOC;
	k2.flags = DB_DBT_MALLOC;
	d1.flags = DB_DBT_MALLOC;
	d2.flags = DB_DBT_MALLOC;
	
	k1.size = 0;
	k2.size = 0;
	
	void * t1;
	void * t2;
	void * t3;
	void * t4;
	
	ret1 = c1->get(c1, &k1, &d1, DB_SET);
	ret2 = c2->get(c2, &k2, &d2, DB_SET);

	t1 = k1.data;t2 = d1.data;t3 = k2.data;t4 = d2.data;
	if (ret1 != 0 or ret2 != 0) {
		printf("Inconceviable!  Root node doesn't exist");
		exit(1);
	}
	if (mdb1->dbt_equal(&d1, &d2)) { 
		std::cout << "Trees identical" << endl;
		c1->close(c1);
		c2->close(c2);
		return;
	}
	while (ret1 == 0 or ret2 == 0) {	//There might be more cases
		//std::cout << "Consider: l:(" << dbt_string(&k1) << ")\tr:(" << dbt_string(&k2) << ")" << endl;
		if (mdb1->dbt_equal(&k1, &k2) and mdb1->dbt_equal(&d1, &d2)) {
			int i = 1;
			//TODO: if last character is 255, need to carry over to higher chars
			((char *) k1.data)[k1.size - i] += 1;
			((char *) k2.data)[k2.size - i] += 1;
			//TODO: Memory leak, need to free
			ret1 = c1->get(c1, &k1, &d1, DB_SET_RANGE);
			ret2 = c2->get(c2, &k2, &d2, DB_SET_RANGE);
			if (ret1 == 0) {
				free(t1);
				free(t2);
				t1 = k1.data;
				t2 = d1.data;
			}
			if (ret2 == 0) {
				free(t3);
				free(t4);
				t3 = k2.data;
				t4 = d2.data;
			}
		} else {
			//Move the left cursor to catch up to the right cursor
			while (ret1 == 0 and (mdb1->dbt_cmp(&k1, &k2) < 0 or ret2 != 0)) {
				//TODO: test
				std::cout << "only-left-side: \"" << dbt_string(&k1) << "\"" <<  endl;
				ret1 = c1->get(c1, &k1, &d1, DB_NEXT);
				if (ret1 == 0) { free(t1);free(t2);t1 = k1.data;t2 = d1.data; }
			}
			//Move the right cursor to catch up to the left cursor
			while (ret2 == 0 && (mdb1->dbt_cmp(&k1, &k2) > 0 or ret1 != 0)) {
				std::cout << "only-right-side: \"" << dbt_string(&k2) << "\"" <<  endl;
				ret2 = c2->get(c2, &k2, &d2, DB_NEXT);
				if (ret2 == 0) { free(t3);free(t4);t3 = k2.data;t4 = d2.data; }
			}
			//keys are guaranteed to be the same (or one is empty), but data different
			if (ret1 == 0 and ret2 != 0) { std::cout << "Inconceviable(1)!" << endl; exit(1); }
			if (ret2 == 0 and ret1 != 0) { std::cout << "Inconceviable(2)!" << endl; exit(1); }
			if (ret1 == 0 and ret2 == 0) {
				std::cout << "different: \"" << dbt_string(&k1) << "\"" << endl;
				ret1 = c1->get(c1, &k1, &d1, DB_NEXT);
				ret2 = c2->get(c2, &k2, &d2, DB_NEXT);
				if (ret1 == 0) {
					free(t1);
					free(t2);
					t1 = k1.data;
					t2 = d1.data;
				}
				if (ret2 == 0) {
					free(t3);
					free(t4);
					t3 = k2.data;
					t4 = d2.data;
				}
			}
		}
	}
  
	c1->close(c1);
	c2->close(c2);
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
  MerkleDB * mdb1 = new MerkleDB("tree1",db_env);
	//pascal_and_c_test(mdb4);
  //std::cout << "alter with: dedaccedff := dsfkj\n";
  //DBT key, data;
  //memset(&key, 0, sizeof(DBT));
  //memset(&data, 0, sizeof(DBT));
  //char key_s[] = "dedaccedfff";
  //char data_s[] = "dsfkj";
  //key.data = key_s;
  //key.size = strlen(key_s);
  //data.data = data_s;
  //data.size = strlen(data_s);
  //mdb2->enqueue(&key, &data);
  //
	//key_s[0] = 'z';
	//mdb2->enqueue(&key, &data);
	//
	//key_s[0] = 'y';
	//mdb3->enqueue(&key, &data);
  //
  //mdb2->flushp();
	//mdb3->flushp();

  //mdb2->print_tree();
	prepare_timer();

	MerkleDB * mdb2 = new MerkleDB("tree2",db_env);
	MerkleDB * mdb3 = new MerkleDB("tree3",db_env);
  //random_order_insertion_test(mdb1, 11230, true);
  //random_order_insertion_test(mdb2, 301433, true);
	//random_order_insertion_test(mdb3, 301433, false);
	MerkleDB * mdb4 = new MerkleDB("tree4",db_env);
	//start_timer();
	//build_random_tree(mdb3, 1000000, 123213, 12312);
	//end_timer();

	//mdb2->print_tree();
	//mdb3->print_tree();
	
	start_timer();
	build_random_tree(mdb2, 10000, 123213, 12312);
	end_timer();
//	mdb2->print_tree();
	std::cout << "*1-first database done, copy, then type character to continue" << endl;
	//getc(stdin);
	start_timer();
	build_random_tree(mdb3, 10000, 123213, 12312);
	end_timer();
//	mdb3->print_tree();
	std::cout << "*2" << endl;

	

	start_timer();
	dfs_with_pruning_test(mdb2, mdb3);
	end_timer();

	std::cout << "*3" << endl;
	build_random_tree(mdb2, 1, 123213, 234981);
	std::cout << "*4" << endl;
	start_timer();
	dfs_with_pruning_test(mdb2, mdb3);
	end_timer();
	//dfs_with_pruning_test(mdb2, mdb3);
	//mdb1->flushp();
	//mdb1->print_tree();
	//mdb3->flushp();
	//mdb3->print_tree();
	//test_print_hex();
  //test_macro(db_env);
  //test_pending(db_env);
  //test_pending2(db_env);
  //test_prefix();
  mdb1->close();
	mdb2->close();
	mdb3->close();
	mdb4->close();
	return 0;
}
#endif
