//bdb scads storage engine
#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <server/TNonblockingServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>

#include <cerrno>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <map>

#include "pthread.h"

#include <signal.h>

#include "StorageDB.h"

// for bulk get, read in 20mb chunks
#define	BUFFER_LENGTH	(20 * 1024 * 1024)

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

using namespace boost;

using namespace SCADS;

// program wide config stuff
static char* env_dir;
static int port;

// this is only ever read, so it's thread safe
ID call_id;


// this is gross, but it's the only way to avoid a segfault
// if the passed procedure has a syntax error
VALUE rb_funcall_wrap(VALUE vargs) {
  VALUE* args = (VALUE*)vargs;
  return
    (args[2] == 0)? 
    rb_funcall(args[0], call_id, 1, args[1]):
    rb_funcall(args[0], call_id, 2, args[1], args[2]);
}

char * strnstr(const char *s, const char *find, size_t slen)
{
  char c, sc;
  size_t len;
  
  if ((c = *find++) != '\0') {
    len = strlen(find);
    do {
      do {
	if (slen < 1 || (sc = *s) == '\0')
	  return (NULL);
	--slen;
	++s;
      } while (sc != c);
      if (len > slen)
	return (NULL);
    } while (strncmp(s, find, len) != 0);
    s--;
  }
  return (char*)s;
}

int uf;


// set application functions
void apply_get(void* vec, DB* db, DBC* cursor, DB_TXN* txn, void* k, void* d) {
  vector<Record>* _return = (std::vector<Record>*)vec;
  Record r;
  DBT *key,*data;
  key = (DBT*)k;
  data = (DBT*)d;
  r.key.assign((const char*)key->data,key->size);
  r.value.assign((const char*)data->data,data->size);
  r.__isset.value = true;
  _return->push_back(r);
}

void apply_del(void* v, DB* db, DBC* cursor, DB_TXN* txn, void* k, void* d) {
  int ret;
  DBT *key = (DBT*)k;
#ifdef DEBUG
  cerr << "Deleting: "<<string((char*)key->data,key->size)<<endl;
#endif
  if (uf & DB_INIT_TXN)
    ret = db->del(db,NULL,key,0);
  else
    ret = cursor->del(cursor,0);
  if (ret)
    db->err(db,ret,"Delete failed");
}

void apply_inc(void* c, DB* db, DBC* cursor, DB_TXN* txn, void*k, void *d) {
  int *i = (int*)c;
  (*i)++;
}


int StorageDB::
open_database(DB **dbpp,                  /* The DB handle that we are opening */
	      const char *file_name,     /* The file in which the db lives */
	      const char *program_name,  /* Name of the program calling this function */
	      const char *env_dir,       /* environment dir */
	      FILE *error_file_pointer)  /* File where we want error messages sent */
{
  DB *dbp;    /* For convenience */
  u_int32_t open_flags;
  int ret;
  

  /* Initialize the DB handle */
  ret = db_create(&dbp, db_env, 0);
  if (ret != 0) {
    fprintf(error_file_pointer, "%s: %s\n", program_name,
	    db_strerror(ret));
    return(ret);
  }

  /* Point to the memory malloc'd by db_create() */
  *dbpp = dbp;
                                                                                                                               
  /* Set up error handling for this database */
  dbp->set_errfile(dbp, error_file_pointer);
  dbp->set_errpfx(dbp, program_name);

  /* Set the open flags */
  if (user_flags &
      DB_INIT_TXN)
    open_flags = DB_CREATE | DB_THREAD | DB_AUTO_COMMIT | DB_MULTIVERSION;
  else
    open_flags = DB_CREATE | DB_THREAD;

  /* Now open the database */
  ret = dbp->open(dbp,        /* Pointer to the database */
		  NULL,       /* Txn pointer */
		  file_name,  /* File name */
		  NULL,       /* Logical db name (unneeded) */
		  DB_BTREE,   /* Database type (using btree) */
		  open_flags, /* Open flags */
		  0);         /* File mode. Using defaults */
  if (ret != 0) {
    dbp->err(dbp, ret, "Database '%s' open failed.", file_name);
    TException te("Could not open database");
    throw te;
  }
                                                                                                                               
  return (0);
}


bool StorageDB::
responsible_for_key(const NameSpace& ns, const RecordKey& key) {
  RecordSet rs;
  get_responsibility_policy(rs,ns);

  if (rs.type == RST_ALL) 
    return true;
  if (rs.type == RST_NONE)
    return false;

  if (rs.type == RST_RANGE) {
    if ( (!rs.range.__isset.start_key ||
	  key >= rs.range.start_key) &&
	 (!rs.range.__isset.end_key ||
	  key <= rs.range.end_key)  )
      return true;
    else
      return false;
  }

  int rb_err;
  VALUE funcall_args[3];
  if (rs.type == RST_KEY_FUNC) {
    VALUE ruby_proc = rb_eval_string_protect(rs.func.func.c_str(),&rb_err);
    if (!rb_respond_to(ruby_proc,call_id)) {
      InvalidSetDescription isd;
      isd.s = rs;
      isd.info = "Your ruby string for your responsiblity policy does not return something that responds to 'call'";
      isd.__isset.s = true;
      isd.__isset.info = true;
      throw isd;
    }
    VALUE v;
    funcall_args[0] = ruby_proc;
    funcall_args[1] = rb_str_new((const char*)(key.c_str()),key.length());
    funcall_args[2] = 0;
    v = rb_protect(rb_funcall_wrap,((VALUE)funcall_args),&rb_err);
    if (rb_err) {
      InvalidSetDescription isd;
      isd.s = rs;
      VALUE lasterr = rb_gv_get("$!");
      VALUE message = rb_obj_as_string(lasterr);
      isd.info = rb_string_value_cstr(&message);
      isd.__isset.s = true;
      isd.__isset.info = true;
      throw isd;
    }
    return (v == Qtrue);
  }

  return false; // if we don't understand, we'll say no
}

bool StorageDB::
responsible_for_set(const NameSpace& ns, const RecordSet& rs) {
  RecordSet policy;
  get_responsibility_policy(policy,ns);
    
  if (policy.type == RST_ALL || policy.type == RST_KEY_FUNC)
    return true;
  if (rs.type == RST_NONE)
    return false;
    
  if (rs.type == RST_RANGE) {
    if (!rs.__isset.range) {
      InvalidSetDescription isd;
      isd.s = rs;
      isd.info = "You specified a range set but did not provide a range description";
      isd.__isset.s = true;
      isd.__isset.info = true;
      throw isd;
    }
    if (
	( !rs.range.__isset.start_key ||
	  (policy.range.__isset.start_key &&
	   rs.range.start_key >= policy.range.start_key) ) &&
	( !rs.range.__isset.end_key ||
	  (policy.range.__isset.end_key &&
	   rs.range.end_key <= policy.range.end_key) ) 
	)
      return true;
    else
      return false;
  }
    
  return false; // if we don't understand, we'll say no
}

void StorageDB::
chkLock(int rc, const string lock, const string action) {
  switch (rc) {
  case 0: // success
    return;
  case EINVAL:
    cerr << "Couldn't get "<<lock<< " for "<<action<<":\n\t"<<
      " The value specified by rwlock does not refer to  an  initialized read-write lock object."<<endl;
    exit(EXIT_FAILURE);
  case EAGAIN:
    cerr << "Couldn't get "<<lock<< " for "<<action<<":\n\t"<<
      "The  read  lock could not be acquired because the maximum number of read locks for rwlock has been exceeded."<<endl;
    exit(EXIT_FAILURE);
  case EDEADLK:
    cerr << "Couldn't get "<<lock<< " for "<<action<<":\n\t"<<      
      "The current thread already owns the read-write lock for writing or reading."<<endl;
  case EPERM:
    cerr << "WARNING: tried to unlock "<<lock<< " for "<<action<<" but the lock is not held."<<endl;
    return; // hopefully this isn't the end of the world
  default:
    cerr << "An unknown error in getting: "<<lock<<" for "<<action<<endl;
    exit(EXIT_FAILURE);
  }
}

DB* StorageDB::
getDB(const NameSpace& ns) {
  map<const NameSpace,DB*>::iterator it;
  DB* db;
  int rc = pthread_rwlock_rdlock(&dbmap_lock); // get the read lock
  chkLock(rc,"dbmap_lock","top read lock");
  it = dbs.find(ns);
  if (it == dbs.end()) { // haven't opened this db yet
    open_database(&db,ns.c_str(),"storage.bdb",env_dir,stderr);
    //MerkleDB* mdb = new MerkleDB(ns,db_env);
    rc = pthread_rwlock_unlock(&dbmap_lock); // unlock read lock
    chkLock(rc,"dbmap_lock","unlock read lock for upgrade");
    rc = pthread_rwlock_wrlock(&dbmap_lock); // need a write lock here
    chkLock(rc,"dbmap_lock","modify dbs map");
    dbs[ns] = db;
    rc = pthread_rwlock_unlock(&dbmap_lock); // unlock write lock
    chkLock(rc,"dbmap_lock","unlock write lock");
    return db;
  }
  db = it->second;
  rc = pthread_rwlock_unlock(&dbmap_lock); // unlock read lock
  chkLock(rc,"dbmap_lock","unlock top read lock");
  return db;
}


void StorageDB::
apply_to_set(const NameSpace& ns, const RecordSet& rs,
	     void(*to_apply)(void*,DB*,DBC*,DB_TXN*,void*,void*),void* apply_arg,
	     bool invokeNone, bool bulk) {
  DB* db_ptr;
  DBC *cursorp;
  DBT cursor_key, cursor_data, key, data;
  DB_TXN *txn;
  int ret,count=0,skipped=0,rb_err;
  u_int32_t cursor_get_flags;
  VALUE ruby_proc;
  VALUE funcall_args[3];
  size_t retklen, retdlen;
  void *retkey, *retdata, *p;

  if (rs.__isset.range &&
      rs.__isset.func) {
    InvalidSetDescription isd;
    isd.s = rs;
    isd.info = "You specified both a range and function in your set";
    isd.__isset.s = true;
    isd.__isset.info = true;
    throw isd;
  }

  if (rs.type == RST_RANGE &&
      !rs.__isset.range) {
    InvalidSetDescription isd;
    isd.s = rs;
    isd.info = "You specified a range set but did not provide a range description";
    isd.__isset.s = true;
    isd.__isset.info = true;
    throw isd;
  }

  if ( (rs.type == RST_KEY_FUNC  ||
	rs.type == RST_KEY_VALUE_FUNC) &&
       !rs.__isset.func ) {
    InvalidSetDescription isd;
    isd.s = rs;
    isd.info = "You specified a function set but did not provide a function";
    isd.__isset.s = true;
    isd.__isset.info = true;
    throw isd;
  }

  if (rs.type == RST_FILTER &&
      !rs.__isset.filter) {
    InvalidSetDescription isd;
    isd.s = rs;
    isd.info = "You asked for a filter but didn't supply a string to filter on";
    isd.__isset.s = true;
    isd.__isset.info = true;
    throw isd;
  }

#ifdef DEBUG
  cout << "apply_to_set called"<<endl<<
    "\tNamespace: "<<ns<<endl<<
    "\tSET:"<<endl;
  if (rs.type == RST_ALL)
    cout << "\tRST_ALL"<<endl;
  else if (rs.type == RST_NONE)
    cout << "\tRST_NONE"<<endl;
  else if (rs.type == RST_RANGE) {
    ostringstream oo,ol;
    oo << rs.range.offset;
    ol << rs.range.limit;
    cout << "\tRST_RANGE"<<endl<<
      "\tstart_key: "<<(rs.range.__isset.start_key?rs.range.start_key:"unset")<<endl<<
      "\tend_key: "<<(rs.range.__isset.end_key?rs.range.end_key:"unset")<<endl<<
      "\toffset: "<<(rs.range.__isset.offset?oo.str():"unset")<<endl<<
      "\tlimit: "<<(rs.range.__isset.limit?ol.str():"unset")<<endl;
  }
  else if (rs.type == RST_KEY_FUNC ||
	   rs.type == RST_KEY_VALUE_FUNC) {
    cout << (rs.type == RST_KEY_FUNC?"\tRST_KEY_FUNC":"\tRST_KEY_VALUE_FUNC")<<endl<<
      "Lang: "<<(rs.func.lang==LANG_RUBY?"\tLANG_RUBY":"UNKNOWN LANG")<<endl<<
      "Func: \t"<<rs.func.func<<endl;
  }
  else if (rs.type == RST_FILTER) {
    cout << "\tRST_FILTER"<<endl<<
      "\t filter: "<<rs.filter<<endl;
  }
  else
    cout << "Unknown set type"<<endl;
#endif


  if (rs.type == RST_NONE || (rs.type == RST_RANGE && rs.range.__isset.limit && rs.range.limit <= 0))
    return;

  if (rs.type == RST_RANGE) { // validate set/start end
    if ( rs.range.__isset.start_key &&
	 rs.range.__isset.end_key &&
	 (rs.range.start_key > rs.range.end_key) ) {
      InvalidSetDescription isd;
      isd.s = rs;
      isd.info = "Your start key is greater than your end key";
      isd.__isset.s = true;
      isd.__isset.info = true;
      throw isd;
    }
  }
  
  if (bulk)
    cursor_get_flags = DB_MULTIPLE_KEY;
  else
    cursor_get_flags = 0;

  memset(&cursor_key, 0, sizeof(DBT));
  memset(&cursor_data, 0, sizeof(DBT));
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));


  if (bulk) {
    if ((cursor_data.data = malloc(BUFFER_LENGTH)) == NULL) {
      string msg("Could not malloc for bulk get: ");
      char* err = strerror(errno);
      msg.append(err);
      TException te(msg);
      throw te;
    }
    cursor_data.ulen = BUFFER_LENGTH;
    cursor_data.flags = DB_DBT_USERMEM;
  }
  else
    cursor_data.flags = DB_DBT_MALLOC;

  // get the database
  db_ptr = getDB(ns);
  txn = NULL;
  /*
  ret = db_env->txn_begin(db_env, NULL, &txn, DB_TXN_SNAPSHOT);
  if (ret != 0) {
    TException te("Could not start transaction");
    throw te;
  }
  */
  if (user_flags & DB_INIT_TXN)
    db_ptr->cursor(db_ptr, txn, &cursorp, DB_TXN_SNAPSHOT);
  else
    db_ptr->cursor(db_ptr, txn, &cursorp, 0);

  if (rs.type == RST_FILTER) 
    count = rs.filter.length();
   
  /* get the initial cursor
   *
   * start at the beginning for all, key/value funcs, and filters since
   * they have to scan everything.  start at start_key for ranges
   */

  switch (rs.type) {
  case RST_ALL:
  case RST_KEY_FUNC:
  case RST_KEY_VALUE_FUNC:
  case RST_FILTER:
    ret = cursorp->get(cursorp, &cursor_key, &cursor_data, DB_FIRST | cursor_get_flags);
    break;
  case RST_RANGE:
    if (rs.range.__isset.start_key) {
      cursor_key.data = const_cast<char*>(rs.range.start_key.c_str());
      cursor_key.size = rs.range.start_key.length();
      ret = cursorp->get(cursorp, &cursor_key, &cursor_data, DB_SET_RANGE | cursor_get_flags);
    } else { // start from the beginning
      ret = cursorp->get(cursorp, &cursor_key, &cursor_data, DB_FIRST | cursor_get_flags);
    }
    break;
  default:
    NotImplemented ni;
    ni.function_name = "get_set with specified set type";
    if (cursorp != NULL) 
      cursorp->close(cursorp); 
    if (txn!=NULL && txn->abort(txn)) 
      cerr << "Transaction abort failed"<<endl;
    if (bulk)
      free(cursor_data.data);
    throw ni;
  }

  if (ret == DB_NOTFOUND) { // nothing to return
    if (cursorp != NULL) 
      cursorp->close(cursorp); 
    if (txn!=NULL && txn->commit(txn,0)) 
      cerr << "Transaction commit failed"<<endl;
    if (invokeNone)
      // no vals, but apply function wants to know that so invoke with nulls
      (*to_apply)(apply_arg,db_ptr,cursorp,txn,NULL,NULL);
    if (bulk)
      free(cursor_data.data);
    return;
  }
  if (ret != 0) { // another error
    db_ptr->err(db_ptr,ret,"Could not get cursor");
    if (cursorp != NULL) 
      cursorp->close(cursorp); 
    if (txn!=NULL && txn->abort(txn)) 
      cerr << "Transaction abort failed"<<endl;
    if (bulk)
      free(cursor_data.data);
    return;
  }

  if (rs.type == RST_KEY_FUNC ||
      rs.type == RST_KEY_VALUE_FUNC) {
    if (rs.func.lang == LANG_RUBY) {
      int stat;
      ruby_proc = rb_eval_string_protect(rs.func.func.c_str(),&stat);
      if (!rb_respond_to(ruby_proc,call_id)) {
	InvalidSetDescription isd;
	isd.s = rs;
	isd.info = "Your ruby string does not return something that responds to 'call'";
	free(cursor_data.data); // free because we're returning
	if (cursorp != NULL)
	  cursorp->close(cursorp);
	if (txn!=NULL && txn->abort(txn)) 
	  cerr << "Transaction abort failed"<<endl;
	isd.__isset.s = true;
	isd.__isset.info = true;
	throw isd;
      }
    } else {
      NotImplemented ni;
      ni.function_name = "get_set only supports ruby functions at the moment";
      free(cursor_data.data);  // ditto
      if (cursorp != NULL) 
	cursorp->close(cursorp); 
      if (txn!=NULL && txn->abort(txn)) 
	cerr << "Transaction abort failed"<<endl;
      throw ni;
    }
  }

  if (bulk) {
    DB_MULTIPLE_INIT(p, &cursor_data);
    DB_MULTIPLE_KEY_NEXT(p,&cursor_data, retkey, retklen, retdata, retdlen);
    if (p == NULL) {
      if (cursorp != NULL) 
	cursorp->close(cursorp); 
      if (txn!=NULL && txn->commit(txn,0)) 
	cerr << "Transaction commit failed"<<endl;
      if (invokeNone)
	// no vals, but apply function wants to know that so invoke with nulls
	(*to_apply)(apply_arg,db_ptr,cursorp,txn,NULL,NULL);
      if (bulk)
	free(cursor_data.data);
      return;
    }
  }
  else {
    retkey = cursor_key.data;
    retklen = cursor_key.size;
    retdata = cursor_data.data;
    retdlen = cursor_data.size;
  }

  key.data = retkey;
  key.size = retklen;
  data.data = retdata;
  data.size = retdlen;

  for(;;) {
    // RST_ALL set
    if (rs.type == RST_ALL)
      (*to_apply)(apply_arg,db_ptr,cursorp,txn,&key,&data);

    // RST_RANGE set
    else if (rs.type == RST_RANGE) {
      if ( rs.range.__isset.end_key &&
	   (strncmp(rs.range.end_key.c_str(),(char*)key.data,key.size) < 0) ) {
	ret = DB_NOTFOUND;
	free(cursor_data.data);
	break;
      }
      if (rs.range.__isset.offset &&
	  skipped < rs.range.offset) 
	skipped++;
      else {
	(*to_apply)(apply_arg,db_ptr,cursorp,txn,&key,&data);
	count++;
      }
      if (rs.range.__isset.limit &&
	  count >= rs.range.limit) {
	ret = DB_NOTFOUND;
	free(cursor_data.data);
	break;
      }
    }

    else if (rs.type == RST_FILTER) {
      if (data.size > count &&
	  strnstr((const char*)data.data,rs.filter.c_str(),data.size)) 
	(*to_apply)(apply_arg,db_ptr,cursorp,txn,&key,&data);      
    }

    // RST_KEY_FUNC/RST_KEY_VALUE_FUNC set
    else if (rs.type == RST_KEY_FUNC || rs.type == RST_KEY_VALUE_FUNC) {
      VALUE v;
      funcall_args[0] = ruby_proc;
      funcall_args[1] = rb_str_new((const char*)(key.data),key.size);
      if (rs.type == RST_KEY_FUNC)
	funcall_args[2] = 0;
      else
	funcall_args[2] = rb_str_new((const char*)(data.data),data.size);
      v = rb_protect(rb_funcall_wrap,((VALUE)funcall_args),&rb_err);
      if (rb_err) {
	InvalidSetDescription isd;
	isd.s = rs;
	VALUE lasterr = rb_gv_get("$!");
	VALUE message = rb_obj_as_string(lasterr);
	isd.info = rb_string_value_cstr(&message);
	free(cursor_data.data);
	if (cursorp != NULL)
	  cursorp->close(cursorp);
	if (txn!=NULL && txn->abort(txn))
	  cerr << "Transaction abort failed"<<endl;
	isd.__isset.s = true;
	isd.__isset.info = true;
#ifdef DEBUG
	cerr << "Error in calling ruby function for key: "<<string((char*)key.data,key.size)<<" message: "<<isd.info<<endl;
#endif
	throw isd;
      }
      if (v == Qtrue)
	(*to_apply)(apply_arg,db_ptr,cursorp,txn,&key,&data);
      else if (v != Qfalse) {
	InvalidSetDescription isd;
	isd.s = rs;
	isd.info = "Your ruby string does not return true or false";
	free(cursor_data.data);
	if (cursorp != NULL)
	  cursorp->close(cursorp);
	if (txn!=NULL && txn->abort(txn))
	  cerr << "Transaction abort failed"<<endl;
	isd.__isset.s = true;
	isd.__isset.info = true;
	throw isd;
      }
    }

    // okay, now get next key
    if (bulk) {
      DB_MULTIPLE_KEY_NEXT(p,&cursor_data, retkey, retklen, retdata, retdlen);
      if (p == NULL) { // need to advance the cursor
	if ((ret = cursorp->get(cursorp, &cursor_key, &cursor_data, DB_NEXT | cursor_get_flags)) != 0) {
	  if (ret != DB_NOTFOUND) 
	    db_ptr->err(db_ptr,ret,"Cursor advance failed");
	  free(cursor_data.data);
	  break;
	}
	DB_MULTIPLE_INIT(p, &cursor_data);
	DB_MULTIPLE_KEY_NEXT(p,&cursor_data, retkey, retklen, retdata, retdlen);
      }
      key.data = retkey;
      key.size = retklen;
      data.data = retdata;
      data.size = retdlen;
    }
    else {
      free(cursor_data.data);
      if ((ret = cursorp->get(cursorp, &cursor_key, &cursor_data, DB_NEXT | cursor_get_flags)) != 0) {
	if (ret != DB_NOTFOUND)
	  db_ptr->err(db_ptr,ret,"Cursor advance failed");
	break;
      }
      key.data = cursor_key.data;
      key.size = cursor_key.size;
      data.data = cursor_data.data;
      data.size = cursor_data.size;
    }
  }
  if (ret != DB_NOTFOUND) {
    db_ptr->err(db_ptr,ret,"Error in apply_to_set");
  }
  
  if (cursorp != NULL)
    cursorp->close(cursorp);
  if (txn!=NULL &&
      (ret = txn->commit(txn,0)))
    db_ptr->err(db_ptr,ret,"Could not commit apply_to_set transaction");
#ifdef DEBUG
  cerr << "apply_to_set done"<<endl;
#endif
}

StorageDB::
StorageDB(int lp,
	  u_int32_t uf,
	  u_int32_t cache) :
  listen_port(lp),
  user_flags(uf) 
{
  u_int32_t env_flags = 0;
  int ret;
  u_int32_t gb;
  
  ret = db_env_create(&db_env, 0);
  if (ret != 0) {
    fprintf(stderr, "Error creating env handle: %s\n", db_strerror(ret));
    exit(-1);
  }

  env_flags = 
    DB_CREATE |     /* If the environment does not exist, create it. */
    DB_THREAD |     /* This gets used by multiple threads */
    DB_INIT_LOCK |  /* Multiple threads might write */
    DB_INIT_MPOOL|  /* Initialize the in-memory cache. */
    //DB_SYSTEM_MEM |
    DB_PRIVATE |
    user_flags;     /* Add in user specified flags */
  
  ret = db_env->set_lk_detect(db_env,DB_LOCK_DEFAULT);

  if (cache != 0) {
    gb = cache/1000;
    u_int32_t bytes = cache-(gb*1000);
    bytes*=1000000;
    cout << "Setting cache size to: "<<gb<<" gigs, "<<bytes<<" bytes"<<endl;
    ret = db_env->set_cachesize(db_env, gb, bytes, 0);
    if (ret != 0) {
      cerr << "Could not set cache size"<<endl;
      exit(-1);
    }
  }


  if (ret != 0) {
    cerr << "Could not set auto deadlock detection."<<endl;
    exit(-1);
  }

  ret = db_env->open(db_env,      /* DB_ENV ptr */
		     env_dir,    /* env home directory */
		     env_flags,  /* Open flags */
		     0);         /* File mode (default) */
  if (ret != 0) {
    fprintf(stderr, "Environment open failed: %s\n", db_strerror(ret));
    exit(-1);
  }
    
  // create the dbs rwlock
  ret = pthread_rwlock_init(&dbmap_lock, NULL);
  switch (ret) {
  case 0: // success
    break;
  case EBUSY:
    cerr << "Could not init rwlock for dbs: \
	The  implementation  has detected an attempt to reinitialize the \
	object referenced by rwlock, a previously  initialized  but  not \
	yet destroyed read-write lock."<<endl;
    exit(EXIT_FAILURE);
  case EINVAL:
    cerr << "Could not init rwlock for dbs: The value specified by attr is invalid."<<endl;
    exit(EXIT_FAILURE);
  default:
    cerr << "Some random error initing rwlock for dbs"<<endl;
    exit(EXIT_FAILURE);
  }

  // start up the thread that will listen for incomming copy/syncs
  (void) pthread_create(&listen_thread,NULL,
			run_listen,this);

  // let's init ruby
  ruby_init();
  call_id = rb_intern("call");
}

int StorageDB::
flush_log(DB* db) {
  if (!(user_flags & DB_INIT_TXN))
    db->sync(db,0);
}

void StorageDB::
closeDBs() {
  cout << "Waiting for copy/sync thread to shut down... ";
  flush(cout);
  pthread_join(listen_thread,NULL);
  cout << "[OK]"<<endl;
  map<const NameSpace,DB*>::iterator iter;
  int rc = pthread_rwlock_wrlock(&dbmap_lock); // need a write lock here
  chkLock(rc,"dbmap_lock","closing down system");
  for( iter = dbs.begin(); iter != dbs.end(); ++iter ) {
    cout << "Closing: "<<iter->first;
    if(iter->second->close(iter->second, 0))
      cout << " [FAILED]"<<endl;
    else
      cout << " [OK]"<<endl;
  }
  if (db_env != NULL) {
    rc = db_env->close(db_env, 0);
    if (rc != 0) {
      fprintf(stderr, "environment close failed: %s\n",
 	      db_strerror(rc));
    }
  }

  rc = pthread_rwlock_unlock(&dbmap_lock);
  chkLock(rc,"dbmap_lock","unlock after cloing down");
}


void StorageDB::
get(Record& _return, const NameSpace& ns, const RecordKey& key) {
  DB* db_ptr;
  DBT db_key, db_data;
  int retval;

  if (!responsible_for_key(ns,key)) {
    NotResponsible nse;
    get_responsibility_policy(nse.policy,ns);
    throw nse;
  }

  db_ptr = getDB(ns);

  /* Zero out the DBTs before using them. */
  memset(&db_key, 0, sizeof(DBT));
  memset(&db_data, 0, sizeof(DBT));
  db_data.flags = DB_DBT_MALLOC;

  db_key.data = const_cast<char*>(key.c_str());
  db_key.size = key.length();

  retval = db_ptr->get(db_ptr, NULL, &db_key, &db_data, 0);
  _return.key = key;
  if (!retval) {  // return 0 means success
    _return.__isset.value = true;
    _return.value.assign((const char*)db_data.data,db_data.size);
    free(db_data.data);
  }
}

void StorageDB::
get_set(std::vector<Record> & _return, const NameSpace& ns, const RecordSet& rs) {
  if (!responsible_for_set(ns,rs)) {
    NotResponsible nse;
    get_responsibility_policy(nse.policy,ns);
    nse.__isset.policy = true;
    throw nse;
  }
  // don't invoke if no vals, but use bulk retrieval
  apply_to_set(ns,rs,apply_get,&_return,false,true);
}

bool StorageDB::
remove_set(const NameSpace& ns, const RecordSet& rs) {
  apply_to_set(ns,rs,apply_del,NULL);
  return true;
}

int32_t StorageDB::
count_set(const NameSpace& ns, const RecordSet& rs) {
  int r = 0;
  // don't invoke if no vals, but use bulk retrieval
  apply_to_set(ns,rs,apply_inc,&r,false,true);
  return r;
}

bool StorageDB::
put(const NameSpace& ns, const Record& rec) {
  DB* db_ptr;
  DBT key, data;
  int ret;

#ifdef DEBUG
  cout << "Put called:"<<endl<<
    "Namespace:\t"<<ns<<endl<<
    "Key:\t"<<rec.key<<endl<<
    "Value:\t"<<rec.value<<endl;
  if (rec.value == "")
    cout << "is empty string"<<endl;
  if (rec.__isset.value)
    cout << "val is set"<<endl;
  else
    cout << "val is NOT set"<<endl;
#endif

  if (!responsible_for_key(ns,rec.key)) {
    NotResponsible nse;
    get_responsibility_policy(nse.policy,ns);
    throw nse;
  }

  db_ptr = getDB(ns);
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  key.data = const_cast<char*>(rec.key.c_str());
  key.size = rec.key.length();

  if (!rec.__isset.value) { // really a delete
    ret = db_ptr->del(db_ptr,NULL,&key,0);
  }
  else {
    data.data = const_cast<char*>(rec.value.c_str());
    data.size = rec.value.length();
    ret = db_ptr->put(db_ptr, NULL, &key, &data, 0);
  }

  /* gross that we're going in and out of c++ strings,
     consider storing the whole string object */
  
  if (ret) {
    db_ptr->err(db_ptr,ret,"Put failed");
    return false;
  }

  ret = flush_log(db_ptr);
  if (ret) {
    db_ptr->err(db_ptr,ret,"Flush failed");
    return false;
  }
    
  return true;
}

bool StorageDB::
set_responsibility_policy(const NameSpace& ns, const RecordSet& policy) {
  // TODO:  should probably lock to make this atomic

#ifdef DEBUG
  cerr << "Setting resp policy"<<endl;
#endif
    
  if (policy.type == RST_KEY_VALUE_FUNC) { // illegal
    InvalidSetDescription isd;
    isd.s = policy;
    isd.info = "Cannot specify a key value function as a responsibility function";
    isd.__isset.s = true;
    isd.__isset.info = true;
    throw isd;
  }

  if (policy.__isset.range &&
      policy.__isset.func) {
    InvalidSetDescription isd;
    isd.s = policy;
    isd.info = "You specified both a range and function in your policy";
    isd.__isset.s = true;
    isd.__isset.info = true;
    throw isd;
  }

  if (policy.type == RST_RANGE &&
      !policy.__isset.range) {
    InvalidSetDescription isd;
    isd.s = policy;
    isd.info = "You specified a range set but did not provide a range description";
    isd.__isset.s = true;
    isd.__isset.info = true;
    throw isd;
  }

  if ( policy.type == RST_RANGE &&
       (policy.range.__isset.offset ||
	policy.range.__isset.limit) ) {
    InvalidSetDescription isd;
    isd.s = policy;
    isd.info = "offset/limit don't make sense for responsibility policies";
    isd.__isset.s = true;
    isd.__isset.info = true;
    throw isd;
  }

  if ( policy.type == RST_KEY_FUNC  &&
       !policy.__isset.func ) {
    InvalidSetDescription isd;
    isd.s = policy;
    isd.info = "You specified a function set but did not provide a function";
    isd.__isset.s = true;
    isd.__isset.info = true;
    throw isd;
  }

  if (policy.type == RST_KEY_FUNC) {
    int rb_err;
    VALUE funcall_args[3];
    VALUE ruby_proc = rb_eval_string_protect(policy.func.func.c_str(),&rb_err);
    if (!rb_respond_to(ruby_proc,call_id)) {
      InvalidSetDescription isd;
      isd.s = policy;
      isd.info = "Your ruby string for your responsiblity policy does not return something that responds to 'call'";
      isd.__isset.s = true;
      isd.__isset.info = true;
      throw isd;
    }
  }

  // okay, let's serialize
  ostringstream os;
  os << policy.type;
  switch (policy.type) {
  case RST_RANGE:
    os << " "<<
      policy.range.start_key<<" "<<
      policy.range.end_key;
    break;
  case RST_KEY_FUNC:
    os << " "<<policy.func.lang<< " "<<policy.func.func;
    break;
  }


#ifdef DEBUG
  cout << "Setting resp_pol:"<<endl<<
    "\tType: "<<policy.type<<endl<<
    "\tLang: "<<LANG_RUBY<<endl<<
    "\tSK: "<<policy.range.start_key<<endl<<
    "\tEK: "<<policy.range.end_key<<endl<<
    "\tFUNC: "<<policy.func.func<<endl<<
    "serialized string:"<<endl<<
    os.str()<<endl;
#endif


  DB* mdDB = getDB("storage_metadata");
  DBT db_key, db_data;
  int retval;


  /* Zero out the DBTs before using them. */


  memset(&db_key, 0, sizeof(DBT));
  memset(&db_data, 0, sizeof(DBT));

  db_key.data = const_cast<char*>(ns.c_str());
  db_key.size = ns.length();

  char buf[os.str().length()+1];
  sprintf(buf,"%s",os.str().c_str());
  db_data.data = buf;
  db_data.size = os.str().length();

#ifdef DEBUG
  printf("About to put: %s\n",buf);
#endif

  retval = mdDB->put(mdDB, NULL, &db_key, &db_data, 0);
  retval |= flush_log(mdDB);

  if (!retval)
    return true;
  TException te("Something went wrong storing your responsibility policy");
  throw te;
}

void StorageDB::
get_responsibility_policy(RecordSet& _return, const NameSpace& ns) {
  DB* mdDB = getDB("storage_metadata");
  DBT db_key, db_data;
  int retval;


  /* Zero out the DBTs before using them. */

    
  memset(&db_key, 0, sizeof(DBT));
  memset(&db_data, 0, sizeof(DBT));

  db_key.data = const_cast<char*>(ns.c_str());
  db_key.size = ns.length();
  db_data.flags = DB_DBT_MALLOC;


  retval = mdDB->get(mdDB, NULL, &db_key, &db_data, 0);
  if (!retval) { // okay, something was there
    string pol((char*)db_data.data,db_data.size);
#ifdef DEBUG
    //cout << "deserialized str:"<<endl<<
    //pol<<endl;
#endif
    int type;
    istringstream is(pol,istringstream::in);
    is >> type;
    _return.type = (SCADS::RecordSetType)type;
    switch (_return.type) {
    case RST_RANGE:
      is >> _return.range.start_key>>_return.range.end_key;
      _return.__isset.range = true;
      _return.range.__isset.start_key = true;
      _return.range.__isset.end_key = true;
      break;
    case RST_KEY_FUNC: {
      int lang;
      stringbuf sb;
      is >> lang >> (&sb);
      _return.func.lang = (SCADS::Language)lang;
      _return.func.func.assign(sb.str());
      _return.__isset.func = true;
      _return.func.__isset.lang = true;
      _return.func.__isset.func = true;
    }
      break;
    }

#ifdef DEBUG
    /*cout << "Deserizalized resp_pol:"<<endl<<
      "\tType: "<<_return.type<<endl<<
      "\tLang: "<<_return.func.lang<<endl<<
      "\tSK: "<<_return.range.start_key<<endl<<
      "\tEK: "<<_return.range.end_key<<endl<<
      "\tFUNC: "<<_return.func.func<<endl;*/
#endif
    free(db_data.data);
  } else if (retval == DB_NOTFOUND) {
    // not found means RST_ALL
    _return.type = RST_ALL;
  } else {
    TException te("Error getting responsibility policy");
    throw te;
  }
}

/*
RecordSet& RecordSet::operator=(const RecordSet &rhs) const {
  type = rhs.type; 
  
  if (rhs.type == RST_RANGE) { // copy range params
    __isset.range = true;
    __isset.func = false;
    if (rhs.range.__isset.start_key) {
      range.start_key.assign(rhs.range.start_key);
      range.__isset.start_key = true;
    }
    else 
      range.__isset.start_key = false;
    
    if (rhs.range.__isset.end_key) {
      range.end_key.assign(rhs.range.end_key);
      range.__isset.end_key = true;
    }
    else
      range.__isset.end_key = false;
  }
  
  if (rhs.type == RST_KEY_FUNC) { // copy func info
    __isset.range = false;
    __isset.func = true;
    func.lang = rhs.func.lang;
    func.func.assign(rhs.func.func);
  }

  return *this;  // Return a reference to myself.
}
*/

enum ServerType {
  ST_SIMPLE,
  ST_POOL,
  ST_THREAD,
  ST_NONBLOCK
};
enum ServerType serverType;

// our server, saved so we can
// stop at exit
TSimpleServer* simpleServer;
TThreadPoolServer* poolServer;
TThreadedServer* threadedServer;
TNonblockingServer *nonblockingServer;

static shared_ptr<StorageDB> storageDB;

int workerCount,lp,cache;

char stopping = 0;

static
void usage(const char* prgm) {
  fprintf(stderr, "Usage: %s [-p PORT] [-d DIRECTORY] [-t TYPE] [-n NUM] [-l PORT]\n\
Starts the BerkeleyDB storage layer.\n\n\
  -p PORT\tRun the thrift server on port PORT\n\
         \tDefault: 9090\n\
  -d DIRECTORY\tStore data files in directory DIRECTORY\n\
              \tDefault: .\n\
  -t TYPE\tWhat thrift server type to run simple,threaded, nonblocking or pooled\n\
         \tDefault: pooled\n\
  -n NUM\tHow many threads to use for pooled server.\n\
        \tIgnored if not running pooled.\n\
        \tDefault: 10\n\
  -l PORT\tStart sync/move listen thread on port PORT\n\
	\tDefault: 9091\n\
  -x\t\tUse transactions.\n\
	\t(Once an env has txn support enabled it cannot be disabled, and vice versa)\n\
  -L\t\tDon't do write ahead logging.\n\
	\t(Logging is always on if you use transactions)\n\
  -h\t\tShow this help\n\n",
	  prgm);
}

static 
void parseArgs(int argc, char* argv[]) {
  int opt;
  char dtxn = 0;
  char dlog = 1;
  env_dir = 0;
  port = 9090;
  serverType = ST_POOL;
  workerCount = 10;
  lp = 9091;
  cache = 500;

  while ((opt = getopt(argc, argv, "hxLp:d:t:n:l:c:")) != -1) {
    switch (opt) {
    case 'p':
      port = atoi(optarg);
      break;
    case 'd':
      env_dir = (char*)malloc(sizeof(char)*(strlen(optarg)+1));
      strcpy(env_dir,optarg);
      break;
    case 'c':
      cache = atoi(optarg);
      break;	
    case 't':
      if (!strcmp(optarg,"simple"))
	serverType = ST_SIMPLE;
      else if (!strcmp(optarg,"threaded"))
	serverType = ST_THREAD;
      else if (!strcmp(optarg,"pooled"))
	serverType = ST_POOL;
      else if (!strcmp(optarg,"nonblocking"))
	serverType = ST_NONBLOCK;
      else {
	cerr << "argument to -t must be one of: simple, threaded, nonblocking or pooled"<<endl;
	exit(EXIT_FAILURE);
      }
      break;
    case 'n':
      workerCount = atoi(optarg);
      break;
    case 'l':
      lp = atoi(optarg);
      break;
    case 'x':
      dtxn = 1;
      break;
    case 'L':
      dlog = 0;
      break;
    case 'h':
    default: /* '?' */
      usage(argv[0]);
      exit(EXIT_FAILURE);
    }
  }
  
  if (dtxn)
    uf = DB_INIT_TXN | DB_INIT_LOG | DB_MULTIVERSION;
  else if(dlog)
    uf = DB_INIT_LOG;
  else
    uf = 0;

  if (!env_dir) {
    cerr << "Warning: -d not specified, running in local dir"<<endl;
    env_dir = (char*)malloc(strlen(".")+1);
    strcpy(env_dir,".");
  }
}



static void ex_program(int sig) {
  cout << "\n\nShutting down."<<endl;
  stopping = 1;
  switch (serverType) {
  case ST_SIMPLE:
    simpleServer->stop();
    break;
  case ST_POOL:
    poolServer->stop();
    break;
  case ST_THREAD:
    threadedServer->stop();
    break;
  case ST_NONBLOCK:
    nonblockingServer->stop();
    break;
  default:
    cerr << "Warning, don't know what kind of server you're running, not calling stop."<<endl;;
  }
  storageDB->closeDBs();
  exit(0);
}


int main(int argc, char **argv) {
  parseArgs(argc,argv);

  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
  shared_ptr<StorageDB> handler(new StorageDB(lp,uf,cache));
  shared_ptr<TProcessor> processor(new StorageProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());

#ifdef DEBUG
  cout << "Running in debug mode"<<endl;
#endif

  storageDB = handler;
  signal(SIGINT, ex_program);
  signal(SIGTERM, ex_program);

  if (!storageDB->isTXN()) {
    cout << "Running without transactions"<<endl;
    if (uf & DB_INIT_LOG)
      cout << "Running with write ahead logging"<<endl;
  }

  switch (serverType) {
  case ST_SIMPLE: {
    TSimpleServer server(processor,
			 serverTransport,
			 transportFactory,
			 protocolFactory);
    simpleServer = &server;
    printf("Starting simple server...\n");
    server.serve();
  }
    break;
  case ST_POOL: {
    shared_ptr<ThreadManager> threadManager =
      ThreadManager::newSimpleThreadManager(workerCount);
    shared_ptr<PosixThreadFactory> threadFactory =
      shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
    TThreadPoolServer server(processor,
			     serverTransport,
			     transportFactory,
			     protocolFactory,
			     threadManager);
    poolServer = &server;
    printf("Starting pooled server with %i worker threads...\n",workerCount);
    server.serve();
  }
    break;
  case ST_THREAD: {
    TThreadedServer server(processor,
			   serverTransport,
			   transportFactory,
			   protocolFactory);
    threadedServer = &server;
    printf("Starting threaded server...\n");
    server.serve();
  }
    break;
  case ST_NONBLOCK: {
    TNonblockingServer server(processor,
			      protocolFactory,
			      port);
    nonblockingServer = &server;
    printf("Starting nonblocking server...\n");
	fflush(stdout);
    server.serve();
  }
    break;
  default:
    cerr << "Invalid server type, nothing to start"<<endl;
    exit(EXIT_FAILURE);
  }
  
  printf("done.\n");
  return 0;
}
