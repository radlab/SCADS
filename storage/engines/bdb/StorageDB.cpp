//bdb scads storage engine
#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>

#include <iostream>
#include <stdexcept>
#include <sstream>
#include <map>

#include <db.h>

#include <signal.h>

#include "gen-cpp/Storage.h"

#include "ruby.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

using namespace boost;

using namespace SCADS;

// program wide config stuff
static char* env_dir;
static int port;

// some ruby stuff
static int did_ruby_init,rb_err;
static ID call_id = 0;
static VALUE funcall_args[3];

// this is gross, but it's the only way to avoid a segfault
// if the passed procedure has a syntax error
VALUE rb_funcall_wrap(VALUE vargs) {
  VALUE* args = (VALUE*)vargs;
  return
    (args[2] == 0)? 
    rb_funcall(args[0], call_id, 1, args[1]):
    rb_funcall(args[0], call_id, 2, args[1], args[2]);
}

// set application functions
void apply_get(void* vec,DB* db, void* k, void* d) {
  vector<Record>* _return = (std::vector<Record>*)vec;
  Record r;
  DBT *key,*data;
  key = (DBT*)k;
  data = (DBT*)d;
  r.key = string((char*)key->data);
  r.value = string((char*)data->data);
  _return->push_back(r);
}

void apply_del(void* v, DB* db, void* k, void* d) {
  DBT *key = (DBT*)k;
  db->del(db,NULL,key,0);
}


class StorageDB : public StorageIf {

private:
  map<const NameSpace,DB*> dbs;
  map<const NameSpace,RecordSet*> key_policies;
  DB_ENV *db_env;

private:
  int open_database(DB **dbpp,       /* The DB handle that we are opening */
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
    open_flags = DB_CREATE;

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
      return(ret);
    }
                                                                                                                               
    return (0);
  }

  void do_ruby_init() {
    ruby_init();
    call_id = rb_intern("call");
    did_ruby_init = 1;
  }

  bool responsible_for_key(const NameSpace& ns, const RecordKey& key) {
    RecordSet rs;
    get_responsibility_policy(rs,ns);

    if (rs.type == RST_ALL) 
      return true;
    if (rs.type == RST_NONE)
      return false;

    if (rs.type == RST_RANGE) {
      if (key >= rs.range.start_key &&
	  key <= rs.range.end_key)
	return true;
      else
	return false;
    }

    if (rs.type == RST_KEY_FUNC) {
      if (!did_ruby_init) 
	do_ruby_init();
      VALUE ruby_proc = rb_eval_string_protect(rs.func.func.c_str(),&rb_err);
      if (!rb_respond_to(ruby_proc,call_id)) {
	InvalidSetDescription isd;
	isd.s = rs;
	isd.info = "Your ruby string for your responsiblity policy does not return something that responds to 'call'";
	throw isd;
      }
      VALUE v;
      funcall_args[0] = ruby_proc;
      funcall_args[1] = rb_str_new2((const char*)(key.c_str()));
      funcall_args[2] = 0;
      v = rb_protect(rb_funcall_wrap,((VALUE)funcall_args),&rb_err);
      if (rb_err) {
	InvalidSetDescription isd;
	isd.s = rs;
	VALUE lasterr = rb_gv_get("$!");
	VALUE message = rb_obj_as_string(lasterr);
	isd.info = rb_string_value_cstr(&message);
	throw isd;
      }
      return (v == Qtrue);
    }

    return false; // if we don't understand, we'll say no
  }

  bool responsible_for_set(const NameSpace& ns, const RecordSet& rs) {
    RecordSet policy;
    get_responsibility_policy(policy,ns);
    
    if (policy.type == RST_ALL || policy.type == RST_KEY_FUNC)
      return true;
    if (rs.type == RST_NONE)
      return false;
    
    if (rs.type == RST_RANGE) {
      if ( (rs.range.start_key >= policy.range.start_key) &&
	   (rs.range.end_key <= policy.range.end_key) )
	return true;
      else
	return false;
    }
    
    return false; // if we don't understand, we'll say no
  }

  DB* getDB(const NameSpace& ns) {
    map<const NameSpace,DB*>::iterator it;
    it = dbs.find(ns);
    if (it == dbs.end()) { // haven't opened this db yet
      DB* db;
      open_database(&db,ns.c_str(),"storage.bdb",env_dir,stderr);
      dbs[ns] = db;
      return db;
    }
    return dbs[ns];
  }


  void apply_to_set(const NameSpace& ns, const RecordSet& rs,
		    void(*to_apply)(void*,DB*,void*,void*),void* apply_arg) {
    DB* db_ptr;
    DBC *cursorp;
    DBT key, data;
    int ret,count=0;
    u_int32_t cursor_flags;
    VALUE ruby_proc;

    if (rs.type == RST_NONE || (rs.type == RST_RANGE && rs.range.limit <= 0))
      return;
    
    db_ptr = getDB(ns);
    db_ptr->cursor(db_ptr, NULL, &cursorp, 0); 

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
   
    switch (rs.type) {
    case RST_ALL:
    case RST_KEY_FUNC:
    case RST_KEY_VALUE_FUNC:
      ret = cursorp->get(cursorp, &key, &data, DB_FIRST);
      break;
    case RST_RANGE:
      key.data = const_cast<char*>(rs.range.start_key.c_str());
      key.size = rs.range.start_key.length()+1;
      ret = cursorp->get(cursorp, &key, &data, DB_SET_RANGE);
      break;
    default:
      NotImplemented ni;
      ni.function_name = "get_set with specified set type";
      if (cursorp != NULL) 
	cursorp->close(cursorp); 
      throw ni;
    }

    if (ret == DB_NOTFOUND) { // nothing to return
      if (cursorp != NULL) 
	cursorp->close(cursorp); 
      return;
    }
    if (ret != 0) { // another error
      cerr << "Error in first cursor get"<<endl;
      if (cursorp != NULL) 
	cursorp->close(cursorp); 
      return;
    }

    if (rs.type == RST_KEY_FUNC ||
	rs.type == RST_KEY_VALUE_FUNC) {
      if (rs.func.lang == LANG_RUBY) {
	if (!did_ruby_init) 
	  do_ruby_init();
	int stat;
	ruby_proc = rb_eval_string_protect(rs.func.func.c_str(),&stat);
	if (!rb_respond_to(ruby_proc,call_id)) {
	  InvalidSetDescription isd;
	  isd.s = rs;
	  isd.info = "Your ruby string does not return something that responds to 'call'";
	  if (cursorp != NULL)
	    cursorp->close(cursorp);
	  throw isd;
	}
      } else {
	NotImplemented ni;
	ni.function_name = "get_set only supports ruby functions at the moment";
	if (cursorp != NULL) 
	  cursorp->close(cursorp); 
	throw ni;
      }
    }

    if (rs.type == RST_ALL ||
	rs.type == RST_RANGE) {
      (*to_apply)(apply_arg,db_ptr,&key,&data);
      count++;
    }
    else if (rs.type == RST_KEY_FUNC || RST_KEY_VALUE_FUNC) {
      VALUE v;
      funcall_args[0] = ruby_proc;
      funcall_args[1] = rb_str_new2((const char*)(key.data));
      if (rs.type == RST_KEY_FUNC)
	funcall_args[2] = 0;
      else
	funcall_args[2] = rb_str_new2((const char*)(data.data));
      v = rb_protect(rb_funcall_wrap,((VALUE)funcall_args),&rb_err);
      if (rb_err) {
	InvalidSetDescription isd;
	isd.s = rs;
	VALUE lasterr = rb_gv_get("$!");
	VALUE message = rb_obj_as_string(lasterr);
	isd.info = rb_string_value_cstr(&message);
	if (cursorp != NULL)
	  cursorp->close(cursorp);
	throw isd;
      }
      if (v == Qtrue)
	(*to_apply)(apply_arg,db_ptr,&key,&data);
      else if (v != Qfalse) {
	InvalidSetDescription isd;
	isd.s = rs;
	isd.info = "Your ruby string does not return true or false";
	if (cursorp != NULL)
	  cursorp->close(cursorp);
	throw isd;
      }
    }

    if (rs.type == RST_RANGE && count > rs.range.limit) {
      if (cursorp != NULL) 
	cursorp->close(cursorp); 
      return;
    }

    while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
      // RST_ALL set
      if (rs.type == RST_ALL)
	(*to_apply)(apply_arg,db_ptr,&key,&data);

      // RST_RANGE set
      else if (rs.type == RST_RANGE) {
	if (strcmp(rs.range.end_key.c_str(),(char*)key.data) < 0) {
	  ret = DB_NOTFOUND;
	  break;
	}
	(*to_apply)(apply_arg,db_ptr,&key,&data);
	count++;
	if (count > rs.range.limit) {
	  ret = DB_NOTFOUND;
	  break;
	}
      }

      // RST_KEY_FUNC/RST_KEY_VALUE_FUNC set
      else if (rs.type == RST_KEY_FUNC || rs.type == RST_KEY_VALUE_FUNC) {
	VALUE v;
	funcall_args[0] = ruby_proc;
	funcall_args[1] = rb_str_new2((const char*)(key.data));
	if (rs.type == RST_KEY_FUNC)
	  funcall_args[2] = 0;
	else
	  funcall_args[2] = rb_str_new2((const char*)(data.data));
	v = rb_protect(rb_funcall_wrap,((VALUE)funcall_args),&rb_err);
	if (rb_err) {
	  InvalidSetDescription isd;
	  isd.s = rs;
	  VALUE lasterr = rb_gv_get("$!");
	  VALUE message = rb_obj_as_string(lasterr);
	  isd.info = rb_string_value_cstr(&message);
	  if (cursorp != NULL)
	    cursorp->close(cursorp);
	  throw isd;
	}
	if (v == Qtrue)
	  (*to_apply)(apply_arg,db_ptr,&key,&data);
	else if (v != Qfalse) {
	  InvalidSetDescription isd;
	  isd.s = rs;
	  isd.info = "Your ruby string does not return true or false";
	  if (cursorp != NULL)
	    cursorp->close(cursorp);
	  throw isd;
	}
      }
    }
    if (ret != DB_NOTFOUND) {
      /* Error handling goes here */
      cerr << "Umm, error in get_set"<<endl;
    }
  
    if (cursorp != NULL) 
      cursorp->close(cursorp); 
  }


public:

  StorageDB() {
    u_int32_t env_flags;
    int ret;
    
    ret = db_env_create(&db_env, 0);
    if (ret != 0) {
      fprintf(stderr, "Error creating env handle: %s\n", db_strerror(ret));
      exit(-1);
    }

    env_flags = DB_CREATE |    /* If the environment does not exist, create it. */
      DB_INIT_MPOOL;           /* Initialize the in-memory cache. */
    
    ret = db_env->open(db_env,      /* DB_ENV ptr */
		       env_dir,    /* env home directory */
		       env_flags,  /* Open flags */
		       0);         /* File mode (default) */
    if (ret != 0) {
      fprintf(stderr, "Environment open failed: %s\n", db_strerror(ret));
      exit(-1);
    }
  }

  void close() {
    map<const NameSpace,DB*>::iterator iter;
    for( iter = dbs.begin(); iter != dbs.end(); ++iter ) {
      cout << "Closing: "<<iter->first;
      if(iter->second->close(iter->second, 0))
	cout << " [FAILED]"<<endl;
      else
	cout << " [OK]"<<endl;
    }
  }

  void get(Record& _return, const NameSpace& ns, const RecordKey& key) {
    DB* db_ptr;
    DBT db_key, db_data;

    if (!responsible_for_key(ns,key)) {
      NotResponsible nse;
      get_responsibility_policy(nse.policy,ns);
      throw nse;
    }

    db_ptr = getDB(ns);

    /* Zero out the DBTs before using them. */
    memset(&db_key, 0, sizeof(DBT));
    memset(&db_data, 0, sizeof(DBT));

    db_key.data = const_cast<char*>(key.c_str());
    db_key.size = key.length()+1;

    db_ptr->get(db_ptr, NULL, &db_key, &db_data, 0);
    string ret((char*)db_data.data);
    _return.value = ret;
  }

  void get_set(std::vector<Record> & _return, const NameSpace& ns, const RecordSet& rs) {
    if (!responsible_for_set(ns,rs)) {
      NotResponsible nse;
      get_responsibility_policy(nse.policy,ns);
      throw nse;
    }
    apply_to_set(ns,rs,apply_get,&_return);
  }

  bool sync_set(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy) {
    return false;
  }

  bool copy_set(const NameSpace& ns, const RecordSet& rs, const Host& h) {
    return false;
  }

  bool remove_set(const NameSpace& ns, const RecordSet& rs) {
    apply_to_set(ns,rs,apply_del,NULL);
    return true;
  }

  bool put(const NameSpace& ns, const Record& rec) {
    DB* db_ptr;
    DBT key, data;
    int ret;
    db_ptr = getDB(ns);
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    key.data = const_cast<char*>(rec.key.c_str());
    key.size = rec.key.length()+1;
    data.data = const_cast<char*>(rec.value.c_str());
    data.size = rec.value.length()+1;
    ret = db_ptr->put(db_ptr, NULL, &key, &data, 0);

    /* gross that we're going in and out of c++ strings,
       consider storing the whole string object */

    switch (ret) {
    case DB_KEYEXIST:
      db_ptr->err(db_ptr, ret,"Put failed because key %s already exists", key.data);
      break;
    case DB_LOCK_DEADLOCK:
      db_ptr->err(db_ptr, ret,"Put failed because a transactional database environment operation was selected to resolve a deadlock.");
      break;
    case DB_LOCK_NOTGRANTED:
      db_ptr->err(db_ptr, ret,"Put failed because a Berkeley DB Concurrent Data Store database environment configured for lock timeouts was unable to grant a lock in the allowed time.");
      break;
    case DB_REP_HANDLE_DEAD:
      db_ptr->err(db_ptr, ret,"Put failed because the database handle has been invalidated because a replication election unrolled a committed transaction.");
      break;
    case DB_REP_LOCKOUT:
      db_ptr->err(db_ptr, ret,"Put failed because the operation was blocked by client/master synchronization.");
      break;
      /*
    case EACCES:
      db_ptr->err(db_ptr, ret,"Put failed because an attempt was made to modify a read-only database.");
      break;
    case INVAL:
      db_ptr->err(db_ptr, ret,"Put failed because an attempt was made to add a record to a fixed-length database that was too large to fit; an attempt was made to do a partial put; an attempt was made to add a record to a secondary index; or if an invalid flag value or parameter was specified.");
      break;
    case ENOSPC:
      db_ptr->err(db_ptr, ret,"Put failed because a btree exceeded the maximum btree depth (255).");
      break;
      */
    default:
      break;
    }
    
    if (!ret) 
      return true;
    return false;
  }

  bool set_responsibility_policy(const NameSpace& ns, const RecordSet& policy) {
    // TODO:  should probably lock to make this atomic
    
    // first let's see if there's an existing one
    map<const NameSpace,RecordSet*>::iterator it;
    it = key_policies.find(ns);
    RecordSet *rs;
    if (it == key_policies.end()) { // haven't set this policy yet, make a new one
      rs = new RecordSet();
      key_policies[ns] = rs;
    }
    else
      rs = it->second;

    // really just an int, so no need to delete
    rs->type = policy.type; 

    if (policy.type == RST_RANGE) { // copy range params
      rs->range.start_key.assign(policy.range.start_key);
      rs->range.end_key.assign(policy.range.end_key);
      rs->range.offset = policy.range.offset;
      rs->range.limit = policy.range.limit;
    }

    if (policy.type == RST_KEY_FUNC) { // copy func info
      rs->func.lang = policy.func.lang;
      rs->func.func.assign(policy.func.func);
    }

    if (policy.type == RST_KEY_VALUE_FUNC) { // illegal
      InvalidSetDescription isd;
      isd.s = policy;
      isd.info = "Cannot specify a key value function as a responsibility function";
      throw isd;
    }
    
    return true;
  }

  void get_responsibility_policy(RecordSet& _return, const NameSpace& ns) {
    map<const NameSpace,RecordSet*>::iterator it;
    it = key_policies.find(ns);
    RecordSet *rs;
    if (it == key_policies.end()) { // haven't set this policy yet
      rs = new RecordSet();
      rs->type = RST_ALL; // policies default to ALL
      key_policies[ns] = rs;
    }
    else
      rs = it->second;
    _return = *rs;
  }

};

static shared_ptr<StorageDB> storageDB;

static
void usage(const char* prgm) {
  fprintf(stderr, "Usage: %s [-p PORT] [-d DIRECTORY]\n\
Starts the BerkeleyDB storage layer.\n\n\
  -p PORT\tRun the thrift server on port PORT (Default: 9090)\n\
  -d DIRECTORY\tStore data files in directory DIRECTORY (Default: .)\n\
  -h\t\tShow this help\n\n",
	  prgm);
}

static 
void parseArgs(int argc, char* argv[]) {
  int opt;
  env_dir = 0;
  port = 9090;

  while ((opt = getopt(argc, argv, "hp:d:")) != -1) {
    switch (opt) {
    case 'p':
      port = atoi(optarg);
      break;
    case 'd':
      env_dir = (char*)malloc(sizeof(char)*(strlen(optarg)+1));
      strcpy(env_dir,optarg);
      break;
    case 'h':
    default: /* '?' */
      usage(argv[0]);
      exit(EXIT_FAILURE);
    }
  }
  
  if (!env_dir) {
    fprintf(stderr,"Warning: -d not specified, running in local dir\n");
    env_dir = (char*)malloc(strlen(".")+1);
    strcpy(env_dir,".");
  }
}


static void ex_program(int sig) {
  cout << "\n\nShutting down."<<endl;
  storageDB->close();
  exit(0);
}


int main(int argc, char **argv) {
  parseArgs(argc,argv);

  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
  shared_ptr<StorageDB> handler(new StorageDB());
  shared_ptr<TProcessor> processor(new StorageProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());

  did_ruby_init = 0;

  storageDB = handler;
  signal(SIGINT, ex_program);

  TSimpleServer server(processor,
                       serverTransport,
                       transportFactory,
                       protocolFactory);

  /**
   * Or you could do one of these

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

  TThreadedServer server(processor,
                         serverTransport,
                         transportFactory,
                         protocolFactory);
  */

  printf("Starting the server...\n");
  server.serve();
  printf("done.\n");
  return 0;
}
