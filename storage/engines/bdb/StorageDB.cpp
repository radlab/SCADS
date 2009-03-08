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

class StorageDB : public StorageIf {

private:
  map<const NameSpace,DB*> dbs;
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
      fprintf(stderr, "Environment open failed: %s", db_strerror(ret));
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
    char data_buf[256];

    db_ptr = getDB(ns);

    /* Zero out the DBTs before using them. */
    memset(&db_key, 0, sizeof(DBT));
    memset(&db_data, 0, sizeof(DBT));

    db_key.data = const_cast<char*>(key.c_str());
    db_key.size = key.length()+1;

    db_data.data = data_buf;
    db_data.ulen = 256;
    db_data.flags = DB_DBT_USERMEM;
    db_ptr->get(db_ptr, NULL, &db_key, &db_data, 0);
    string ret(data_buf);
    _return.value = ret;
  }

  void get_set(std::vector<Record> & _return, const NameSpace& ns, const RecordSet& rs) {

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
    return false;
  }

  void get_responsibility_policy(RecordSet& _return, const NameSpace& ns) {

  }

  bool sync_set(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy) {
    return false;
  }

  bool copy_set(const NameSpace& ns, const RecordSet& rs, const Host& h) {
    return false;
  }

  bool remove_set(const NameSpace& ns, const RecordSet& rs) {
    return false;
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
  cout << "Shutting down."<<endl;
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
