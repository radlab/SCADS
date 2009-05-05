#ifndef STORAGEDB_H
#define STORAGEDB_H

#include <db.h>
#include "gen-cpp/Storage.h"
#include "MerkleDB.h"
#include "ruby.h"

#ifndef MSG_MORE
#define MSG_MORE 0
#endif

#define NODE_DATA   0
#define NODE_MERKLE 1
#define MERKLE_NO   2
#define MERKLE_YES  3
#define MERKLE_MARK 4

#define dbt_string(dbt) std::string((char*)(dbt)->data,(dbt)->size)

using namespace std;

VALUE rb_funcall_wrap(VALUE vargs);

namespace SCADS {

void* run_listen(void *args);

class StorageDB : public StorageIf {

private:
  DB_ENV *db_env;
  pthread_rwlock_t dbmap_lock;
  map<const NameSpace,DB*> dbs;
  map<const NameSpace,MerkleDB*> merkle_dbs;
  pthread_t listen_thread,flush_threadp;
  pthread_cond_t flush_cond;
  pthread_mutex_t flush_tex, flushing_tex;
  int listen_port;
  u_int32_t user_flags;

public:
  StorageDB(int,u_int32_t,u_int32_t);

private:
  int open_database(DB **dbpp,                  /* The DB handle that we are opening */
		    const char *file_name,     /* The file in which the db lives */
		    const char *program_name,  /* Name of the program calling this function */
		    const char *env_dir,       /* environment dir */
		    FILE *error_file_pointer);  /* File where we want error messages sent */

  bool responsible_for_key(const NameSpace& ns, const RecordKey& key);
  bool responsible_for_set(const NameSpace& ns, const RecordSet& rs);

  bool simple_sync(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy);
  bool merkle_sync(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy);

public:
  void apply_to_set(const NameSpace& ns, const RecordSet& rs,
		    void(*to_apply)(void*,DB*,DBC*,DB_TXN*,void*,void*),void* apply_arg,
		    bool invokeNone = false, bool bulk = false);

  int flush_log(DB*);

  DB* getDB(const NameSpace& ns);
  MerkleDB* getMerkleDB(const NameSpace& ns, bool nullOk=false);
  int get_listen_port() { return listen_port; }
  DB_ENV* getENV() { return db_env; }
  bool isTXN() { return user_flags & DB_INIT_TXN; }

  void closeDBs();
  void get(Record& _return, const NameSpace& ns, const RecordKey& key);
  void get_set(std::vector<Record> & _return, const NameSpace& ns, const RecordSet& rs);
  bool sync_set(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy);
  bool copy_set(const NameSpace& ns, const RecordSet& rs, const Host& h);
  bool remove_set(const NameSpace& ns, const RecordSet& rs);
  bool put(const NameSpace& ns, const Record& rec);
  bool putDBTs(DB* db_ptr, MerkleDB* mdb_ptr,DBT* key, DBT* data);
  int32_t count_set(const NameSpace& ns, const RecordSet& rs);

  bool set_responsibility_policy(const NameSpace& ns, const RecordSet& policy);
  void get_responsibility_policy(RecordSet& _return, const NameSpace& ns);

  pthread_rwlock_t* get_dbmap_lock() { return &dbmap_lock; }
  map<const NameSpace,MerkleDB*>* get_merkle_dbs() { return &merkle_dbs; }
  void flush_wait(struct timespec* time);
  void flush_lock(bool);

};


class ReadDBTException : public std::exception {
public:
  ReadDBTException() {}

  ReadDBTException(const std::string& msg) :
    message_(msg) {}

  virtual ~ReadDBTException() throw() {}

  virtual const char* what() const throw() {
    return
      message_.empty()?
      "Default ReadDBTException.":
      message_.c_str();
  }

protected:
  std::string message_;

};

}


#endif // STORAGEDB_H
