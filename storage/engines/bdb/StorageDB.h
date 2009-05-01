#ifndef STORAGEDB_H
#define STORAGEDB_H

#include <db.h>
#include "gen-cpp/Storage.h"
#include "ruby.h"

#ifndef MSG_MORE
#define MSG_MORE 0
#endif

using namespace std;

VALUE rb_funcall_wrap(VALUE vargs);

namespace SCADS {

void* run_listen(void *args);

class StorageDB : public StorageIf {

private:
  DB_ENV *db_env;
  pthread_rwlock_t dbmap_lock;
  map<const NameSpace,DB*> dbs;
  pthread_t listen_thread;
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

  void chkLock(int rc, const string lock, const string action);


public:
  void apply_to_set(const NameSpace& ns, const RecordSet& rs,
		    void(*to_apply)(void*,DB*,DBC*,DB_TXN*,void*,void*),void* apply_arg,
		    bool invokeNone = false, bool bulk = false);

  DB* getDB(const NameSpace& ns);
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
  int32_t count_set(const NameSpace& ns, const RecordSet& rs);

  bool set_responsibility_policy(const NameSpace& ns, const RecordSet& policy);
  void get_responsibility_policy(RecordSet& _return, const NameSpace& ns);

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
