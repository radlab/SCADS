#ifndef STORAGEDB_H
#define STORAGEDB_H

#include <db.h>
#include "gen-cpp/Storage.h"

#ifndef MSG_MORE
#define MSG_MORE 0
#endif

using namespace std;

namespace SCADS {

void* run_listen(void *args);

class StorageDB : public StorageIf {

private:
  DB_ENV *db_env;
  pthread_rwlock_t dbmap_lock;
  map<const NameSpace,DB*> dbs;
  pthread_t listen_thread;
  int listen_port;

public:
  StorageDB(int);

private:
  int open_database(DB **dbpp,                  /* The DB handle that we are opening */
		    const char *file_name,     /* The file in which the db lives */
		    const char *program_name,  /* Name of the program calling this function */
		    const char *env_dir,       /* environment dir */
		    FILE *error_file_pointer);  /* File where we want error messages sent */

  bool responsible_for_key(const NameSpace& ns, const RecordKey& key);
  bool responsible_for_set(const NameSpace& ns, const RecordSet& rs);

  void chkLock(int rc, const string lock, const string action);

  void apply_to_set(const NameSpace& ns, const RecordSet& rs,
		    void(*to_apply)(void*,DB*,void*,void*),void* apply_arg);

public:

  DB* getDB(const NameSpace& ns);
  int get_listen_port() { return listen_port; }

  void closeDBs();
  void get(Record& _return, const NameSpace& ns, const RecordKey& key);
  void get_set(std::vector<Record> & _return, const NameSpace& ns, const RecordSet& rs);
  bool sync_set(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy);
  bool copy_set(const NameSpace& ns, const RecordSet& rs, const Host& h);
  bool remove_set(const NameSpace& ns, const RecordSet& rs);
  bool put(const NameSpace& ns, const Record& rec);

  bool set_responsibility_policy(const NameSpace& ns, const RecordSet& policy);
  void get_responsibility_policy(RecordSet& _return, const NameSpace& ns);

};


}


#endif // STORAGEDB_H
