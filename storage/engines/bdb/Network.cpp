#include "StorageDB.h"
#include "TQueue.h"

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <arpa/inet.h>

#include <string>
#include <iostream>
#include <sstream>

#define BACKLOG 10
#define VERSTR "SCADSBDB0.1"
#define BUFSZ 1024
#define COMMIT_LIMIT 20000

#define SYNC_SIMPLE 0
#define SYNC_MERKLE 1

#define print_hex(buf, len) for (int i = (len) - 1; i >= 0 ; i--) { printf("%X%X", (0x0F & (((char *)buf)[i]) >> 4), (0x0F & (((char *)buf)[i])));}

extern char stopping;
extern ID call_id;

namespace SCADS {

char node_data = NODE_DATA;
char node_merkle = NODE_MERKLE;
char merkle_no = MERKLE_NO;
char merkle_yes = MERKLE_YES;
char merkle_mark = MERKLE_MARK;
char merkle_stop = MERKLE_STOP;

using namespace std;
using namespace apache::thrift;

// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa) {
  if (sa->sa_family == AF_INET)
    return &(((struct sockaddr_in*)sa)->sin_addr);
  return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

int fill_buf(int* sock, char* buf, int off) {
  int recvd;
  if((recvd = recv(*sock,buf+off,BUFSZ-off,0)) == -1) {
    perror("fill_buf");
    close(*sock);
    return 0;
  }
#ifdef DEBUG
  /*  if (recvd > 0) {
    printf("filled buffer (off: %i, recvd: %i)\n[",off,recvd);
    for(int i = 0;i<recvd;i++)
      printf("%i ",buf[i]);
      printf("\b]\n");
      }*/
#endif
  return recvd;
}

int readHash(int* sock, MerkleHash* hash, DBT* save, char* buf, char* pos, char** endp) {
  int len = sizeof(MerkleHash);
  char *end = *endp;
  if ((end-pos) < len) {
    int rem = end-pos;
    memcpy(buf,pos,rem); // move data to the front
    if (save != NULL &&
	!save->flags) { // other dbt isn't malloced and needs its data saved and moved
      void* od = malloc(sizeof(char)*save->size);
      memcpy(od,save->data,save->size);
      save->data = od;
      save->flags = 1; // is malloced now
    }
    len = fill_buf(sock,buf,rem);
    if (len == 0) { // socket got closed down
      ReadDBTException e("Socket was orderly shutdown by peer");
      throw e;
    }
    *endp = buf+len+rem;
    return readHash(sock,hash,save,buf,buf,endp);
  }
  memcpy(hash,pos,len);
  return ((pos+len)-buf);
}

int fill_dbt(int* sock, DBT* k, DBT* other, char* buf, char* pos, char** endp) {
  int len;
  char *end = *endp;
  if (k->flags) { // in the middle of processing, so no need to read type
    len = (k->size - k->dlen);
  }
  else {
    if ((end-pos) < 5) {
      if ((end-pos) > 0 &&
	  (*pos == MERKLE_MARK ||
	   *pos == MERKLE_STOP) ) { // just kick out here
	k->doff = (int)(*pos);
	return ((pos+1)-buf);
      }
      int rem = end-pos;
      memcpy(buf,pos,rem); // move data to the front
      if (other != NULL &&
	  !other->flags) { // other dbt isn't malloced and needs its data saved and moved
	void* od = malloc(sizeof(char)*other->size);
	memcpy(od,other->data,other->size);
	other->data = od;
	other->flags = 1; // is malloced now
      }
      len = fill_buf(sock,buf,rem);
      if (len == 0) { // socket got closed down
	ReadDBTException e("Socket was orderly shutdown by peer");
	throw e;
      }
      *endp = buf+len+rem;
      return fill_dbt(sock,k,other,buf,buf,endp);
    }
    // here we have at least 5 bytes to work with
    // set the type
    k->doff = (int)(*pos);
    if (k->doff == MERKLE_MARK || k->doff == MERKLE_STOP) // no data for marks/stops
      return ((pos+1)-buf);
    // now get the length
    memcpy(&len,pos+1,4);
    k->size = len;
    pos+=5;
  }
  if (k->doff == NODE_DATA && len == 0) // means we're done
    return len;


  if (pos+len > end) { // we're spilling over a page
    if (k->flags) {
      memcpy(((char*)k->data)+k->dlen,pos,end-pos);
      k->dlen+=(end-pos);
    }
    else {
      k->data = malloc(sizeof(char)*len);
      k->flags = 1; // tmp use to say free after insert
      k->dlen = (end-pos);
      memcpy(k->data,pos,end-pos);
    }
    if (other != NULL &&
	!other->flags) { // other dbt isn't malloced and needs its data saved and moved
      void* od = malloc(sizeof(char)*other->size);
      memcpy(od,other->data,other->size);
      other->data = od;
      other->flags = 1; // is malloced now
    }
    len = fill_buf(sock,buf,0);
    if (len == 0) {
      ReadDBTException e("Socket was orderly shutdown by peer");
      throw e;
    }
    *endp = buf+len;
    return fill_dbt(sock,k,other,buf,buf,endp);
  }
  else { // okay, enough data in buf to finish off
    if (k->flags)
      memcpy(((char*)k->data)+k->dlen,pos,len);
    else
      k->data = pos;
  }

  return ((pos+len)-buf);
}

void do_throw(int errnum, string msg) {
  char* err = strerror(errnum);
  msg.append(err);
  TException te(msg);
  throw te;
}

int do_copy(int sock, StorageDB* storageDB, char* dbuf) {
  char *end;
  int off = 0;
  DB* db_ptr;
  MerkleDB* mdb_ptr;
  DB_ENV* db_env;
  DBT k,d;
  int fail = 0;
  int ic;

  // do all the work
  memset(&k, 0, sizeof(DBT));
  end = dbuf;
  try {
    off = fill_dbt(&sock,&k,NULL,dbuf,dbuf,&end);
  } catch (ReadDBTException &e) {
    cerr << "Could not read namespace: "<<e.what()<<endl;
    if (k.flags)
      free(k.data);
    return 1;
  }

  string ns = string((char*)k.data,k.size);
  if (k.flags)
    free(k.data);

#ifdef DEBUG
  cout << "Namespace is: "<<ns<<endl;
#endif
  db_ptr = storageDB->getDB(ns);
  mdb_ptr = storageDB->getMerkleDB(ns);

  DB_TXN *txn;
  txn = NULL;
  if (storageDB->isTXN()) {
    db_env = storageDB->getENV();
    fail = db_env->txn_begin(db_env, NULL, &txn, DB_TXN_SNAPSHOT);
    if (fail != 0) {
      TException te("Could not start transaction");
      throw te;
    }
  }

  for(ic=0;;ic++) { // now read all our key/vals
    int kf,df;

    if (ic != 0 &&
	txn != NULL &&
	ic % COMMIT_LIMIT == 0) {
      // gross, commit every 10000 so we don't run out of locks
      if (txn!=NULL && txn->commit(txn,0)) {
	cerr << "Commit of copy transaction failed in loop"<<endl;
	return 1;
      }
      fail = db_env->txn_begin(db_env, NULL, &txn, DB_TXN_SNAPSHOT);
      if (fail != 0) {
	TException te("Could not start transaction");
	throw te;
      }
    }

    memset(&k, 0, sizeof(DBT));
    memset(&d, 0, sizeof(DBT));
    try {
      off = fill_dbt(&sock,&k,NULL,dbuf,dbuf+off,&end);
    } catch (ReadDBTException &e) {
      cerr << "Could not read key in copy: "<<e.what()<<endl;
      if (k.flags)
	free(k.data);
      if (txn!=NULL && txn->abort(txn))
	cerr << "Transaction abort failed"<<endl;
      return 1;
    }
    if (off == 0)
      break;
    try {
      off = fill_dbt(&sock,&d,&k,dbuf,dbuf+off,&end);
    } catch (ReadDBTException &e) {
      cerr << "Could not read data in copy: "<<e.what()<<endl;
      if (k.flags)
	free(k.data);
      if (d.flags)
	free(d.data);
      if (txn!=NULL && txn->abort(txn))
	cerr << "Transaction abort failed"<<endl;
      return 1;
    }
#ifdef DEBUG
    cout << "key: "<<string((char*)k.data,k.size)<<" data: "<<string((char*)d.data,d.size)<<endl;
#endif
    kf=k.flags;
    df=d.flags;
    k.flags = 0;
    k.dlen = 0;
    d.flags = 0;
    d.dlen = 0;

    if (mdb_ptr == NULL) {
      if (db_ptr->put(db_ptr, txn, &k, &d, 0) != 0)
	fail = 1;
    } else {
      if ( (db_ptr->put(db_ptr, txn, &k, &d, 0) != 0) ||
	   (mdb_ptr->enqueue(&k,&d) != 0) )
	fail = 1;
    }

    if (kf)
      free(k.data);
    if (df)
      free(d.data);

    if (fail)
      break;
  }

  if (fail) {
    if (txn!=NULL && txn->abort(txn))
      cerr << "Transaction abort failed"<<endl;
  }
  else {
    if (txn!=NULL && txn->commit(txn,0))
      cerr << "Commit of copy transaction failed"<<endl;
  }

  if (!fail)
    fail = storageDB->flush_log(db_ptr);

  return fail;
}

// read a policy out of buf.  returns 0 on success, 1 otherwise
int deserialize_policy(char* buf, ConflictPolicy* pol) {
  char* pos = buf;
  memcpy(&(pol->type),pos,4); // copy over the type
  pos+=4;
  if (pol->type == CPT_GREATER)
    return 0; // nothing more to do
  else if (pol->type == CPT_FUNC) {
    memcpy(&(pol->func.lang),pos,4);
    pos+=4;
    if (pol->func.lang == LANG_RUBY) {
      int plen;
      memcpy(&plen,pos,4);
      pos+=4;
      pol->func.func.assign(pos,plen);
    }
    else {
      cerr<<"Policy on wire has an invalid language specified: "<<(pol->func.lang)<<endl;
      return 1;
    }
  } else {
    cerr<<"Policy on wire has an invalid type: "<<(pol->type)<<endl;
    return 1;
  }
  return 0;
}

struct sync_sync_args {
  int sock;
  int off;
  int ic;
  bool isTXN;
  char* dbuf;
  char *end;
  ConflictPolicy* pol;
  DBT k,d;
  DB_ENV *db_env;
  int remdone;
};

static
void send_mark(int sock) {
  if (send(sock,&merkle_mark,1,MSG_MORE) == -1)
    do_throw(errno,"Failed to send mark: ");
}

static
void send_merkle_stop(int sock) {
  if (send(sock,&merkle_stop,1,MSG_MORE) == -1)
    do_throw(errno,"Failed to send mark: ");
}

static
void send_vals(int sock,DBT* key, DBT* data, char type) {
  // DBTSEND OK
  if (send(sock,&type,1,MSG_MORE) == -1)
    do_throw(errno,"Failed to send message type: ");
  if (send(sock,&(key->size),4,MSG_MORE) == -1)
    do_throw(errno,"Failed to send key length: ");
  if (send(sock,((const char*)key->data),key->size,MSG_MORE) == -1)
    do_throw(errno,"Failed to send a key: ");
  if (data != NULL) {
    if (send(sock,&type,1,MSG_MORE) == -1)
      do_throw(errno,"Failed to send message type: ");
    if (send(sock,&(data->size),4,MSG_MORE) == -1)
      do_throw(errno,"Failed to send data length: ");
    if (send(sock,data->data,data->size,MSG_MORE) == -1)
      do_throw(errno,"Failed to send data: ");
  }
}

static
void send_hash(int sock, MerkleHash hash) {
  if (send(sock,&hash,sizeof(MerkleHash),MSG_MORE) == -1)
    do_throw(errno,"Failed to send hash: ");
}

static
void send_string(int sock, const string& str) {
  // DBTSEND OK
  int len = str.length();
  if (send(sock,&node_data,1,MSG_MORE) == -1)
    do_throw(errno,"Failed to send string type: ");
  if (send(sock,&len,4,MSG_MORE) == -1)
    do_throw(errno,"Failed to send string length: ");
  if (len != 0) {
    if (send(sock,str.c_str(),len,MSG_MORE) == -1)
      do_throw(errno,"Failed to send string: ");
  }
}

void apply_dump(void *s, DB* db, DBC* cursor, DB_TXN *txn, void *k, void *d) {
  int* sock = (int*)s;
  send_vals(*sock,(DBT*)k,(DBT*)d,node_data);
}

void sync_sync_put(DB* db, DBC* cursor, DB_TXN* txn, DBT* lkey, DBT* ldata, struct sync_sync_args* args, bool adv=true) {
  if (cursor == NULL || args->isTXN) {
    if (db->put(db, txn, &(args->k), &(args->d), 0) != 0)
      cerr << "Failed to put remote key: "<<string((char*)args->k.data,args->k.size)<<endl;
  }
  else {
    if (cursor->put(cursor, &(args->k), &(args->d), DB_KEYFIRST) != 0)
      cerr << "Failed to put remote key: "<<string((char*)args->k.data,args->k.size)<<endl;
    if (adv) {
      if (cursor->get(cursor, lkey, ldata, DB_NEXT))
	cerr << "Failed to advance local cursor"<<endl;
    }
  }

  if (txn!=NULL) {
    if ((++(args->ic) % COMMIT_LIMIT) == 0) {
      if (txn->commit(txn,0)) {
	cerr << "Commit of sync_sync transaction failed in loop"<<endl;
	return;
      }
      if ((args->db_env)->txn_begin(args->db_env, NULL, &txn, DB_TXN_SNAPSHOT)) {
	TException te("Could not start transaction");
	throw te;
      }
    }
  }
}

void sync_sync(void* s, DB* db, DBC* cursor, DB_TXN *txn, void* k, void* d) {
  int kf,df,minl,ic;
  DBT *key,*data;
  struct sync_sync_args* args = (struct sync_sync_args*)s;
  key = (DBT*)k;
  data = (DBT*)d;


  if (args->remdone) { // no more remote keys, just send over whatever else we have
    send_vals(args->sock,key,data,node_data);
    return;
  }

  if (args->k.size == 0) { // need a new one
    try {
      args->off = fill_dbt(&(args->sock),&(args->k),NULL,args->dbuf,args->dbuf+(args->off),&(args->end));
    } catch (ReadDBTException &e) {
      cerr << "Could not read key for sync: "<<e.what()<<endl;
      // TODO: rethrow or return fail code
      if (args->k.flags)
	free(args->k.data);
      return;
    }
    if (args->off == 0) {
      args->remdone = 1;
      if (key != NULL)
	send_vals(args->sock,key,data,node_data);
      return;
    }
    try {
      args->off = fill_dbt(&(args->sock),&(args->d),&(args->k),args->dbuf,args->dbuf+(args->off),&(args->end));
    } catch (ReadDBTException &e) {
      cerr << "Could not read data for sync: "<<e.what()<<endl;
      if (args->k.flags)
	free(args->k.data);
      if (args->d.flags)
	free(args->d.data);
      // TODO: rethrow or return fail code
      return;
    }
#ifdef DEBUG
    cerr << "[read for sync] key: "<<string((char*)args->k.data,args->k.size)<<" data: "<<string((char*)args->d.data,args->d.size)<<endl;
    //cerr << "[Local info is] key: "<<string((char*)key->data,key->size)<<" data: "<<string((char*)data->data,data->size)<<endl;
#endif
  }

  if (key == NULL) {
    // we have no local values, this is basically a copy
#ifdef DEBUG
    cerr<<"No local data, inserting all remote keys"<<endl;
#endif

    for(ic=0;;ic++) {
      int kf,df;

      if (args->k.size == 0) {
	try {
	  args->off = fill_dbt(&(args->sock),&(args->k),NULL,args->dbuf,args->dbuf+(args->off),&(args->end));
	} catch (ReadDBTException &e) {
	  cerr << "Could not read key for sync: "<<e.what()<<endl;
	  if (args->k.flags)
	    free(args->k.data);
	  // TODO: rethrow or return fail code
	  return;
	}
	if (args->off == 0) {
#ifdef DEBUG
	  cerr<<"Okay, read all remote keys, returning"<<endl;
#endif
	  return;
	}
	try {
	  args->off = fill_dbt(&(args->sock),&(args->d),&(args->k),args->dbuf,args->dbuf+(args->off),&(args->end));
	} catch (ReadDBTException &e) {
	  cerr << "Could not read data for sync: "<<e.what()<<endl;
	  if (args->k.flags)
	    free(args->k.data);
	  if (args->d.flags)
	    free(args->d.data);
	  // TODO: rethrow or return fail code
	  return;
	}
      }

#ifdef DEBUG
      cerr << "key: "<<string((char*)args->k.data,args->k.size)<<" data: "<<string((char*)args->d.data,args->d.size)<<endl;
#endif

      kf=args->k.flags;
      df=args->d.flags;
      args->k.flags = 0;
      args->k.dlen = 0;
      args->d.flags = 0;
      args->d.dlen = 0;


      sync_sync_put(db,cursor,txn,NULL,NULL,args,false);

      if (kf)
	free(args->k.data);
      if (df)
	free(args->d.data);

      memset(&(args->k), 0, sizeof(DBT));
      memset(&(args->d), 0, sizeof(DBT));
    }
  }

  for (;;) {
    if (key->size < args->k.size) {
      // local key is shorter, and therefore less
      // send over local key since other side is missing it
#ifdef DEBUG
      cerr << "Local key "<<string((char*)key->data,key->size)<<" is shorter, sending my local value over"<<endl;
#endif
      send_vals(args->sock,key,data,node_data);
      // don't want to clear keys, will deal with on next pass
      return;
    }

    while (key->size > args->k.size) {
      // remote key is shorter
      // need to keep inserting remote keys until we catch up
      kf=args->k.flags;
      df=args->d.flags;
      args->k.flags = 0;
      args->k.dlen = 0;
      args->d.flags = 0;
      args->d.dlen = 0;

#ifdef DEBUG
      cerr << "Local key "<<string((char*)key->data,key->size)<<" is longer, Inserting to catch up."<<endl;
#endif

      sync_sync_put(db,cursor,txn,key,data,args);

      if (kf)
	free(args->k.data);
      if (df)
	free(args->d.data);

      memset(&(args->k), 0, sizeof(DBT));
      memset(&(args->d), 0, sizeof(DBT));
      try {
	args->off = fill_dbt(&(args->sock),&(args->k),NULL,args->dbuf,args->dbuf+args->off,&(args->end));
      } catch (ReadDBTException &e) {
	cerr << "Could not read key for sync: "<<e.what()<<endl;
	// TODO: rethrow or return fail code
	if (args->k.flags)
	  free(args->k.data);
	return;
      }
      if (args->off == 0) {
	args->remdone = 1;
	return; // kick out, we'll see that there's no more remote keys and send over anything else we have
      }
      try {
	args->off = fill_dbt(&(args->sock),&(args->d),&(args->k),args->dbuf,args->dbuf+args->off,&(args->end));
      } catch (ReadDBTException &e) {
	cerr << "Could not read data for sync: "<<e.what()<<endl;
	if (args->k.flags)
	  free(args->k.data);
	if (args->d.flags)
	  free(args->d.data);
	// TODO: rethrow or return fail code
	return;
      }
#ifdef DEBUG
      cerr << "[read for sync] key: "<<string((char*)args->k.data,args->k.size)<<" data: "<<string((char*)args->d.data,args->d.size)<<endl;
      //cerr << "[Local info is] key: "<<string((char*)key->data,key->size)<<" data: "<<string((char*)data->data,data->size)<<endl;
#endif
    }

    // go back to top since we've gone from greater local to greater remote
    if (key->size != args->k.size) continue;

    // okay, keys are the same length, let's see what we need to do
    int cmp = strncmp((char*)key->data,(char*)args->k.data,key->size);
#ifdef DEBUG
    cerr << "Keys are same length, cmp is: "<<cmp<<endl;
#endif
    if (cmp < 0) {
      // local key is shorter, send it over
#ifdef DEBUG
      cerr << "Local key "<<string((char*)key->data,key->size)<<" is less, sending over."<<endl;
#endif
      send_vals(args->sock,key,data,node_data);
      // don't want to clear keys, will deal with on next pass
      return;
    }

    if (cmp > 0) {
      // remote key is shorter
      // need to keep inserting remote keys until we catch up
#ifdef DEBUG
      cerr << "Local key "<<string((char*)key->data,key->size)<<" is greater, inserting to catch up."<<endl;
#endif
      kf=args->k.flags;
      df=args->d.flags;
      args->k.flags = 0;
      args->k.dlen = 0;
      args->d.flags = 0;
      args->d.dlen = 0;

      sync_sync_put(db,cursor,txn,key,data,args);

      if (kf)
	free(args->k.data);
      if (df)
	free(args->d.data);

      memset(&(args->k), 0, sizeof(DBT));
      memset(&(args->d), 0, sizeof(DBT));
      try {
	args->off = fill_dbt(&(args->sock),&(args->k),NULL,args->dbuf,args->dbuf+args->off,&(args->end));
      } catch (ReadDBTException &e) {
	cerr << "Could not read key for sync: "<<e.what()<<endl;
	// TODO: rethrow or return fail code
	if (args->k.flags)
	  free(args->k.data);
	return;
      }
      if (args->off == 0) {
	args->remdone = 1;
	return; // ditto to above remdone comment
      }
      try {
	args->off = fill_dbt(&(args->sock),&(args->d),&(args->k),args->dbuf,args->dbuf+args->off,&(args->end));
      } catch (ReadDBTException &e) {
	cerr << "Could not read data for sync: "<<e.what()<<endl;
	// TODO: rethrow or return fail code
	if (args->k.flags)
	  free(args->k.data);
	if (args->d.flags)
	  free(args->d.data);
	return;
      }
#ifdef DEBUG
      cerr << "[read for sync] key: "<<string((char*)args->k.data,args->k.size)<<" data: "<<string((char*)args->d.data,args->d.size)<<endl;
      //cerr << "[Local info is] key: "<<string((char*)key->data,key->size)<<" data: "<<string((char*)data->data,data->size)<<endl;
#endif
      continue; // back to top since this new key could fall into any of the categories
    }

    // okay here we finally know we have the same keys
    break;
  }

  kf=args->k.flags;
  df=args->d.flags;
  args->k.flags = 0;
  args->k.dlen = 0;
  args->d.flags = 0;
  args->d.dlen = 0;

  int dcmp = 0;
  if (data->size == args->d.size)
    dcmp = memcmp(data->data,args->d.data,data->size);

#ifdef DEBUG
  cerr << "Same keys, dmp is: "<<dcmp<<" local size is: "<<data->size<<" remote size is: "<<args->d.size<<endl;
#endif

  if (data->size != args->d.size || dcmp) { // data didn't match
    if (args->pol->type == CPT_GREATER) {

      if (data->size > args->d.size ||
	  dcmp > 0) { // local is greater, keep that, send over our value
#ifdef DEBUG
	cerr << "Local data is greater, sending over."<<endl;
#endif
	send_vals(args->sock,key,data,node_data);
      }

      else if (data->size < args->d.size ||
	       dcmp < 0) { // remote is greater, insert, no need to send back
#ifdef DEBUG
	cerr << "Local data is less, inserting greater value locally."<<endl;
#endif
	sync_sync_put(db,cursor,txn,key,data,args,false);
      }

    }
    else if (args->pol->type == CPT_FUNC) {
#ifdef DEBUG
      cerr << "Executing ruby conflict policy"<<endl;
#endif
      int stat,rb_err;
      VALUE funcall_args[3];
      DBT rd;
      VALUE ruby_proc = rb_eval_string_protect(args->pol->func.func.c_str(),&stat);
      if (!rb_respond_to(ruby_proc,call_id)) {
	// TODO: Validate earlier so this can't happen
	cerr << "Invalid ruby policy specified"<<endl;
      }
      VALUE v;
      funcall_args[0] = ruby_proc;
      funcall_args[1] = rb_str_new((const char*)(data->data),data->size);
      funcall_args[2] = rb_str_new((const char*)(args->d.data),args->d.size);
      v = rb_protect(rb_funcall_wrap,((VALUE)funcall_args),&rb_err);
      if (rb_err) {
	VALUE lasterr = rb_gv_get("$!");
	VALUE message = rb_obj_as_string(lasterr);
	cerr <<  "Error evaling ruby conflict pol"<< rb_string_value_cstr(&message)<<endl;
      }
      char* newval = rb_string_value_cstr(&v);
#ifdef DEBUG
      cerr << "Ruby resolved to: "<<newval<<endl;
#endif

      if (strncmp(newval,(char*)data->data,data->size)) {
#ifdef DEBUG
	cerr<<"Ruby func returned something different from my local value, inserting locally"<<endl;
#endif
	void *td = args->d.data;
	u_int32_t ts = args->d.size;
	args->d.data = newval;
	args->d.size = strlen(newval);
	sync_sync_put(db,cursor,txn,key,data,args,false);
	args->d.data = td;
	args->d.size = ts;
      }
      if (strncmp(newval,(char*)args->d.data,args->d.size)) {
#ifdef DEBUG
	cerr<<"Ruby func returned something different from remote key, sending over"<<endl;
#endif
	void *td = args->d.data;
	u_int32_t ts = args->d.size;
	args->d.data = newval;
	args->d.size = strlen(newval);
	send_vals(args->sock,key,&(args->d),node_data);
	args->d.data = td;
	args->d.size = ts;
      }
    }
    else {
      cerr << "Invalid policy way down in sync_sync"<<endl;
    }
  }

  // okay, we're all done here free things up and zero them
  if (kf)
    free(args->k.data);
  if (df)
    free(args->d.data);

  memset(&(args->k), 0, sizeof(DBT));
  memset(&(args->d), 0, sizeof(DBT));
}

#define DATA_EQUAL 0
#define REM_REPLS  1
#define LOC_REPLS  2
#define BOTH_REPL  3


/* resolve data according to a policy
 * returns:
 *   DATA_EQUAL if the data was equal
 *   REM_REPLS if remote data replaces local data
 *   LOC_REPLS if local data should replace remote data
 *   BOTH_REPL if both local and remote data need to be updated
 */
int resolve_data(DBT* ld, DBT* rd,
		 ConflictPolicy& pol) {
  int dcmp = 0;
  if (ld->size == rd->size)
    dcmp = memcmp(ld->data,rd->data,ld->size);

  if (ld->size != rd->size || dcmp) { // data didn't match
    if (pol.type == CPT_GREATER) {
      if (ld->size > rd->size ||
	  dcmp > 0) { // local is greater
	return LOC_REPLS;
      }

      else if (ld->size < rd->size ||
	       dcmp < 0) { // remote is greater, insert, no need to send back
	return REM_REPLS;
      }
    }
    else if (pol.type == CPT_FUNC) {
      cerr << "Don't support ruby conflict just at the moment"<<endl;
      return DATA_EQUAL;
    }
  }
  return DATA_EQUAL; // matched
}

int do_merkle_sync(int sock, const NameSpace& ns,
		   StorageDB* storageDB,
		   DB* db, MerkleDB* mdb,
		   char* dbuf, char* end, int off,
		   ConflictPolicy& pol) {
  DBT key,data,lkey,ldata;
  int ret;
  int yesed_size; // shorted thing i've yesed, so i can ignore until i get something this short again DFS ONLY
  bool ignoring = false;
#ifdef DEBUG
    cout << "Doing a merkle sync"<<endl;
#endif
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  int kf,df;
  for(;;) {
    kf = 0;
    df = 0;
    try {
      off = fill_dbt(&sock,&key,NULL,dbuf,dbuf+off,&end);
    } catch (ReadDBTException &e) {
      cerr << "Could not read key in do_merkle_sync: "<<e.what()<<endl;
      if (key.flags)
	free(key.data);
      return 1;
    }
    if (off == 0)
      return 0;
    switch(key.doff) { // doff is where we store the type
    case NODE_DATA: {
#ifdef DEBUG
      cout << "Got data node in do_merkle_sync.  key:"<<dbt_string(&key)<<endl;
#endif
      try {
	off = fill_dbt(&sock,&data,&key,dbuf,dbuf+off,&end);
      } catch (ReadDBTException &e) {
	cerr << "Could not read data in do_merkle_sync: "<<e.what()<<endl;
	if (key.flags)
	  free(key.data);
	if (data.flags)
	  free(data.data);
	return 1;
      }
      int res;
      kf = key.flags;
      key.flags = 0;
      df = data.flags;
      data.flags = 0;
      //if (ignoring) break;
      memset(&ldata, 0, sizeof(DBT));
      ldata.flags = DB_DBT_MALLOC;
      ret = db->get(db,NULL,&key,&ldata,0);
      if (ret == DB_NOTFOUND) {
#ifdef DEBUG
	cout << "Local data missing, inserting"<<endl;
	cout << "[remote] key: "<<dbt_string(&key)<<" data: "<<dbt_string(&data)<<endl;
#endif
	res = REM_REPLS;
      } else if (ret == 0) {
#ifdef DEBUG
	cout << "[local]  key: "<<dbt_string(&key)<<" data: "<<dbt_string(&ldata)<<endl;
	cout << "[remote] key: "<<dbt_string(&key)<<" data: "<<dbt_string(&data)<<endl;
#endif
	res = resolve_data(&ldata,&data,pol);
      } else {
	db->err(db,ret,"Getting local data");
	cerr << "[remote] key: "<<dbt_string(&key)<<" data: "<<dbt_string(&data)<<endl;
	res = DATA_EQUAL;
      }
      switch (res) {
      case DATA_EQUAL: // nothing to do
	break;
      case REM_REPLS: { // need to insert locally
#ifdef DEBUG
	cout << "Local is less, inserting"<<endl;
#endif
	storageDB->putDBTs(db,mdb,&key,&data);
	break;
      }
      case LOC_REPLS: { // local replaces, send back over
#ifdef DEBUG
	cout << "Local is greater, sending back"<<endl;
#endif
	send_vals(sock,&key,&ldata,node_data);
	break;
      }
      case BOTH_REPL: { // do both
	cout << "BOTH_REPL not handled at the moment"<<endl;
	break;
      }
      default:
	cerr << "Wrong value for data resolution, something is bad"<<endl;
      }
    }
      // this breaks the NODE_DATA case
      break;
    case NODE_MERKLE: {
      MerkleHash hash;
      try {
	off = readHash(&sock,&hash,&key,dbuf,dbuf+off,&end);
      } catch (ReadDBTException &e) {
	cerr << "Could not read hash in do_merkle_sync: "<<e.what()<<endl;
	if (key.flags)
	  free(key.data);
	return 1;
      }
      kf = key.flags;
      key.flags = 0;
      df = data.flags;
      data.flags = 0;
      if (ignoring) {
	if (key.size <= yesed_size)
	  ignoring = false;
	else
	  break;
      }
#ifdef DEBUG
      cout << "Got merkle node in do_merkle_sync.  Key is: "<<string((char*)key.data,key.size)<<" hash: "<<hash<<endl;
#endif
      memset(&ldata, 0, sizeof(DBT));
      ldata.flags = DB_DBT_MALLOC;
      ret = mdb->dbp->get(mdb->dbp,NULL,&key,&ldata,0);
      if (ret == DB_NOTFOUND) { // I don't have it, that's a no
#ifdef DEBUG
	cout << "I don't have this node, I'm going to reply no"<<endl;
#endif
	send_vals(sock,&key,NULL,merkle_no);
	break;
      }
      MerkleNode * ln =  (MerkleNode *)(ldata.data);
#ifdef DEBUG
      cout<<"local node: "<<ln->digest<<endl;
      cout<<"remote node: "<<hash<<endl;
#endif
      /*** BSF CODE ***/
      /*
      if (ln->digest != hash) {
#ifdef DEBUG
	cout << "digests don't match, sending a no"<<endl;
#endif
	send_vals(sock,&key,NULL,merkle_no);
#ifdef DEBUG
	cout << "sent"<<endl;
#endif
      }
      */
      /*** END BFS CODE ***/

      /*** DFS CODE ***/
      if (ln->digest != hash &&
	  is_leaf(&key)) {
#ifdef DEBUG
	cout << "digests don't match and it's a leaf, sending a no"<<endl;
#endif
	send_vals(sock,&key,NULL,merkle_no);
      }

      if (!is_leaf(&key) &&
	  ln->digest == hash) {
#ifdef DEBUG
	cout << "digests match and it's not a leaf, sending a yes"<<endl;
#endif
	send_vals(sock,&key,NULL,merkle_yes);
	ignoring = true;
	yesed_size = key.size;
      }
      /*** END DFS CODE ***/
      break;
    }
    case MERKLE_STOP:  // should only get in the DFS case
#ifdef DEBUG
      cout << "End of merkle tree, sending over final marker"<<endl;
#endif
      send_string(sock,"");
      break;
    case MERKLE_MARK: {
#ifdef DEBUG
      cout << "Got merkle mark in do_merkle_sync.  Sending mark back"<<endl;
#endif
      kf = key.flags;
      send_mark(sock);
      break;
    }
    }
    if (kf)
      free(key.data);
    if (df)
      free(data.data);
  }
}

int do_merkle_dfs_sync(int sock, const NameSpace& ns,
		       StorageDB* storageDB,
		       DB* db, MerkleDB* mdb,
		       char* dbuf, char* end, int off,
		       ConflictPolicy& pol) {
  DBT key,data,lkey,ldata,mkey,mdata;
  int ret;
  bool eot = false;
  bool rok = false;
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  int kf,df;

  // init the merkle tree
  DBC* cursorp;
  DB* dbp = mdb->dbp;
  memset(&mkey, 0, sizeof(DBT));
  mkey.flags = DB_DBT_MALLOC;
  memset(&mdata, 0, sizeof(DBT));
  mdata.flags = DB_DBT_MALLOC;
  dbp->cursor(dbp, NULL, &cursorp, 0);
  ret = cursorp->get(cursorp, &mkey, &mdata, DB_NEXT);
  if (ret != 0) {
    cerr << "couldn't get merkle cursor in dfs_sync"<<endl;
    return 1;
  }

  for(;;) {
    kf = 0;
    df = 0;
    try {
      off = fill_dbt(&sock,&key,NULL,dbuf,dbuf+off,&end);
    } catch (ReadDBTException &e) {
      cerr << "Could not read key in do_merkle_sync: "<<e.what()<<endl;
      if (key.flags)
	free(key.data);
      return 1;
    }

    if (off == 0)
      return 0;

    switch(key.doff) { // doff is where we store the type
    case NODE_DATA: {
#ifdef DEBUG
      cout << "Got data node in do_merkle_dfs_sync.  key:"<<dbt_string(&key)<<endl;
#endif
      try {
	off = fill_dbt(&sock,&data,&key,dbuf,dbuf+off,&end);
      } catch (ReadDBTException &e) {
	cerr << "Could not read data in do_merkle_dfs_sync: "<<e.what()<<endl;
	if (key.flags)
	  free(key.data);
	if (data.flags)
	  free(data.data);
	return 1;
      }
      int res;
      kf = key.flags;
      key.flags = 0;
      df = data.flags;
      data.flags = 0;

      if (key.size != 0) { // always process root node
	int c = mdb->dbt_cmp(&mkey,&key);
	if (c > 0) break;
	if (c < 0) { // need to catch up, TODO: should this EVER happen?
	  free(mkey.data);
	  free(mdata.data);
	  mkey.size = key.size;
	  mkey.data = key.data;
	  ret = cursorp->get(cursorp, &mkey, &mdata, DB_SET_RANGE);
	  if (ret == DB_NOTFOUND) { // end of my tree
	    eot = true;
	    ret = cursorp->get(cursorp, &mkey, &mdata, DB_LAST);
	  }
	}
      }


      memset(&ldata, 0, sizeof(DBT));
      ldata.flags = DB_DBT_MALLOC;
      key.size--; // remove null term
      ret = db->get(db,NULL,&key,&ldata,0);
      if (ret == DB_NOTFOUND) {
#ifdef DEBUG
	cout << "Local data missing, inserting"<<endl;
	cout << "[remote] key: "<<dbt_string(&key)<<" data: "<<dbt_string(&data)<<endl;
#endif
	res = REM_REPLS;
      } else if (ret == 0) {
#ifdef DEBUG
	cout << "[local]  key: "<<dbt_string(&key)<<" data: "<<dbt_string(&ldata)<<endl;
	cout << "[remote] key: "<<dbt_string(&key)<<" data: "<<dbt_string(&data)<<endl;
#endif
	res = resolve_data(&ldata,&data,pol);
      } else {
	db->err(db,ret,"Getting local data");
	cerr << "[remote] key: "<<dbt_string(&key)<<" data: "<<dbt_string(&data)<<endl;
	res = DATA_EQUAL;
      }
      switch (res) {
      case DATA_EQUAL: // nothing to do
	break;
      case REM_REPLS: { // need to insert locally
#ifdef DEBUG
	cout << "Local is less, inserting"<<endl;
#endif
	storageDB->putDBTs(db,mdb,&key,&data);
	break;
      }
      case LOC_REPLS: { // local replaces, send back over
#ifdef DEBUG
	cout << "Local is greater, sending back"<<endl;
#endif
	send_vals(sock,&key,&ldata,node_data);
	break;
      }
      case BOTH_REPL: { // do both
	cout << "BOTH_REPL not handled at the moment"<<endl;
	break;
      }
      default:
	cerr << "Wrong value for data resolution, something is bad"<<endl;
      }
      key.size++;
      // advance by one
      free(mkey.data);
      free(mdata.data);
      ret = cursorp->get(cursorp, &mkey, &mdata, DB_NEXT);
      if (ret == DB_NOTFOUND) {
	eot = true;
	ret = cursorp->get(cursorp, &mkey, &mdata, DB_LAST);
      }
    }
      // this breaks the NODE_DATA case
      break;
    case NODE_MERKLE: {
      MerkleHash hash;
      try {
	off = readHash(&sock,&hash,&key,dbuf,dbuf+off,&end);
      } catch (ReadDBTException &e) {
	cerr << "Could not read hash in do_merkle_sync: "<<e.what()<<endl;
	if (key.flags)
	  free(key.data);
	return 1;
      }
      kf = key.flags;
      key.flags = 0;
      df = data.flags;
      data.flags = 0;

      if (eot) { // i've hit the end of my tree
	if (!rok && is_leaf(&key) && (mdb->dbt_cmp(&key,&mkey) > 0)) {
#ifdef DEBUG
	  cout << "data node after the end of my tree, sending no"<<endl;
#endif
	  send_vals(sock,&key,NULL,merkle_no);
	}
	break;
      }

      if (key.size != 0) { // always process root node
	int c = mdb->dbt_cmp(&mkey,&key);
	if (c > 0) break;
	if (c < 0) { // need to catch up
	  free(mkey.data);
	  free(mdata.data);
	  mkey.size = key.size;
	  mkey.data = key.data;
	  ret = cursorp->get(cursorp, &mkey, &mdata, DB_SET_RANGE);
	  if (ret == DB_NOTFOUND) { // end of my tree
	    eot = true;
	    ret = cursorp->get(cursorp, &mkey, &mdata, DB_LAST);
	  }
	  c = mdb->dbt_cmp(&mkey,&key);
	}
	if (c > 0) { // means i'm missing this node
	  break;
	}
      }

#ifdef DEBUG
      cout << "Got merkle node in do_merkle_sync.  Key is: "<<string((char*)key.data,key.size)<<" hash: "<<hash<<endl;
#endif
      MerkleNode * ln = (MerkleNode *)(mdata.data);
#ifdef DEBUG
      cout<<"local node: "<<ln->digest<<endl;
      cout<<"remote node: "<<hash<<endl;
#endif
      /*      if (ln->digest != hash &&
	  is_leaf(&key)) {
#ifdef DEBUG
	cout << "digests don't match and it's a leaf, sending a no"<<endl;
#endif
	send_vals(sock,&key,NULL,merkle_no);

	}*/

      if (!is_leaf(&key) &&
	  ln->digest == hash) {
#ifdef DEBUG
	cout << "digests match and it's not a leaf, sending a yes"<<endl;
#endif
	send_vals(sock,&key,NULL,merkle_yes);
	if (key.size == 0) { // root node, we're done, just wait for the other side to notice
	  eot = true;
	  rok = true;
	  break;
	} else {
	  // advance to next branch
	  free(mkey.data);
	  free(mdata.data);
	  mkey.data = key.data;
	  mkey.size = key.size;
	  char* cd = (char*)mkey.data;
	  cd[mkey.size-1]++;
	  ret = cursorp->get(cursorp,&mkey,&mdata,DB_SET_RANGE);
	  if (ret == DB_NOTFOUND) {
	    eot = true;
	    ret = cursorp->get(cursorp, &mkey, &mdata, DB_LAST);
	  }
	}
      } else {
	// advance by one
	free(mkey.data);
	free(mdata.data);
	ret = cursorp->get(cursorp, &mkey, &mdata, DB_NEXT);
	if (ret == DB_NOTFOUND) {
	  eot = true;
	  ret = cursorp->get(cursorp, &mkey, &mdata, DB_LAST);
	}
      }
      /*** END DFS CODE ***/
      break;
    }
    case MERKLE_STOP:
#ifdef DEBUG
      cout << "End of merkle tree, sending over final marker"<<endl;
#endif
      send_string(sock,"");
      break;
    } // end of switch
    if (kf)
      free(key.data);
    if (df)
      free(data.data);
  }
  free(mkey.data);
  free(mdata.data);
  if (cursorp != NULL)
    cursorp->close(cursorp);
}

int do_sync(int sock, StorageDB* storageDB, char* dbuf, char sync_type) {
  char *end;
  int off = 0;
  DB* db_ptr;
  DBT *lk,*ld;
  DBT k;
  int fail = 0;
  ConflictPolicy policy;

  // do all the work
  memset(&k, 0, sizeof(DBT));
  end = dbuf;
  try {
    off = fill_dbt(&sock,&k,NULL,dbuf,dbuf+off,&end); // read the namespace
  } catch (ReadDBTException &e) {
    cerr << "Could not read namespace for do_sync: "<<e.what()<<endl;
    if (k.flags)
      free(k.data);
    return 1;
  }

  string ns = string((char*)k.data,k.size);
  if (k.flags)
    free(k.data);

#ifdef DEBUG
  cout << "Namespace is: "<<ns<<endl;
#endif

  try {
    off = fill_dbt(&sock,&k,NULL,dbuf,dbuf+off,&end); // read the policy
  } catch (ReadDBTException &e) {
    cerr << "Could not read policy for do_sync: "<<e.what()<<endl;
    if (k.flags)
      free(k.data);
    return 1;
  }
  if (deserialize_policy((char*)k.data,&policy)) {
    cerr<<"Failed to read sync policy"<<endl;
    if (k.flags)
      free(k.data);
    return 1;
  }

#ifdef DEBUG
  cout<<"Policy type: "<<
    (policy.type == CPT_GREATER?"greater":"userfunc:\n ")<<
    (policy.type == CPT_GREATER?"":policy.func.func)<<endl;
#endif

  try {
    off = fill_dbt(&sock,&k,NULL,dbuf,dbuf+off,&end); // read the record set
  } catch (ReadDBTException &e) {
    cerr << "Could not read record set for do_sync: "<<e.what()<<endl;
    if (k.flags)
      free(k.data);
    return 1;
  }
  RecordSet rs;
  string rss((char*)k.data,k.size);
#ifdef DEBUG
  cout << "read sync set srt:"<<endl<<
    rss<<endl;
#endif
  int type;
  istringstream is(rss,istringstream::in);
  is >> type;
  rs.type = (SCADS::RecordSetType)type;
  switch (rs.type) {
  case RST_RANGE:
    is >> rs.range.start_key>>rs.range.end_key;
    rs.__isset.range = true;
    rs.range.__isset.start_key = true;
    rs.range.__isset.end_key = true;
    break;
  case RST_KEY_FUNC: {
    int lang;
    stringbuf sb;
    is >> lang >> (&sb);
    rs.func.lang = (SCADS::Language)lang;
    rs.func.func.assign(sb.str());
    rs.__isset.func = true;
    rs.func.__isset.lang = true;
    rs.func.__isset.func = true;
  }
    break;
  }

  DB *db = storageDB->getDB(ns);

  if (sync_type == SYNC_SIMPLE) {
    struct sync_sync_args args;
    args.sock = sock;
    args.dbuf = dbuf;
    args.end = end;
    args.off = off;
    args.ic = 0;
    args.pol = &policy;
    args.db_env = storageDB->getENV();
    args.remdone = 0;
    args.isTXN = storageDB->isTXN();
    memset(&(args.k), 0, sizeof(DBT));
    memset(&(args.d), 0, sizeof(DBT));

    storageDB->apply_to_set(ns,rs,sync_sync,&args,true);
    if (!args.remdone)
      sync_sync(&args,db,NULL,NULL,NULL,NULL);
  }

  else if (sync_type == SYNC_MERKLE) {
    MerkleDB *mdb = storageDB->getMerkleDB(ns);
    storageDB->flush_lock(false);
    mdb->flushp();
    // BFS
    //do_merkle_sync(sock,ns,storageDB,db,mdb,dbuf,end,off,policy);
    // DFS
    do_merkle_dfs_sync(sock,ns,storageDB,db,mdb,dbuf,end,off,policy);
    storageDB->flush_lock(true);
  }

#ifdef DEBUG
  cerr << "sync_sync set done, sending end message"<<endl;
#endif

  if(sync_type == SYNC_SIMPLE)
    send_string(sock,"");

  return storageDB->flush_log(db);
}

void* run_listen(void* arg) {
  int status,recvd;
  struct addrinfo hints;
  struct addrinfo *res, *rp;
  StorageDB *storageDB = (StorageDB*) arg;
  char dbuf[BUFSZ];
  char *p;
  char abuf[INET6_ADDRSTRLEN];
  int sock,as;
  struct sockaddr_storage peer_addr;
  socklen_t peer_addr_len;

  memset(&hints,0, sizeof(addrinfo));
  //hints.ai_family = AF_UNSPEC; // uncomment for possible ipv6
  hints.ai_family = AF_INET; // ipv4 for now
  hints.ai_socktype = SOCK_STREAM; // UDP someday?
  hints.ai_flags = AI_PASSIVE;

  sprintf(dbuf,"%i",storageDB->get_listen_port());

  if ((status = getaddrinfo(NULL, dbuf,
			    &hints, &res)) != 0) {
    fprintf(stderr, "getaddrinfo error: %s\n", gai_strerror(status));
    exit(1);
  }


  // loop and find something to bind to
  for (rp = res; rp != NULL; rp = rp->ai_next) {
    sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);

    if (sock == -1)
      continue;

    if (bind(sock,rp->ai_addr,rp->ai_addrlen) == 0)
      break; // success

    close(sock);
  }

  if (rp == NULL) {
    fprintf(stderr, "Bind failed for copy/sync port\n");
    exit(1);
  }

  freeaddrinfo(res);

  if (listen(sock,BACKLOG) == -1) {
    perror("listen");
    exit(EXIT_FAILURE);
  }

  printf("Listening for sync/copy on port %s...\n",dbuf);

  peer_addr_len = sizeof(struct sockaddr_storage);

  fd_set readfds;
  struct timeval tv;


  while(!stopping) {
    FD_ZERO(&readfds);
    FD_SET(sock, &readfds);
    tv.tv_sec = 2;
    tv.tv_usec = 500000;
    // don't care about writefds and exceptfds:
    select(sock+1, &readfds, NULL, NULL, &tv);

    if (!FD_ISSET(sock, &readfds)) // timeout
      continue;

    as = accept(sock,(struct sockaddr *)&peer_addr,&peer_addr_len);

#ifdef DEBUG
    inet_ntop(peer_addr.ss_family,
	      get_in_addr((struct sockaddr *)&peer_addr),
	      abuf, sizeof(abuf));
    printf("server: got connection from %s\n", abuf);
#endif

    if (send(as,VERSTR,11,0) == -1) {
      perror("send");
      close(as);
      continue;
    }

    if ((recvd = recv(as, dbuf, 1, 0)) == -1) {
      perror("Could not read operation");
      close(as);
      continue;
    }

    // should maybe fire off new thread for the actual copy/sync?
    // would need to make dbuf private to each thread
    int stat = 0;
    if (dbuf[0] == 0) // copy
      stat = do_copy(as,storageDB,dbuf);
    else if (dbuf[0] == 1) // sync
      stat = do_sync(as,storageDB,dbuf,storageDB->isMerkle()?SYNC_MERKLE:SYNC_SIMPLE);
    else if (dbuf[0] == 2) { // dump
#ifdef DEBUG
      cerr << "Dumping all data"<<endl;
#endif
      DBT k;
      memset(&k, 0, sizeof(DBT));
      char* end = dbuf;
      try {
	fill_dbt(&as,&k,NULL,dbuf,dbuf,&end);
      } catch (ReadDBTException &e) {
	cerr << "Could not read namespace: "<<e.what()<<endl;
	if (k.flags)
	  free(k.data);
	stat = 1;
      }
      if (!stat) {
	string ns = string((char*)k.data,k.size);
#ifdef DEBUG
	cout << "Namespace is: "<<ns<<endl;
#endif
	if (k.flags)
	  free(k.data);

	RecordSet rs;
	rs.type = RST_ALL;
	rs.__isset.range = false;
	rs.__isset.func = false;
	try {
	  storageDB->apply_to_set(ns,rs,apply_dump,&as);
	} catch (TException &e) {
	  stat = 1;
	  stat = 1;
	  cerr << "An error occured while dumping: "<<e.what()<<endl;
	}
	send_string(as,"");
      }
    }
    else {
      cerr <<"Unknown operation requested on copy/sync port: "<<((int)dbuf[0])<<endl;
      close(as);
      continue;
    }
    //#ifdef DEBUG
    cout << "done.  stat: "<<stat<<endl;
    //#endif
    if (send(as,&stat,1,0) == -1)
      perror("send STAT");
    close(as);
  }

  close(sock);
}

int open_socket(const Host& h) {
  int sock, numbytes;
  struct addrinfo hints, *res, *rp;

  int rv;
  char buf[12];

  string::size_type loc;
  loc = h.find_last_of(':');
  if (loc == string::npos) { // :
    TException te("Host parameter must be of form host:port");
    throw te;
  }
  if (loc == 0) {
    TException te("Host parameter cannot start with a :");
    throw te;
  }
  if (loc == (h.length()-1)) {
    TException te("Host parameter cannot end with a :");
    throw te;
  }
  string host = h.substr(0,loc);
  string port = h.substr(loc+1);


  memset(&hints, 0, sizeof hints);
  //hints.ai_family = AF_UNSPEC; // uncomment for possible ipv6
  hints.ai_family = AF_INET; // ipv4 for now
  hints.ai_socktype = SOCK_STREAM;

  if ((rv = getaddrinfo(host.c_str(), port.c_str(), &hints, &res)) != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
    return 1;
  }

  // loop through all the results and connect to the first we can
  for(rp = res; rp != NULL; rp = rp->ai_next) {
    if ((sock = socket(rp->ai_family, rp->ai_socktype,
		       rp->ai_protocol)) == -1) {
      perror("open_socket: socket");
      continue;
    }

    if (connect(sock, rp->ai_addr, rp->ai_addrlen) == -1) {
      close(sock);
      perror("open_socket: connect");
      continue;
    }

    break;
  }

  if (rp == NULL) {
    TException te("Could not connect\n");
    throw te;
  }

#ifdef DEBUG
  char s[INET6_ADDRSTRLEN];
  inet_ntop(rp->ai_family, get_in_addr((struct sockaddr *)rp->ai_addr),
            s, sizeof s);
  printf("connecting to %s\n", s);
#endif

  freeaddrinfo(res);

  if ((numbytes = recv(sock, buf, 11, 0)) == -1)
    do_throw(errno,"Error receiving version string: ");

  buf[numbytes] = '\0';

  if (strncmp(VERSTR,buf,11)) {
    TException te("Version strings didn't match");
    throw te;
  }

  return sock;
}

void apply_copy(void* s, DB* db, DBC* cursor, DB_TXN* txn, void* k, void* d) {
  int *sock = (int*)s;
  DBT *key,*data;
  key = (DBT*)k;
  data = (DBT*)d;
  send_vals(*sock,key,data,node_data);
}

bool StorageDB::
copy_set(const NameSpace& ns, const RecordSet& rs, const Host& h) {
  int numbytes;
  char stat;

#ifdef DEBUG
  cerr << "copy_set called.  copying to host: "<<h<<endl;
#endif

  int sock = open_socket(h);

  stat = 0; // copy command
  if (send(sock,&stat,1,MSG_MORE) == -1)
    do_throw(errno,"Error sending copy operation: ");

  send_string(sock,ns);

  apply_to_set(ns,rs,apply_copy,&sock);

  // send done message
  send_string(sock,"");

#ifdef DEBUG
  cerr << "Sent done. "<<numbytes<<" bytes"<<endl;
#endif

  if ((numbytes = recv(sock, &stat, 1, 0)) == -1)
    do_throw(errno,"Could not read final status: ");

#ifdef DEBUG
  cerr << "Read final status: "<<((int)stat)<<" ("<<numbytes<<" bytes)"<<endl;
#endif

  close(sock);

  if(stat) // non-zero means a fail
    return false;

  return true;
}


// same as copy, just send all our keys
void sync_send(void* s, DB* db, DBC* cursor, DB_TXN* txn, void* k, void* d) {
  int *sock = (int*)s;
  DBT *key,*data;
  key = (DBT*)k;
  data = (DBT*)d;
#ifdef DEBUG
  cerr << "[Sending for sync] key: "<<string((char*)key->data,key->size)<<" data: "<<string((char*)data->data,data->size)<<endl;
#endif
  send_vals(*sock,key,data,node_data);
}

struct sync_recv_args {
  int sock;
  int stat;
  int isTXN;
  DB* db_ptr;
  DB_ENV* db_env;
};

// receive keys as a response from a sync and insert them
// arg should be the namespace for the sync
void* sync_recv(void* arg) {
  char *end;
  int off = 0;
  int ic;
  DBT k,d;
  char dbuf[BUFSZ];
  struct sync_recv_args* args = (struct sync_recv_args*)arg;

  int sock = args->sock;
  DB* db_ptr = args->db_ptr;
  DB_ENV* db_env = args->db_env;
  DB_TXN* txn;
  args->stat = 0;
  end = dbuf;

  txn = NULL;

  if (args->isTXN) {
    if (db_env->txn_begin(db_env, NULL, &txn, DB_TXN_SNAPSHOT)) {
      cerr << "Could not start transaction in sync_recv"<<endl;
      return NULL;
    }
  }

  for(ic=0;;ic++) { // now read all our key/vals
    int kf,df;

    if (txn != NULL) {
      if ( ic != 0 &&
	   (ic % COMMIT_LIMIT) == 0 ) {
	if (txn->commit(txn,0)) {
	  cerr << "Commit of sync_recv transaction failed in loop"<<endl;
	  return NULL;
	}
	if (db_env->txn_begin(db_env, NULL, &txn, DB_TXN_SNAPSHOT)) {
	  cerr << "Could not start transaction in sync_recv"<<endl;
	  return NULL;
	}
      }
    }

    memset(&k, 0, sizeof(DBT));
    memset(&d, 0, sizeof(DBT));
    try {
      off = fill_dbt(&sock,&k,NULL,dbuf,dbuf+off,&end);
    } catch (ReadDBTException &e) {
      cerr << "Could not recieve sync key back: "<<e.what()<<endl;
      args->stat = 1; // fail
      if (k.flags)
	free(k.data);
      if (d.flags)
	free(d.data);
      break;
    }
    if (off == 0) {
      cerr << "off is 0, breaking"<<endl;
      break;
    }
    try {
      off = fill_dbt(&sock,&d,&k,dbuf,dbuf+off,&end);
    } catch (ReadDBTException &e) {
      cerr << "Could not read sync data back: "<<e.what()<<endl;
      args->stat = 1; // fail
      if (k.flags)
	free(k.data);
      if (d.flags)
	free(d.data);
      break;
    }
#ifdef DEBUG
    cout << "[to update] key: "<<string((char*)k.data,k.size)<<" [synced] data: "<<string((char*)d.data,d.size)<<endl;
#endif
    kf=k.flags;
    df=d.flags;
    k.flags = 0;
    k.dlen = 0;
    d.flags = 0;
    d.dlen = 0;

    if (db_ptr->put(db_ptr, txn, &k, &d, 0) != 0) {
      cerr<<"Couldn't insert synced key: "<<string((char*)k.data,k.size)<<endl;
      continue;
    }

    if (kf)
      free(k.data);
    if (df)
      free(d.data);
  }
  if (txn != NULL && txn->commit(txn,0)) {
    cerr<<"could not commit sync_recv transaction"<<endl;
  }
}

bool StorageDB::
simple_sync(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy) {
  int numbytes;
  char stat;

  // TODO:MAKE SURE POLICY IS VALID

  int sock = open_socket(h);
  int nslen = ns.length();

  stat = 1; // sync command
  if (send(sock,&stat,1,MSG_MORE) == -1)
    do_throw(errno,"Error sending sync operation: ");

  send_string(sock,ns);

  // DBTSEND OK

  nslen =
    (policy.type == CPT_GREATER)?
    4:
    (12+policy.func.func.length());

  if (send(sock,&node_data,1,MSG_MORE) == -1)
    do_throw(errno,"Failed to send message type: ");

  if (send(sock,&(nslen),4,MSG_MORE) == -1)
    do_throw(errno,"Error sending policy length: ");

  if (send(sock,&(policy.type),4,MSG_MORE) == -1)
    do_throw(errno,"Error sending policy type: ");

  if (policy.type == CPT_FUNC) {
    if (send(sock,&(policy.func.lang),4,MSG_MORE) == -1)
      do_throw(errno,"Error sending policy language: ");

    nslen = policy.func.func.length();
    if (send(sock,&nslen,4,MSG_MORE) == -1)
      do_throw(errno,"Error sending policy function length: ");

    if (send(sock,policy.func.func.c_str(),nslen,MSG_MORE) == -1)
      do_throw(errno,"Error sending policy language: ");
  }

  // gross, but let's serialize the record set okay, let's serialize
  ostringstream os;
  os << rs.type;
  switch (rs.type) {
  case RST_RANGE:
    os << " "<<
      rs.range.start_key<<" "<<
      rs.range.end_key;
    break;
  case RST_KEY_FUNC:
    os << " "<<rs.func.lang<< " "<<rs.func.func;
    break;
  }

  send_string(sock,os.str());

  struct sync_recv_args args;
  args.sock = sock;
  args.db_ptr = getDB(ns);
  args.db_env = db_env;
  args.isTXN = (user_flags & DB_INIT_TXN);

  pthread_t recv_thread;
  (void) pthread_create(&recv_thread,NULL,
			sync_recv,&args);

  apply_to_set(ns,rs,sync_send,&sock);

  // send done message
  send_string(sock,"");

  // wait for recv thread to read back all the keys
  pthread_join(recv_thread,NULL);

  if ((numbytes = recv(sock, &stat, 1, 0)) == -1)
    do_throw(errno,"Could not read final status: ");


  close(sock);

  if(stat || args.stat) // non-zero means a fail
    return false;

  if (flush_log(args.db_ptr))
    return false;

  return true;
}


void apply_send(void *s, void *k, void *d) {
  int* sock = (int*)s;
  DBT* data = (DBT*)d;
#ifdef DEBUG
  cout << "Sending merkle_node for key: "<<dbt_string((DBT*)k)<<endl;
#endif
  MerkleNode * mn = (MerkleNode *)(data->data);
  send_vals(*sock,(DBT*)k,NULL,node_merkle);
  send_hash(*sock,mn->digest);
}


int merkle_send(int sock, DB* db, MerkleDB* mdb, TQueue<DBT>* q) {
  DBT data;
  int ret;
  memset(&data, 0, sizeof(DBT));
  data.flags = DB_DBT_MALLOC;
  for(;;) {
    DBT key = q->dequeue();
    if (key.doff == 1) { // send a mark
#ifdef DEBUG
      cout << "Got a key with doff 1, sending a mark"<<endl;
#endif
      send_mark(sock);
      continue;
    }
    if (key.doff == 2)  // end
      break;

    void* olddata = key.data;

    if (is_leaf(&key)) {
      key.size--; // remove null term
      int ret = db->get(db,NULL,&key,&data,0);
      if (ret == 0) {  // i had data there, send it over
#ifdef DEBUG
	cout << "Sending over my local data: "<<dbt_string(&key)<<endl;
#endif
	send_vals(sock,&key,&data,node_data);
	free(data.data);
      }
      key.size++;
    } else {  // an internal node, send its children
      mdb->apply_children(&key,apply_send,&sock);
    }


    if (key.size > 0)
      free(olddata); // was malloced by bdb in the queue call
  }
#ifdef DEBUG
  cout << "Done with merkle_send"<<endl;
#endif
  return 0;
}

struct merkle_recv_args {
  int sock;
  int stat;
  DB* db;
  StorageDB* storageDB;
  MerkleDB* merkleDB;
  TQueue<DBT>* tq;
};

void* merkle_recv(void* arg) {
  char *end;
  int off = 0;
  int ic;
  DBT key,data;
  char dbuf[BUFSZ];
  struct merkle_recv_args* args = (struct merkle_recv_args*)arg;

  int sock = args->sock;
  DB* db = args->db;
  StorageDB* storageDB = args->storageDB;
  MerkleDB* merkleDB = args->merkleDB;
  TQueue<DBT>* tq = args->tq;
  args->stat = 0;
  end = dbuf;
  bool sawMark = false;

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  for (;;) {
    int kf = 0;
    int df = 0;
    try {
      off = fill_dbt(&sock,&key,NULL,dbuf,dbuf+off,&end);
    } catch (ReadDBTException &e) {
      cerr << "Could not read next item in handle_merkle_reply: "<<e.what()<<endl;
      if (key.flags)
	free(key.data);
      return NULL;
    }

    if (off == 0)
      break;

    int code = key.doff;
    key.doff = 0;

    switch (code) {
    case MERKLE_MARK:
      if (sawMark) { // this is a double mark, we're done
#ifdef DEBUG
	cout << "Got mark twice in merkle reply, returning"<<endl;
#endif
	key.doff = 2;
	tq->enqueue(key);
	return NULL;
      } else {
#ifdef DEBUG
	cout << "Got mark in merkle reply, queuing doff=1"<<endl;
#endif
	key.doff = 1;
	tq->enqueue(key);
	sawMark = true;
      }
      break;
    case NODE_DATA: {
#ifdef DEBUG
      cout << "Got data in merkle reply, inserting locally"<<endl;
#endif
      sawMark = false;
      try {
	off = fill_dbt(&sock,&data,&key,dbuf,dbuf+off,&end);
      } catch (ReadDBTException &e) {
	cerr << "Could not read next item in handle_merkle_reply: "<<e.what()<<endl;
	if (key.flags)
	  free(key.data);
	if (data.flags)
	  free(data.data);
	return 0;
      }
      kf = key.flags;
      df = data.flags;
      key.flags = 0;
      data.flags = 0;
#ifdef DEBUG
      cout << "key: "<<dbt_string(&key)<<" data: "<<dbt_string(&data)<<endl;
#endif
      storageDB->putDBTs(db,merkleDB,&key,&data);
      break;
    }
    case MERKLE_YES: {
#ifdef DEBUG
      cout << "Merkle yes in handle"<<endl;
#endif
      sawMark = false;
      kf = key.flags;
      break;
    }
    case MERKLE_NO: {
#ifdef DEBUG
      cout << "Merkle no in handle for: "<<string((char*)key.data,key.size)<<". Need to add this nodes children to queue"<<endl;
#endif
      sawMark = false;
      kf = key.flags;
      key.flags = 0;
      if (!kf) { // key wasn't malloced
	void* d = malloc(sizeof(char)*key.size);
	memcpy(d,key.data,key.size);
	key.data = d;
      }
      tq->enqueue(key);
      kf = 0; // will free in dequeue
    }
      break;
    }
    if (kf)
      free(key.data);
    if (df)
      free(data.data);
  }
}


static DBT last_yes;
static pthread_mutex_t last_yes_tex,sending_tex;

void* merkle_dfs_recv(void* arg) {
  char *end;
  int off = 0;
  int ic;
  DBT key,data, ldata;
  char dbuf[BUFSZ];
  struct merkle_recv_args* args = (struct merkle_recv_args*)arg;

  int sock = args->sock;
  DB* db = args->db;
  StorageDB* storageDB = args->storageDB;
  MerkleDB* merkleDB = args->merkleDB;
  args->stat = 0;
  end = dbuf;

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  memset(&ldata, 0, sizeof(DBT));
  ldata.flags = DB_DBT_MALLOC;

  for (;;) {
    int kf = 0;
    int df = 0;
    try {
      off = fill_dbt(&sock,&key,NULL,dbuf,dbuf+off,&end);
    } catch (ReadDBTException &e) {
      cerr << "Could not read next item in handle_merkle_reply: "<<e.what()<<endl;
      if (key.flags)
	free(key.data);
      return NULL;
    }

    if (off == 0)
      return 0;

    int code = key.doff;
    key.doff = 0;

    switch (code) {
    case NODE_DATA: {
#ifdef DEBUG
      cout << "Got data in merkle reply for: "<<dbt_string(&key)<<endl;
#endif
      try {
	if (data.flags)
	  cout << "BAD!"<<endl;
	off = fill_dbt(&sock,&data,&key,dbuf,dbuf+off,&end);
      } catch (ReadDBTException &e) {
	cerr << "Could not read next item in handle_merkle_reply: "<<e.what()<<endl;
	if (key.flags)
	  free(key.data);
	if (data.flags)
	  free(data.data);
	return 0;
      }
      kf = key.flags;
      df = data.flags;
      key.flags = 0;
      data.flags = 0;
#ifdef DEBUG
      cout << "key: "<<dbt_string(&key)<<" data: "<<dbt_string(&data)<<endl;
#endif
      storageDB->putDBTs(db,merkleDB,&key,&data);
      break;
    }
    case MERKLE_YES: {
#ifdef DEBUG
      cout << "Merkle yes in handle"<<endl;
#endif
      (void)pthread_mutex_lock(&last_yes_tex);
      if (last_yes.size < key.size)
	last_yes.data = realloc(last_yes.data,key.size);
      last_yes.size = key.size;
      memcpy(last_yes.data,key.data,key.size);
      last_yes.flags = 1;
      (void)pthread_mutex_unlock(&last_yes_tex);
      kf = key.flags;
      key.flags = 0;
      break;
    }
    case MERKLE_NO: {
#ifdef DEBUG
      cout << "Merkle no in handle for: "<<string((char*)key.data,key.size)<<endl;
#endif
      cout << "THIS SHOULDN'T HAPPEN"<<endl;
      exit(2);
      if (is_leaf(&key)) {
	key.size--; // remove null term
	int ret = db->get(db,NULL,&key,&ldata,0);
	if (ret == 0) {  // i had data there, send it over
#ifdef DEBUG
	  cout << "Sending over my local data: "<<dbt_string(&key)<<endl;
#endif
	  //(void)pthread_mutex_lock(&sending_tex);
	  //send_vals(sock,&key,&ldata,node_data);
	  //(void)pthread_mutex_unlock(&sending_tex);
	  free(ldata.data);
	}
	key.size++;
      } else
	cerr << "SHOULD NOT GET NO'S FOR NON LEAF NODES IN DFS TRAVERAL"<<endl;
      kf = key.flags;
      key.flags = 0;
    }
      break;
    }
    if (kf)
      free(key.data);
    if (df)
      free(data.data);
  }
}

bool cursor_advance(DBC* cursorp, DBT* key, DBT* data) {
  (void)pthread_mutex_lock(&last_yes_tex);
  if (!last_yes.flags) { // we've already handled this one
    (void)pthread_mutex_unlock(&last_yes_tex);
    return true;
  }
  if (last_yes.size == 0) { // was a yes to our root node
#ifdef DEBUG
    cout << "Yes to root, kicking out"<<endl;
#endif
    (void)pthread_mutex_unlock(&last_yes_tex);
    return false;
  }
  last_yes.flags = 0;
  int s = key->size<last_yes.size?
    key->size:
    last_yes.size;
  int mc = memcmp(key->data,last_yes.data,s);
  if ( (mc > 0) ||
       ( (mc == 0) &&
	 (key->size > last_yes.size)) )  { // i'm already ahead of this
    (void)pthread_mutex_unlock(&last_yes_tex);
    return true;
  }

  // okay, last_yes is ahead of us, let's advance
  free(key->data);
  free(data->data);
  key->data = last_yes.data;
  key->size = last_yes.size;
  char* cd = (char*)key->data;
  cd[key->size-1]++;
  int ret = cursorp->get(cursorp,key,data,DB_SET_RANGE);
  (void)pthread_mutex_unlock(&last_yes_tex);
  return ret==0;
}

// DFS traversal.  just send keys, skipping if needed
void send_merkle_dfs(int sock, DB* db, MerkleDB* mdb) {
  DBC *cursorp;
  DBT key, data, ldata;
  int ret;
  DB* dbp = mdb->dbp;

  /* Get a cursor */
  dbp->cursor(dbp, NULL, &cursorp, 0);

  /* Initialize our DBTs. */
  memset(&key, 0, sizeof(DBT));
  key.flags = DB_DBT_MALLOC;
  memset(&data, 0, sizeof(DBT));
  data.flags = DB_DBT_MALLOC;
  memset(&ldata, 0, sizeof(DBT));
  ldata.flags = DB_DBT_MALLOC;

  /* Iterate over the database, retrieving each record in turn. */
  MerkleNode * mn;
  while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    if(!cursor_advance(cursorp,&key,&data))
      break;
    mn = (MerkleNode *)data.data;

    if (is_leaf(&key)) {
      key.size--; // remove null term
      int ret = db->get(db,NULL,&key,&ldata,0);
      key.size++;
      if (ret == 0) {  // i had data there, send it over
	send_vals(sock,&key,&ldata,node_data);
	free(ldata.data);
      }
    } else {
      send_vals(sock,&key,NULL,node_merkle);
      send_hash(sock,mn->digest);
    }
    free(key.data);
    free(data.data);
  }

#ifdef DEBUG
  cout << "finished sending merkle tree, sending STOP"<<endl;
#endif

  send_merkle_stop(sock);

  if (cursorp != NULL)
    cursorp->close(cursorp);
}

bool StorageDB::
merkle_sync(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy) {
  int numbytes,ret;
  char stat;
  DBT key,data;


  // TODO:MAKE SURE POLICY IS VALID
  DB* dbp = getDB(ns);
  MerkleDB* mdb = getMerkleDB(ns,true);
  if (dbp == NULL ||
      mdb == NULL ||
      mdb->qdb == NULL)
    return true; // nothing to do

  // block the flushing thread, if a flush is currently in progress
  // this will block until it is finished
  flush_lock(false);

  // run a flush now to catch up
  mdb->flushp();

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  int sock = open_socket(h);
  int nslen = ns.length();

  stat = 1; // sync command
  if (send(sock,&stat,1,MSG_MORE) == -1)
    do_throw(errno,"Error sending sync operation: ");

  send_string(sock,ns);

  // DBTSEND OK

  nslen =
    (policy.type == CPT_GREATER)?
    4:
    (12+policy.func.func.length());

  if (send(sock,&node_data,1,MSG_MORE) == -1)
    do_throw(errno,"Failed to send message type: ");

  if (send(sock,&(nslen),4,MSG_MORE) == -1)
    do_throw(errno,"Error sending policy length: ");

  if (send(sock,&(policy.type),4,MSG_MORE) == -1)
    do_throw(errno,"Error sending policy type: ");

  if (policy.type == CPT_FUNC) {
    if (send(sock,&(policy.func.lang),4,MSG_MORE) == -1)
      do_throw(errno,"Error sending policy language: ");

    nslen = policy.func.func.length();
    if (send(sock,&nslen,4,MSG_MORE) == -1)
      do_throw(errno,"Error sending policy function length: ");

    if (send(sock,policy.func.func.c_str(),nslen,MSG_MORE) == -1)
      do_throw(errno,"Error sending policy language: ");
  }

  // gross, but let's serialize the record set okay, let's serialize
  ostringstream os;
  os << rs.type;
  switch (rs.type) {
  case RST_RANGE:
    os << " "<<
      rs.range.start_key<<" "<<
      rs.range.end_key;
    break;
  case RST_KEY_FUNC:
    os << " "<<rs.func.lang<< " "<<rs.func.func;
    break;
  }

  send_string(sock,os.str());

  data.flags = DB_DBT_MALLOC;

  ret = mdb->dbp->get(mdb->dbp,NULL,&key,&data,0);
  if (ret == DB_NOTFOUND) { // no root node?
    cerr << "Umm, no root node found in merkle tree, something's wrong"<<endl;
    return false;
  }
#ifdef DEBUG
  cout << "Found root node, going ahead with sync"<<endl;
#endif


  /*** BFS CODE ***
  TQueue<DBT> tq(100000000);

  // first send the root
  MerkleNode * root =  (MerkleNode *)(data.data);
  send_vals(sock,&key,NULL,node_merkle);
  send_hash(sock,root->digest);
  send_mark(sock);


  struct merkle_recv_args args;
  args.sock = sock;
  args.db = dbp;
  args.storageDB = this;
  args.merkleDB = mdb;
  args.tq = &tq;

  pthread_t recv_thread;
  (void) pthread_create(&recv_thread,NULL,
			merkle_recv,&args);

  stat = merkle_send(sock,dbp,mdb,&tq);
  pthread_join(recv_thread,NULL);
  *** END BFS CODE */

  /*** DFS CODE ***/
  last_yes.flags = 0;
  last_yes.size = 0;
  last_yes.data = NULL;
  if (pthread_mutex_init(&last_yes_tex,NULL))
    do_throw(errno,"Couldn't create last_yes mutex:");
  if (pthread_mutex_init(&sending_tex,NULL)) {
    TException te("Couldn't create sending mutex");
    throw te;
  }

  struct merkle_recv_args args;
  args.sock = sock;
  args.db = dbp;
  args.storageDB = this;
  args.merkleDB = mdb;
  args.tq = NULL;

  pthread_t recv_thread;
  (void) pthread_create(&recv_thread,NULL,
			merkle_dfs_recv,&args);

  send_merkle_dfs(sock,dbp,mdb);

  pthread_join(recv_thread,NULL);

  if ( (ret = (pthread_mutex_destroy(&last_yes_tex))) != 0) {
    switch(ret) {
    case EBUSY: {
      TException te("Couldn't destroy last_yes mutex, it is locked or referenced");
      throw te;
    }
    case EINVAL: {
      TException te("Couldn't destroy last_yes mutex, it is invalid");
      throw te;
    }
    default: {
      TException te("Couldn't destroy last_yes mutex, unknown error");
      throw te;
    }
    }
  }
  if (pthread_mutex_destroy(&sending_tex))
    do_throw(errno,"Couldn't destroy sending mutex:");

  if (last_yes.data != NULL)
    free(last_yes.data);
  /*** END DFS CODE ***/

#ifdef DEBUG
  cout << "Done, sending final done"<<endl;
#endif
  send_string(sock,"");

#ifdef DEBUG
  cout << "Sent final done, gonna wait for final status"<<endl;
#endif

  if ((numbytes = recv(sock, &stat, 1, 0)) == -1)
    do_throw(errno,"Could not read final status: ");

  cout << "okay, all done"<<endl;

  flush_lock(true);
  close(sock);

  if(stat) // non-zero means a fail
    return false;

  if (flush_log(dbp))
    return false;

  return true;
}

bool StorageDB::
sync_set(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy) {
  if (doMerkle)
    return merkle_sync(ns,rs,h,policy);
  else
    return simple_sync(ns,rs,h,policy);
}

}
