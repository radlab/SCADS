#include "StorageDB.h"

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

extern char stopping;

namespace SCADS {

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
    exit(EXIT_FAILURE);
  }
#ifdef DEBUG
  if (recvd > 0) {
    printf("filled buffer:\n[");
    for(int i = 0;i<recvd;i++) 
      printf("%i ",buf[i]);
    printf("\b]\n");
  }
#endif
  return recvd;
}

int fill_dbt(int* sock, DBT* k, DBT* other, char* buf, char* pos, char** endp) {
  int len;
  char *end = *endp;
  if (k->flags)
    len = (k->size - k->dlen);
  else { 
    if ((end-pos) < 4) {
      memcpy(buf,pos,(end-pos)); // move data to the front
      if (other != NULL &&
	  !other->flags) { // other dbt isn't malloced and needs its data saved and moved
	void* od = malloc(sizeof(char)*other->size);
	memcpy(od,other->data,other->size);
	other->data = od;
	other->flags = 1; // is malloced now
      }
      len = fill_buf(sock,buf,(end-pos));
      *endp = buf+len;
      return fill_dbt(sock,k,other,buf,buf,endp);
    }
    memcpy(&len,pos,4);
    k->size = len;
    pos+=4;
  }
  if (len == 0) // means we're done
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

int do_copy(int sock, StorageDB* storageDB, char* dbuf) {
  char *end;
  int off = 0;
  DB* db_ptr;
  DBT k,d;
  int fail = 0;

  // do all the work
  memset(&k, 0, sizeof(DBT));
  end = dbuf;
  off = fill_dbt(&sock,&k,NULL,dbuf,dbuf,&end);

  string ns = string((char*)k.data,k.size);
  if (k.flags)
    free(k.data);

#ifdef DEBUG
  cout << "Namespace is: "<<ns<<endl;
#endif
  db_ptr = storageDB->getDB(ns);

  for(;;) { // now read all our key/vals
    int kf,df;
    memset(&k, 0, sizeof(DBT));
    memset(&d, 0, sizeof(DBT));
    off = fill_dbt(&sock,&k,NULL,dbuf,dbuf+off,&end);
    if (off == 0)
      break;
    off = fill_dbt(&sock,&d,&k,dbuf,dbuf+off,&end);
#ifdef DEBUG
    cout << "key: "<<string((char*)k.data,k.size)<<" data: "<<string((char*)d.data,d.size)<<endl;
#endif
    kf=k.flags;
    df=d.flags;
    k.flags = 0;
    k.dlen = 0;
    d.flags = 0;
    d.dlen = 0;

    if (db_ptr->put(db_ptr, NULL, &k, &d, 0) != 0) {
      fail = 1;
      break;
    }

    if (kf)
      free(k.data);
    if (df)
      free(d.data);
  }
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
  char* dbuf;
  char *end;
  ConflictPolicy* pol;
  DBT k,d;
  int remdone;
};

void send_vals(int sock,DBT* key, DBT* data) {
  if (send(sock,&(key->size),4,MSG_MORE) == -1) {
    cerr<<"Failed to send data length: "<<strerror(errno)<<endl;
    return;
  }
  if (send(sock,((const char*)key->data),key->size,MSG_MORE) == -1) {
    cerr<<"Failed to send a key: "<<strerror(errno)<<endl;
    return;
  }
  if (send(sock,&(data->size),4,MSG_MORE) == -1) {
    cerr<<"Failed to send data length: "<<strerror(errno)<<endl;
    return;
  }
  if (send(sock,data->data,data->size,MSG_MORE) == -1) {
    cerr<<"Failed to send data: "<<strerror(errno)<<endl;
    return;
  }
}

void sync_sync(void* s, DB* db, void* k, void* d) {
  int kf,df,minl;
  DBT *key,*data;
  struct sync_sync_args* args = (struct sync_sync_args*)s;
  key = (DBT*)k;
  data = (DBT*)d;  


  if (args->remdone) { // no more remote keys, just send over whatever else we have
    send_vals(args->sock,key,data);
    return;
  }

  if (args->k.size == 0) { // need a new one
    args->off = fill_dbt(&(args->sock),&(args->k),NULL,args->dbuf,args->dbuf+(args->off),&(args->end));
    if (args->off == 0) {
      args->remdone = 1;
      send_vals(args->sock,key,data);
      return;
    }
    args->off = fill_dbt(&(args->sock),&(args->d),&(args->k),args->dbuf,args->dbuf+(args->off),&(args->end));
#ifdef DEBUG
    cout << "[read for sync] key: "<<string((char*)args->k.data,args->k.size)<<" data: "<<string((char*)args->d.data,args->d.size)<<endl;
#endif
  }
  
  if (key == NULL) {
    // we have no local values, this is basically a copy
#ifdef DEBUG
    cerr<<"No local data, inserting all remote keys"<<endl;
#endif
    for(;;) { 
      int kf,df;

      if (args->k.size == 0) {
	args->off = fill_dbt(&(args->sock),&(args->k),NULL,args->dbuf,args->dbuf+(args->off),&(args->end));
	if (args->off == 0) {
#ifdef DEBUG
	  cerr<<"Okay, read all remote keys, returning"<<endl;
#endif
	  return;
	}
	args->off = fill_dbt(&(args->sock),&(args->d),&(args->k),args->dbuf,args->dbuf+(args->off),&(args->end));
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

      if (db->put(db, NULL, &(args->k), &(args->d), 0) != 0) 
	cerr << "Failed to put remote key: "<<string((char*)args->k.data,args->k.size)<<endl;

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
      cerr << "Local key is shorter, sending my local value over"<<endl;
#endif
      send_vals(args->sock,key,data);
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
      cerr << "Local key is longer, Inserting to catch up."<<endl;
#endif

      if (db->put(db, NULL, &(args->k), &(args->d), 0) != 0) 
	cerr << "Failed to put remote key: "<<string((char*)args->k.data,args->k.size)<<endl;

      if (kf)
	free(args->k.data);
      if (df)
	free(args->d.data);

      memset(&(args->k), 0, sizeof(DBT));
      memset(&(args->d), 0, sizeof(DBT));  
      args->off = fill_dbt(&(args->sock),&(args->k),NULL,args->dbuf,args->dbuf+args->off,&(args->end));
      if (args->off == 0) {
	args->remdone = 1;
	return; // kick out, we'll see that there's no more remote keys and send over anything else we have
      }
      args->off = fill_dbt(&(args->sock),&(args->d),&(args->k),args->dbuf,args->dbuf+args->off,&(args->end));
#ifdef DEBUG
      cout << "[read for sync] key: "<<string((char*)args->k.data,args->k.size)<<" data: "<<string((char*)args->d.data,args->d.size)<<endl;
#endif
    }

    // go back to top since we've gone from greater local to greater remote
    if (key->size != args->k.size) continue;

    // okay, keys are the same length, let's see what we need to do
    int cmp = strncmp((char*)key->data,(char*)args->k.data,key->size);
    if (cmp < 0) {
      // local key is shorter, send it over
#ifdef DEBUG
      cerr << "Local key is less, sending over."<<endl;
#endif
      send_vals(args->sock,key,data);
      // don't want to clear keys, will deal with on next pass
      return;
    }

    if (cmp > 0) {
      // remote key is shorter
      // need to keep inserting remote keys until we catch up
#ifdef DEBUG
      cerr << "Local key is greater, inserting to catch up."<<endl;
#endif
      kf=args->k.flags;
      df=args->d.flags;
      args->k.flags = 0;
      args->k.dlen = 0;
      args->d.flags = 0;
      args->d.dlen = 0;

      if (db->put(db, NULL, &(args->k), &(args->d), 0) != 0) 
	cerr << "Failed to put remote key: "<<string((char*)args->k.data,args->k.size)<<endl;

      if (kf)
	free(args->k.data);
      if (df)
	free(args->d.data);

      memset(&(args->k), 0, sizeof(DBT));
      memset(&(args->d), 0, sizeof(DBT));  
      args->off = fill_dbt(&(args->sock),&(args->k),NULL,args->dbuf,args->dbuf+args->off,&(args->end));
      if (args->off == 0) {
	args->remdone = 1;
	return; // ditto to above remdone comment
      }
      args->off = fill_dbt(&(args->sock),&(args->d),&(args->k),args->dbuf,args->dbuf+args->off,&(args->end));
#ifdef DEBUG
      cout << "[read for sync] key: "<<string((char*)args->k.data,args->k.size)<<" data: "<<string((char*)args->d.data,args->d.size)<<endl;
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
  
  int dcmp;
  if (data->size == args->d.size)
    dcmp = memcmp(data->data,args->d.data,data->size);

  if (data->size != args->d.size || dcmp) { // data didn't match
    // put if for ruby func here
    if (args->pol->type == CPT_GREATER) {

      if (data->size > args->d.size ||
	  dcmp > 0) { // local is greater, keep that, send over our value
#ifdef DEBUG
      cerr << "Local data is greater, sending over."<<endl;
#endif
	send_vals(args->sock,key,data);
      }

      else if (data->size < args->d.size ||
	       dcmp < 0) { // remote is greater, insert, no need to send back
#ifdef DEBUG
      cerr << "Local data is less, inserting greater value locally."<<endl;
#endif
	if (db->put(db, NULL, &(args->k), &(args->d), 0) != 0) 
	  cerr << "Failed to put remote key: "<<string((char*)args->k.data,args->k.size)<<endl;
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

int do_sync(int sock, StorageDB* storageDB, char* dbuf) {
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
  off = fill_dbt(&sock,&k,NULL,dbuf,dbuf+off,&end); // read the namespace

  string ns = string((char*)k.data,k.size);
  if (k.flags)
    free(k.data);

#ifdef DEBUG
  cout << "Namespace is: "<<ns<<endl;
#endif

  off = fill_dbt(&sock,&k,NULL,dbuf,dbuf+off,&end); // read the policy
  if (deserialize_policy((char*)k.data,&policy)) {
    cerr<<"Failed to read sync policy"<<endl;
    return 1;
  }

#ifdef DEBUG
  cout<<"Policy type: "<<
    (policy.type == CPT_GREATER?"greater":"userfunc:\n ")<<
    (policy.type == CPT_GREATER?"":policy.func.func)<<endl;
#endif

  off = fill_dbt(&sock,&k,NULL,dbuf,dbuf+off,&end); // read the record set
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

  struct sync_sync_args args;
  args.sock = sock;
  args.dbuf = dbuf;
  args.end = end;
  args.off = off;
  args.pol = &policy;
  args.remdone = 0;
  memset(&(args.k), 0, sizeof(DBT));
  memset(&(args.d), 0, sizeof(DBT));

  storageDB->apply_to_set(ns,rs,sync_sync,&args,true);

#ifdef DEBUG
  cerr << "sync_sync set done, sending end message"<<endl;
#endif

  type = 0;
  if (send(sock,&type,4,MSG_MORE) == -1) 
    cerr<<"Error sending end of sync_sync"<<endl;

  return 0;
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
    fprintf(stderr, "Bind failed\n");
    exit(1);
  }

  freeaddrinfo(res);

  if (listen(sock,BACKLOG) == -1) {
    perror("listen");
    exit(EXIT_FAILURE);
  }
      
  printf("Listening for sync/copy on port %s...\n",dbuf);

  peer_addr_len = sizeof(struct sockaddr_storage);

  while(!stopping) {
    as = accept(sock,(struct sockaddr *)&peer_addr,&peer_addr_len);

    inet_ntop(peer_addr.ss_family,
	      get_in_addr((struct sockaddr *)&peer_addr),
	      abuf, sizeof(abuf));
#ifdef DEBUG
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
      stat = do_sync(as,storageDB,dbuf);
    else {
      cerr <<"Unknown operation requested on copy/sync port"<<endl;
      close(as);
      continue;
    }

#ifdef DEBUG      
    cout << "done.  stat: "<<stat<<endl;
#endif
    if (send(as,&stat,1,0) == -1) 
      perror("send STAT");
    close(as);
  }


  printf("Shutting down listen thread\n");
}

void do_throw(int errnum, string msg) {
  char* err = strerror(errnum);
  int b = strlen(err);
  msg.append(err);
  TException te(msg);
  throw te;
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

void apply_copy(void* s, DB* db, void* k, void* d) {
  int *sock = (int*)s;
  DBT *key,*data;
  key = (DBT*)k;
  data = (DBT*)d;
  if (send(*sock,&(key->size),4,MSG_MORE) == -1) 
    do_throw(errno,"Failed to send data length: ");
  if (send(*sock,((const char*)key->data),key->size,MSG_MORE) == -1)
    do_throw(errno,"Failed to send a key: ");
  if (send(*sock,&(data->size),4,MSG_MORE) == -1) 
    do_throw(errno,"Failed to send data length: ");
  if (send(*sock,data->data,data->size,MSG_MORE) == -1)
    do_throw(errno,"Failed to send data: ");
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

  int nslen = ns.length();  
  if (send(sock,&nslen,4,MSG_MORE) == -1) 
    do_throw(errno,"Error sending namespace length: ");
  
  if (send(sock,ns.c_str(),nslen,MSG_MORE) == -1) 
    do_throw(errno,"Error sending namespace: ");

  apply_to_set(ns,rs,apply_copy,&sock);
  
  // send done message
  nslen = 0;
  if ((numbytes = send(sock,&nslen,4,0)) == -1)
    do_throw(errno,"Failed to send done message: ");

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
void sync_send(void* s, DB* db, void* k, void* d) {
  int *sock = (int*)s;
  DBT *key,*data;
  key = (DBT*)k;
  data = (DBT*)d;
#ifdef DEBUG
  cerr << "[Sending for sync] key: "<<string((char*)key->data,key->size)<<" data: "<<string((char*)data->data,data->size)<<endl;
#endif
  if (send(*sock,&(key->size),4,MSG_MORE) == -1) 
    do_throw(errno,"Failed to send key length: ");
  if (send(*sock,((const char*)key->data),key->size,MSG_MORE) == -1)
    do_throw(errno,"Failed to send a key: ");
  if (send(*sock,&(data->size),4,MSG_MORE) == -1) 
    do_throw(errno,"Failed to send data length: ");
  if (send(*sock,data->data,data->size,MSG_MORE) == -1)
    do_throw(errno,"Failed to send data: ");
}

struct sync_recv_args {
  int sock;
  DB* db_ptr;
};

// receive keys as a response from a sync and insert them
// arg should be the namespace for the sync
void* sync_recv(void* arg) {
  char *end;
  int off = 0;
  DBT k,d;
  char dbuf[BUFSZ];
  struct sync_recv_args* args = (struct sync_recv_args*)arg;
  
  int sock = args->sock;
  DB* db_ptr = args->db_ptr;
  end = dbuf;

  for(;;) { // now read all our key/vals
    int kf,df;
    memset(&k, 0, sizeof(DBT));
    memset(&d, 0, sizeof(DBT));
    off = fill_dbt(&sock,&k,NULL,dbuf,dbuf+off,&end);
    if (off == 0) {
      cerr << "off is 0, breaking"<<endl;
      break;
    }
    off = fill_dbt(&sock,&d,&k,dbuf,dbuf+off,&end);
#ifdef DEBUG
    cout << "[to update] key: "<<string((char*)k.data,k.size)<<" [synced] data: "<<string((char*)d.data,d.size)<<endl;
#endif
    kf=k.flags;
    df=d.flags;
    k.flags = 0;
    k.dlen = 0;
    d.flags = 0;
    d.dlen = 0;

    if (db_ptr->put(db_ptr, NULL, &k, &d, 0) != 0) {
      cerr<<"Couldn't insert synced key: "<<string((char*)k.data,k.size)<<endl;
      continue;
    }
    
    if (kf)
      free(k.data);
    if (df)
      free(d.data);
  }
}

bool StorageDB::
sync_set(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy) {
  int numbytes;
  char stat;

  // TODO:MAKE SURE POLICY IS VALID

  int sock = open_socket(h);
  int nslen = ns.length();

  stat = 1; // sync command
  if (send(sock,&stat,1,MSG_MORE) == -1) 
    do_throw(errno,"Error sending sync operation: ");

  if (send(sock,&nslen,4,MSG_MORE) == -1) 
    do_throw(errno,"Error sending namespace length: ");
  
  if (send(sock,ns.c_str(),nslen,MSG_MORE) == -1) 
    do_throw(errno,"Error sending namespace: ");
  
  nslen =
    (policy.type == CPT_GREATER)?
    4:
    (12+policy.func.func.length());
  
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

  nslen = os.str().length();
  if (send(sock,&nslen,4,MSG_MORE) == -1) 
    do_throw(errno,"Error sending RecordSet length: ");
  if (send(sock,os.str().c_str(),nslen,MSG_MORE) == -1)
    do_throw(errno,"Error sending RecordSet for sync: ");

  struct sync_recv_args args;
  args.sock = sock;
  args.db_ptr = getDB(ns);

  pthread_t recv_thread;
  (void) pthread_create(&recv_thread,NULL,
			sync_recv,&args);

  apply_to_set(ns,rs,sync_send,&sock);
  
  // send done message
  nslen = 0;
  if (send(sock,&nslen,4,0) == -1) 
    do_throw(errno,"Failed to send done message: ");

  // wait for recv thread to read back all the keys
  pthread_join(recv_thread,NULL);

  if ((numbytes = recv(sock, &stat, 1, 0)) == -1) 
    do_throw(errno,"Could not read final status: ");


  close(sock);

  if(stat) // non-zero means a fail
    return false;

  return true;
}


}
