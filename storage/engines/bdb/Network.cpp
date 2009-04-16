#include "StorageDB.h"

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <arpa/inet.h>

#include <string>
#include <iostream>

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

  while(!stopping) {
    char *end;
    int off = 0;
    peer_addr_len = sizeof(struct sockaddr_storage);
    as = accept(sock,(struct sockaddr *)&peer_addr,&peer_addr_len);

    inet_ntop(peer_addr.ss_family,
	      get_in_addr((struct sockaddr *)&peer_addr),
	      abuf, sizeof(abuf));
#ifdef DEBUG
    printf("server: got connection from %s\n", abuf);
#endif
    
    // fire off new thread here
    {
      DB* db_ptr;
      DBT k,d;
      int fail = 0;
      if (send(as,VERSTR,11,0) == -1) {
	perror("send");
	close(as);
	continue;
      }

      // do all the work
      memset(&k, 0, sizeof(DBT));
      end = dbuf;
      off = fill_dbt(&as,&k,NULL,dbuf,dbuf,&end);

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
	off = fill_dbt(&as,&k,NULL,dbuf,dbuf+off,&end);
	if (off == 0)
	  break;
	off = fill_dbt(&as,&d,&k,dbuf,dbuf+off,&end);
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
#ifdef DEBUG      
      cout << "done"<<endl;
#endif
      if (send(as,&fail,1,0) == -1) 
	perror("send STAT");
      close(as);
    }
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
  int nslen = ns.length();

  if (send(sock,&nslen,4,MSG_MORE) == -1) 
    do_throw(errno,"Error sending namespace length: ");
  
  if (send(sock,ns.c_str(),nslen,MSG_MORE) == -1) 
    do_throw(errno,"Error sending namespace: ");

  apply_to_set(ns,rs,apply_copy,&sock);
  
  // send done message
  nslen = 0;
  if (send(sock,&nslen,4,0) == -1) 
    do_throw(errno,"Failed to send done message: ");

  if ((numbytes = recv(sock, &stat, 1, 0)) == -1) 
    do_throw(errno,"Could not read final status: ");

  if(stat) // non-zero means a fail
    return false;

  return true;
}

struct sync_args {
  int sock;
  const ConflictPolicy policy;
};


void apply_sync(void* s, DB* db, void* k, void* d) {
  struct sync_args* args = (struct sync_args*)d;
  int sock = args->sock;
  DBT *key,*data;
  key = (DBT*)k;
  data = (DBT*)d;
  if (send(sock,&(key->size),4,MSG_MORE) == -1) 
    do_throw(errno,"Failed to send data length: ");
  if (send(sock,((const char*)key->data),key->size,MSG_MORE) == -1)
    do_throw(errno,"Failed to send a key: ");
  if (send(sock,&(data->size),4,MSG_MORE) == -1) 
    do_throw(errno,"Failed to send data length: ");
  if (send(sock,data->data,data->size,MSG_MORE) == -1)
    do_throw(errno,"Failed to send data: ");
}

bool StorageDB::
sync_set(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy) {
  int numbytes;
  char stat;

  if(1)return false;

  int sock = open_socket(h);
  int nslen = ns.length();

  if (send(sock,&nslen,4,MSG_MORE) == -1) 
    do_throw(errno,"Error sending namespace length: ");
  
  if (send(sock,ns.c_str(),nslen,MSG_MORE) == -1) 
    do_throw(errno,"Error sending namespace: ");

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

  apply_to_set(ns,rs,apply_copy,&sock);
  
  // send done message
  nslen = 0;
  if (send(sock,&nslen,4,0) == -1) 
    do_throw(errno,"Failed to send done message: ");

  if ((numbytes = recv(sock, &stat, 1, 0)) == -1) 
    do_throw(errno,"Could not read final status: ");

  if(stat) // non-zero means a fail
    return false;

  return true;
}


}
