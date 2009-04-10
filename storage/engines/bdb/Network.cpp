#include "StorageDB.h"

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

int fill_dbt(int* sock, DBT* k, char* buf, char* pos, char** endp) {
  int len,used;
  char *end = *endp;
  if (k->flags)
    len = k->size;
  else { 
    if ((end-pos) < 4) {
      memcpy(buf,pos,(end-pos)); // move data to the front
      len = fill_buf(sock,buf,(end-pos));
      *endp = buf+len;
      return fill_dbt(sock,k,buf,buf,endp);
    }
    memcpy(&len,pos,4);
    k->size = len;
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
    len = fill_buf(sock,buf,0);
    *endp = buf+len;
    return fill_dbt(sock,k,buf,buf,endp);
  }
  else {
    if (k->flags) {
      memcpy(((char*)k->data)+k->dlen,pos,end-pos);
      used = end-pos;
    }
    else {
      k->data = pos+4;
      used = len+4;
    }
  }

  return used;
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
    int usd = 0;
    peer_addr_len = sizeof(struct sockaddr_storage);
    as = accept(sock,(struct sockaddr *)&peer_addr,&peer_addr_len);

    inet_ntop(peer_addr.ss_family,
	      get_in_addr((struct sockaddr *)&peer_addr),
	      abuf, sizeof(abuf));
    printf("server: got connection from %s\n", abuf);
    
    // fire off new thread here
    {
      DB* db_ptr;
      DBT k,d;
      if (send(as,VERSTR,11,0) == -1) {
	perror("send");
	close(as);
	continue;
      }

      // do all the work
      memset(&k, 0, sizeof(DBT));
      end = dbuf;
      off = fill_dbt(&as,&k,dbuf,dbuf,&end);

      string ns = string((char*)k.data,k.size);
      if (k.flags)
	free(k.data);

      cout << "Namespace is: "<<ns<<endl;
      db_ptr = storageDB->getDB(ns);

      for(;;) { // now read all our key/vals
	int kf,df;
	memset(&k, 0, sizeof(DBT));
	memset(&d, 0, sizeof(DBT));
	usd = fill_dbt(&as,&k,dbuf,dbuf+off,&end);
	if (usd == 0)
	  break;
	off+=usd;
	usd = fill_dbt(&as,&d,dbuf,dbuf+off,&end);
	off+=usd;
	cout << "key: "<<string((char*)k.data,k.size)<<" data: "<<string((char*)d.data,d.size)<<endl;
	kf=k.flags;
	df=d.flags;
	k.flags = 0;
	k.dlen = 0;
	d.flags = 0;
	d.dlen = 0;

	if (db_ptr->put(db_ptr, NULL, &k, &d, 0) != 0) {
	  cerr << "Fail to put!"<<endl;
	  exit(EXIT_FAILURE);
	}	

	if (kf)
	  free(k.data);
	if (df)
	  free(d.data);
      }
      
      cout << "done"<<endl;

      close(as);
    }
  }


  printf("Shutting down listen thread\n");
}

void apply_copy(void* s, DB* db, void* k, void* d) {
  int *sock = (int*)s;
  DBT *key,*data;
  key = (DBT*)k;
  data = (DBT*)d;
  if (send(*sock,&(key->size),4,MSG_MORE) == -1) {
    TException te("Failed to send data len");
    throw te;
  }
  if (send(*sock,((const char*)key->data),key->size,MSG_MORE) == -1) {
    TException te("Failed to send a key");
    throw te;
  }
  if (send(*sock,&(data->size),4,MSG_MORE) == -1) {
    TException te("Failed to send data len");
    throw te;
  }
  if (send(*sock,data->data,data->size,MSG_MORE) == -1) {
    TException te("Failed to send data");
    throw te;
  }
}


bool StorageDB::
copy_set(const NameSpace& ns, const RecordSet& rs, const Host& h) {
  int sock, numbytes;
  struct addrinfo hints, *res, *rp;

  int rv;
  char s[INET6_ADDRSTRLEN];
  char buf[12];

  string::size_type loc;
  loc = h.find_last_of(':');
  if (loc == string::npos) { // :
    TException te("Host parameter must be of form host:port");
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
      perror("copy_set: socket");
      continue;
    }
    
    if (connect(sock, rp->ai_addr, rp->ai_addrlen) == -1) {
      close(sock);
      perror("copy_set: connect");
      continue;
    }
    
    break;
  }

  if (rp == NULL) {
    TException te("Could not connect\n");
    throw te;
  }
  
  inet_ntop(rp->ai_family, get_in_addr((struct sockaddr *)rp->ai_addr),
            s, sizeof s);
  printf("copy: connecting to %s\n", s);
  
  freeaddrinfo(res);
  
  if ((numbytes = recv(sock, buf, 11, 0)) == -1) {
    perror("recv");
    exit(1);
  }
  
  buf[numbytes] = '\0';
  
  if (strncmp(VERSTR,buf,11)) {
    TException te("Version strings didn't match for copy");
    throw te;
  }

  int nslen = ns.length();
  if (send(sock,&nslen,4,MSG_MORE) == -1) {
    TException te("Failed to send namespace len");
    throw te;
  }
  
  if (send(sock,ns.c_str(),ns.length(),MSG_MORE) == -1) {
    perror("send ns");
    TException te("Failed to send namespace");
    throw te;
  }


  apply_to_set(ns,rs,apply_copy,&sock);
  
  // send done message
  nslen = 0;
  if (send(sock,&nslen,4,0) == -1) {
    TException te("Failed to send done message");
    throw te;
  }
  
  return true;
}

bool StorageDB::
sync_set(const NameSpace& ns, const RecordSet& rs, const Host& h, const ConflictPolicy& policy) {
  return false;
}


}
