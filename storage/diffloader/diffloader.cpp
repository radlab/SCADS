// load data in 100-byte records for grep test
// loads 100 byte records
// records will be sequential ids starting from a starting key (default 0, use -k to change)

// -s [megabytes] to set number of megabytes to load (default: 10 megs)
// -p [percent] percent difference between the nodes
// -P [port for host 1] (default: 9091)
// -Q [port for host 2] (default: 9091)
// -q quiet,don't print anything (except timing if you asked for it)
// -t print load time
// -k [start key] first key to load (default: 0)

#include <string.h>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>

#include <stdio.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <arpa/inet.h>

#define VERSTR "SCADSBDB0.1"

#ifndef MSG_MORE
#define MSG_MORE 0
#endif

#include <sys/time.h>
#include <time.h>

#include <math.h>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "gen-cpp/Storage.h"

#include <iostream>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace SCADS;

using namespace boost;

#define THRIFT_HOST "localhost"
#define THRIFT_PORT 9091

// timing stuff
static struct timeval start_time, cur_time, diff_time;

#define start_timing() if(timing) gettimeofday(&start_time,NULL)
#define end_timing() {							\
    if (timing) {							\
      gettimeofday(&cur_time,NULL);					\
      timersub(&cur_time,&start_time,&diff_time);			\
      cout << "Took "<<diff_time.tv_sec <<" seconds, and " << diff_time.tv_usec<<" microseconds."<<endl; \
    }									\
  }

static void printProgress(int perc, unsigned long curKey) {
  int i;
  int ne = perc/4; //using 25 spaces
  ostringstream oss;
  cout << "\rLoading: [";
  for (i = 0;i < ne;i++)
    cout << "=";
  for (i = (ne+1);i < 25;i++)
    cout << " ";
  cout << "] "<<perc<<"% (Key: "<<setfill('0')<<setw(10)<<curKey<<")";
  flush(cout);
}

void fillval(char* val,char* pattern, char pos) {
  for (int i = 0;i < 90;i++) {
    val[i] = ((random()%93)+32);
    if (val[i]==*pattern)
      val[i]++; // ensures we don't get pattern in anything we generate
  }
  val[90]='\0';
  if (pos) {
    int p = (random()%86);
    val[p]=*pattern;
    val[p+1]=*(pattern+1);
    val[p+2]=*(pattern+2);
  }
}

int open_sock(const char* host, char* port) {
  int sockfd,rv;
  struct addrinfo hints, *servinfo, *p;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((rv = getaddrinfo(host, port, &hints, &servinfo)) != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
    return -1;
  }

  // loop through all the results and connect to the first we can
  for(p = servinfo; p != NULL; p = p->ai_next) {
    if ((sockfd = socket(p->ai_family, p->ai_socktype,
			 p->ai_protocol)) == -1) {
      perror("client: socket");
      continue;
    }

    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(sockfd);
      perror("client: connect");
      continue;
    }
      
    break;
  }

  if (p == NULL) {
    fprintf(stderr, "client: failed to connect\n");
    return -1;
  }

  freeaddrinfo(servinfo); // all done with this structure
  return sockfd;
}

char node_data = 0;

void sendval(int sock,int* len, char* val) {
  if (send(sock,&node_data,1,MSG_MORE) == -1) {
    perror("Error sending data type: ");
    exit(2);
  }
  if (send(sock,len,4,MSG_MORE) == -1) {
    perror("Error len: ");
    exit(2);
  }
  if (send(sock,val,*len,MSG_MORE) == -1) {
    perror("Error sending data: ");
    exit(2);
  }
}

int main(int argc,char* argv[]) {
  int size = 10;
  double perc = 0.0;
  char pattern[4];
  char key[11];
  char val[91];
  char diffval[91];
  int opt,timing = 0;
  char loadType = 1;
  char quiet = 0;
  long startkey = 0;
  char stat;

  unsigned int s;
  FILE* f = fopen("/dev/urandom","r");
  fread(&s,sizeof(unsigned int),1,f);
  fclose(f);
  srand(s);


  sprintf(pattern,"%s","foo");
  int port1 = 9091;
  int port2 = 8081;
  while ((opt = getopt(argc,argv, "s:p:P:Q:qtk:")) != -1) {
    switch (opt) {
    case 's':
      size = atoi(optarg);
      break;
    case 'p':
      perc = atof(optarg);
      break;
    case 'P':
      port1 = atoi(optarg);
      break;
    case 'Q':
      port2 = atoi(optarg);
      break;
    case 'q':
      quiet = 1;
      break;
    case 't':
      timing = 1;
      break;
    case 'k':
      startkey = atol(optarg);
      break;
    default:
      fprintf(stderr,"Usage: %s [-Ttq] -s [size] -p [percent] -c [pattern] -P [port] -k [start key]\n",argv[0]);
      exit(EXIT_FAILURE);
    }
  }
  const char* host1 = argv[optind];
  const char* host2 = argv[optind+1];

  if (timing & !quiet)
    cout << "Will print timing info"<<endl;

  unsigned long records = size*10000;
  unsigned long rlim = records+startkey;
  int diff;
  if (perc > 0)
    diff = ceil(perc/100*records);
  else
    diff = 0;
  int m;
  if (perc > 0)
    m = (records/diff)-1;
  else
    m = 1000;
  int pcount = 0;

  if (!quiet)
    cout << "loading "<<records<<" records. ("<<size<<" megs). "<<diff<<" with differnt pattern: "<<pattern<<endl;

  
  int sock1, sock2, numbytes;  
  int keylen = 10;
  int dlen = 90;
  int rv;
  int nslen;
  char buf[12];
  string ns("difftest");

  fillval(val,pattern,0);
  val[0]='b';
  memcpy(diffval,val,91);
  diffval[0]='a';

  if (!quiet)
    cout << "doing a binary load on copy/sync port"<<endl;

  sprintf(buf,"%i",port1);
  sock1 = open_sock(host1,buf);
  if (sock1 == -1) {
    printf("Couldn't connect to host1\n");
    return 1;
  }
  sprintf(buf,"%i",port2);
  sock2 = open_sock(host2,buf);
  if (sock2 == -1) {
    printf("Couldn't connect to host2\n");
    return 1;
  }

  if ((numbytes = recv(sock1, buf, 11, 0)) == -1) {
    perror("Error receiving version string 1: ");
    return 2;
  }

  buf[numbytes] = '\0';
    
  if (strncmp(VERSTR,buf,11)) {
    fprintf(stderr,"Version strings 1 didn't match");
    return 2;
  }

  if ((numbytes = recv(sock2, buf, 11, 0)) == -1) {
    perror("Error receiving version string 2: ");
    return 2;
  }
  
  buf[numbytes] = '\0';
    
  if (strncmp(VERSTR,buf,11)) {
    fprintf(stderr,"Version strings 2 didn't match");
    return 2;
  }

  // now send copy
  nslen = 0;
  if (send(sock1,&nslen,1,MSG_MORE) == -1) {
    perror("Error sending copy op: ");
    return 2;
  }
  if (send(sock2,&nslen,1,MSG_MORE) == -1) {
    perror("Error sending copy op: ");
    return 2;
  }


  // now send our namespace
  nslen = ns.length();
  printf("Sending namespace: %s\n",ns.c_str());
  if (send(sock1,&node_data,1,MSG_MORE) == -1) {
    perror("Error sending data type: ");
    return 2;
  }
  if (send(sock1,&nslen,4,MSG_MORE) == -1) {
    perror("Error sending namespace length: ");
    return 2;
  }
  if (send(sock1,ns.c_str(),nslen,MSG_MORE) == -1) {
    perror("Error sending namespace: ");
    return 2;
  }
  if (send(sock2,&node_data,1,MSG_MORE) == -1) {
    perror("Error sending data type 2: ");
    return 2;
  }
  if (send(sock2,&nslen,4,MSG_MORE) == -1) {
    perror("Error sending namespace length 2: ");
    return 2;
  }
  if (send(sock2,ns.c_str(),nslen,MSG_MORE) == -1) {
    perror("Error sending namespace 2: ");
    return 2;
  }


  if (!quiet)
    printProgress(0,0);
  start_timing();
  // now send all our keys
  for (unsigned long cr = 0,i = startkey;i < rlim;i++,cr++) {
    if (!quiet && (cr % 1000 == 0))
      printProgress(((100*cr)/records),i);
      
    sprintf(key,"%010li",i);
    sendval(sock1,&keylen,key);
    sendval(sock1,&dlen,val);
    sendval(sock2,&keylen,key);
    //if (cr%m==0 && pcount<diff) {
    if (pcount<diff) {
      pcount++;
      sendval(sock2,&dlen,diffval);
    }
    else
      sendval(sock2,&dlen,val);
  }
  keylen = 0;
  if (send(sock1,&node_data,1,MSG_MORE) == -1) {
    perror("Error sending data type: ");
    return 2;
  }
  if (send(sock2,&node_data,1,MSG_MORE) == -1) {
    perror("Error sending data type: ");
    return 2;
  }
  if (send(sock1,&keylen,4,MSG_MORE) == -1) {
    perror("Error sending final key: ");
    return 1;
  }
  if (send(sock2,&keylen,4,MSG_MORE) == -1) {
    perror("Error sending final key: ");
    return 1;
  }
  if (!quiet) {
    printProgress(100,(rlim-1));
    cout<<endl<<"Waiting for success code."<<endl;
  }

  if ((numbytes = recv(sock1, &stat, 1, 0)) == -1)  
    cerr << "Could not read final status on host 1, your load might not have worked"<<endl;
  else if(!quiet)
    cout << "Done on host 1"<<endl;
  if ((numbytes = recv(sock2, &stat, 1, 0)) == -1)  
    cerr << "Could not read final status on host 2, your load might not have worked"<<endl;
  else if(!quiet)
    cout << "Done on host 2"<<endl;

  end_timing();
}
