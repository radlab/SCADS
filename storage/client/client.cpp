// simple client

#include <string.h>
#include <sstream>
#include <iostream>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>

#include<readline/readline.h>
#include<readline/history.h>

#include <sys/time.h>
#include <time.h>


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

static int timestamp = 0;

// Set these to the location and port of your thrift interface
#define THRIFT_HOST "localhost"
#define THRIFT_PORT 9090

// timing stuff
static struct timeval start_time, cur_time, diff_time;

#define start_timing() if(timing) gettimeofday(&start_time,NULL)
#define end_timing() {				\
    if (timing) {				\
      gettimeofday(&cur_time,NULL);					\
      timersub(&cur_time,&start_time,&diff_time);			\
      cout << "Took "<<diff_time.tv_sec <<" seconds, and " << diff_time.tv_usec<<" microseconds."<<endl; \
    }									\
  }


static vector<string> strsplit(string str,char delim=' ',char esc='"') {
  vector<string> ret;
  ostringstream oss;
  char inesc = 0;
  const char* cstr = str.c_str();
  const char* c = cstr;
  while (*c) {
    if (inesc) {
      while (*c &&
	     *c != esc) {
	oss << *c;
	c++;
      }
      // means we hit end/delim
      if (*c)
	c++;
      inesc = 0;
    }
    else {
      while (*c &&
	     *c != delim &&
	     *c != esc) {
	oss << *c;
	c++;
      }
      // now we're either at a delim/esc/endofstring
      if (*c) {
	if (*c == esc)
	  inesc = 1;
	c++;
      }
    }
    if (oss.str() != "") {
      ret.push_back(oss.str());
      oss.str("");
    }
  }
  return ret;
}

static 
void put(StorageClient &client,
	 const NameSpace &ns,
	 const Record &rec) {
  client.put(ns,rec);
}
	
static 
void get(StorageClient &client,
	 Record &r,
	 const NameSpace &ns,
	 const RecordKey &key) {
  client.get(r,ns,key);
}

static 
void remove(StorageClient &client,
	    const NameSpace &ns,
	    const RecordKey &key) {
  Record r;
  r.key = key;
  r.__isset.key = true;
  r.__isset.value = false;
  put(client,ns,r);
}

static void range(StorageClient &client,
		  vector<Record> &results,
		  const NameSpace &ns,
		  const RecordKey &start_key,
		  const RecordKey &end_key,
		  int32_t offset= 0,
		  int32_t limit = 0) {
  RecordSet rs;
  rs.type = RST_RANGE;
  rs.__isset.range = true;
  RangeSet range;
  range.__isset.start_key = true;
  range.__isset.end_key = true;
  range.offset = 0;
  range.limit = 0;
  range.start_key = start_key;
  range.end_key = end_key;
  rs.range = range;
  client.get_set(results,ns,rs);
}

static void ruby(StorageClient &client,
		 vector<Record> &results,
		 RecordSetType &rst,
		 const NameSpace &ns,
		 const string &func) {
  UserFunction uf;
  uf.lang = LANG_RUBY;
  uf.func = func;
  RecordSet rs;
  rs.type = rst; // Should be RST_KEY_FUNC or RST_KEY_VALUE_FUNC
  rs.func = uf;
  client.get_set(results,ns,rs);
}

static void removeRange(StorageClient &client,
			const NameSpace &ns,
			const RecordKey &start_key,
			const RecordKey &end_key) {
  RangeSet range;
  range.start_key = start_key;
  range.end_key = end_key;
  RecordSet rs;
  rs.type = RST_RANGE;
  rs.range = range;
  rs.range.offset = 0;
  rs.range.limit = 100;
  client.remove_set(ns,rs);
}

static void copyRange(StorageClient &client,
		      const Host &h,
		      const NameSpace &ns,
		      const RecordKey &start_key,
		      const RecordKey &end_key) {
  RecordSet rs;
  rs.type = RST_RANGE;
  rs.__isset.range = true;
  RangeSet range;
  range.__isset.start_key = true;
  range.__isset.end_key = true;
  range.offset = 0;
  range.limit = 0;
  range.start_key = start_key;
  range.end_key = end_key;
  rs.range = range;
  client.copy_set(ns,rs,h);
}
	
static void printPutUsage() {
  printf("invalid put, put is used as:\n");
  printf("put namespace key value\n");
}

static void printGetUsage() {
  printf("invalid get, get is used as:\n");
  printf("get namespace key\n");
}

static void printRangeUsage() {
  printf("invalid range, range is used as:\n");
  printf("range namespace start_key end_key [offset] [limit]\n");
}

static void printRemoveRangeUsage() {
  printf("invalid removeRange, removeRange is used as:\n");
  printf("removeRange namespace start_key end_key\n");
}

static void printRubyUsage() {
  cout <<"invalid ruby, ruby is used as:"<<endl<<
    " to pass a function that only gets applied to keys:"<<endl<<
    "  ruby key namespace \"ruby string\""<<endl<<
    " to pass a function that gets applied to keys and values:"<<endl<<
    "  ruby keyvalue namespace \"ruby string\""<<endl;
}


char histFile[128];

void ex_program(int sig) {
  if (histFile[0] != '\0');
  write_history(histFile);
  exit(0);
}

int main(int argc,char* argv[]) {
  string* line;
  int opt,timing = 0;
  int port = THRIFT_PORT;
  char pbuf[32];
  while ((opt = getopt(argc,argv, "tp:")) != -1) {
    switch (opt) {
    case 't':
      timing = 1;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    default:
      fprintf(stderr,"Usage: %s [-t] [-p port] [hostname]\n",argv[0]);
      exit(EXIT_FAILURE);
    }
  }
  const char* host = (optind>=argc)?THRIFT_HOST:argv[optind];
  //cout << "Connecting to: "<<host<<endl;
  if (timing)
    cout << "Will print timing info"<<endl;
  shared_ptr<TTransport> socket(new TSocket(host, port));
  shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  StorageClient client(protocol);

  signal(SIGINT, ex_program);
  using_history();

  char *hd = getenv("HOME");
  if (!hd || (strlen(hd) > 111)) {
    printf("No home dir or path too long.  Not saving/restoring history.\n");
    histFile[0] = '\0';
  } 
  else {
    snprintf(histFile,127,"%s/%s",hd,".storageclient-history");
    if (read_history(histFile)) 
      perror("Couldn't read history: ");
  }

  snprintf(pbuf,32,"%s:%i> ",host,port);
  pbuf[30]=' ';
  pbuf[29]='>';

  try {
    transport->open();
    
    for (;;) {
      char* l = readline(pbuf);
      if (l && *l)
	add_history(l);
      line = new string(l);
      free(l);
      vector<string> v = strsplit(*line);
      string cmd = v[0];

      if (cmd == "quit")
	break;

      else if (cmd == "help") {
	cout << "Avaliable commands (type any command with no args for usage of that command):"<<endl<<
	  " put\t\tput a record"<<endl<<
	  " get\t\tget a record"<<endl<<
	  " range\t\tget a sequential range of records between two keys"<<endl<<
	  " ruby\t\tget a set of records that match a ruby function"<<endl<<
	  " remote\t\t remove a key"<<endl<<
	  " removeRange\t\tremove a sequential range of records between two keys"<<endl<<
	  " quit\t\tquit the program"<<endl<<
	  " help\t\tthis help"<<endl;
	continue;
      }

      else if (cmd == "put") {
	if (v.size() != 4) {
	  printPutUsage();
	  continue;
	}
	//cout << "doing: put |"<<v[1]<<"| |"<<v[2]<<"| |"<<v[3]<<"|"<<endl;
	Record r;
	r.key = v[2];
	r.__isset.key = true;
	r.value = v[3];
	r.__isset.value = true;
	start_timing();
	put(client,v[1],r);
	cout << "Done"<<endl;
	end_timing();
	continue;
      }

      else if (cmd == "get") {
	if (v.size() != 3) {
	  printGetUsage();
	  continue;
	}
	try {
	  Record r;
	  start_timing();
	  get(client,r,v[1],v[2]);
	  end_timing();
	  if (r.__isset.value)
	    cout << r.value << endl;
	  else
	    cout << "No value for "<<v[2]<<endl;
 	} catch (TException e) {
	  cout << "[Exception]: "<<e.what()<<endl;
        }
      }

      else if (cmd == "range") {
	if (v.size() < 4) {
	  printRangeUsage();
	  continue;
	}
	try {
	  vector<Record> recs;
	  start_timing();
	  range(client,recs,v[1],v[2],v[3],
		v.size()>4?atoi(v[4].c_str()):0,
		v.size()>6?atoi(v[5].c_str()):0);
	  end_timing();
	  printf("returned: %i values\n\n",(int)(recs.size()));
	  if (recs.size() != 0) {
	    vector<Record>::iterator it;
	    it = recs.begin();
	    while(it != recs.end()) {
	      cout << "Key:\t"<<(*it).key<<endl;
	      cout << "Value:\t"<<(*it).value<<endl<<endl;
	      it++;
	    }
	  }
	} catch (TException e) {
	  cout << "[Exception]: "<<e.what()<<endl;
        }
      }
      else if (cmd == "remove") {
	if (v.size() != 3) {
	  cout << "Invalid remove"<<endl;
	  continue;
	}
	try {
	  start_timing();
	  remove(client,v[1],v[2]);
	  end_timing();
	} catch (TException e) {
	  cout << "[Exception]: "<<e.what()<<endl;
	}
      }

      else if (cmd == "ruby") {
	if (v.size() != 4) {
	  printRubyUsage();
	  continue;
	}
	try {
	  vector<Record> recs;
	  RecordSetType rst;
	  if (v[1] == "key")
	    rst = RST_KEY_FUNC;
	  else if (v[1] == "keyvalue")
	    rst = RST_KEY_VALUE_FUNC;
	  else {
	    printRubyUsage();
	    continue;
	  }
	  start_timing();
	  ruby(client,recs,rst,v[2],v[3]);
	  end_timing();
	  printf("returned: %i values\n\n",(int)(recs.size()));
	  if (recs.size() != 0) {
	    vector<Record>::iterator it;
	    it = recs.begin();
	    while(it != recs.end()) {
	      cout << "Key:\t"<<(*it).key<<endl;
	      cout << "Value:\t"<<(*it).value<<endl<<endl;
	      it++;
	    }
	  }
	} catch (TException e) {
	  cout << "[Exception]: "<<e.what()<<endl;
        }
      }

      else if (cmd == "removeRange") {
	if (v.size() != 4) {
	  printRemoveRangeUsage();
	  continue;
	}
	try {
	  start_timing();
	  removeRange(client,v[1],v[2],v[3]);
	  end_timing();
	  cout << "Done"<<endl;
	} catch (TException e) {
	  cout << "[Exception]: "<<e.what()<<endl;
        }
      }

      else if (cmd == "copy") {
	if (v.size() != 5) {
	  printf("invalid copy.  do: copy host:port namespace start_key end_key\n");
	  continue;
	}
	try {
	  start_timing();
	  copyRange(client,v[1],v[2],v[3],v[4]);
	  end_timing();
	  cout << "Done"<<endl;
	} catch(TException e) {
	  cout << "[Exception]: "<<e.what()<<endl;
	}
      }

      /*
      else if (cmd == "strsplit") { // debug command 
	cout << "tokens:"<<endl;
	vector<string>::iterator it;
	it = v.begin();
	while(it != v.end()) {
	  cout <<(*it)<<endl;
	  it++;
	}
      }
      */

      else {
	cout << "Invalid command: "<<(*line)<<endl;
	continue;
      }
    }

    transport->close();

  } catch (TException &tx) {
    printf("ERROR: %s\n", tx.what());
  }
  ex_program(2);
} 
