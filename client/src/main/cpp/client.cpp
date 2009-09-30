
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

#include "gen-cpp/StorageEngine.h"

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
#define end_timing() {																									\
    if (timing) {																												\
      gettimeofday(&cur_time,NULL);																			\
      timersub(&cur_time,&start_time,&diff_time);												\
      cout << "Took "<<diff_time.tv_sec <<" seconds, and " << diff_time.tv_usec<<" microseconds."<<endl; \
    }																																		\
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
void put(StorageEngineClient &client,
				 const NameSpace &ns,
				 const Record &rec) {
  client.put(ns,rec);
}

static
void test_and_set(StorageEngineClient &client,
									const NameSpace &ns,
									const Record &rec,
									const ExistingValue& eVal) {
	client.test_and_set(ns,rec,eVal);
}

static void putRange(StorageEngineClient &client,
										 const NameSpace &ns,
										 int s, int e) {
  Record r;
  r.__isset.key = true;
  r.__isset.value = true;
  ostringstream oss;
  while(s <= e) {
    oss.str("");
    oss.width(5);
    oss.fill('0');
    oss << s++;
    r.key.assign(oss.str());
    r.value.assign(oss.str());
    put(client,ns,r);
  }
}

static void putRandom(StorageEngineClient &client,
											const NameSpace &ns,
											int s, int e, int c) {
  Record r;
  r.__isset.key = true;
  r.__isset.value = true;
  ostringstream oss;
  int rng = e-s;
  for(int i = 0;i < c;i++) {
    int v = (rand()%rng)+s;
    oss.str("");
    oss.width(5);
    oss.fill('0');
    oss << v;
    r.key.assign(oss.str());
    r.value.assign(oss.str());
    put(client,ns,r);
  }
}

static
void get(StorageEngineClient &client,
				 Record &r,
				 const NameSpace &ns,
				 const RecordKey &key) {
  client.get(r,ns,key);
}

static
void getall(StorageEngineClient &client,
						vector<Record> &results,
						const NameSpace &ns) {
	RecordSet rs;
	rs.type = RST_ALL;
	rs.__isset.range = false;
	rs.__isset.func = false;
	client.get_set(results,ns,rs);
}

static
void remove(StorageEngineClient &client,
						const NameSpace &ns,
						const RecordKey &key) {
  Record r;
  r.key = key;
  r.__isset.key = true;
  r.__isset.value = false;
  put(client,ns,r);
}

static void range(StorageEngineClient &client,
									vector<Record> &results,
									const NameSpace &ns,
									const RecordKey &start_key,
									const RecordKey &end_key,
									int32_t offset= 0,
									int32_t limit = 0) {
  RecordSet rs;
  rs.type = RST_RANGE;
  RangeSet range;
  range.start_key = start_key;
  range.__isset.start_key = true;
  range.end_key = end_key;
  range.__isset.end_key = true;
  range.offset = offset;
  range.__isset.offset = (offset != 0);
  range.limit = limit;
  range.__isset.limit = (limit != 0);
  rs.range = range;
  rs.__isset.range = true;
  client.get_set(results,ns,rs);
}

static void filter(StorageEngineClient &client,
									 vector<Record> &results,
									 const NameSpace &ns,
									 const string filter) {
  RecordSet rs;
  rs.type = RST_FILTER;
  rs.__isset.filter = true;
  rs.filter = filter;
  client.get_set(results,ns,rs);
}

static int32_t count(StorageEngineClient &client,
										 const NameSpace &ns,
										 const RecordKey *start_key,
										 const RecordKey *end_key,
										 int32_t offset= 0,
										 int32_t limit = 0) {
  RecordSet rs;
  if (start_key != NULL) {
    rs.type = RST_RANGE;
    rs.__isset.range = true;
    RangeSet range;
    range.__isset.start_key = true;
    range.__isset.end_key = true;
    range.offset = 0;
    range.limit = 0;
    range.start_key = *start_key;
    range.end_key = *end_key;
    rs.range = range;
  } else {
    rs.type = RST_ALL;
  }
  return client.count_set(ns,rs);
}

static void ruby(StorageEngineClient &client,
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
  rs.__isset.range = false;
  rs.__isset.func = true;
  client.get_set(results,ns,rs);
}

static void removeRange(StorageEngineClient &client,
												const NameSpace &ns,
												const RecordKey &start_key,
												const RecordKey &end_key) {
  RangeSet range;
  range.start_key = start_key;
	range.__isset.start_key = true;
  range.end_key = end_key;
	range.__isset.end_key = true;
  RecordSet rs;
  rs.type = RST_RANGE;
  rs.range = range;
	rs.__isset.range = true;
  client.remove_set(ns,rs);
}

static void copyRange(StorageEngineClient &client,
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

static void syncRangeGreater(StorageEngineClient &client,
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
  ConflictPolicy pol;
  pol.type = CPT_GREATER;
  client.sync_set(ns,rs,h,pol);
}

/*
static void instResp(StorageEngineClient &client) {
  RecordSet rs;
  rs.type = RST_RANGE;
  rs.__isset.range = true;
  RangeSet range;
  range.__isset.start_key = true;
  range.__isset.end_key = false;
  range.__isset.offset = false;
  range.__isset.limit = false;
  range.start_key = "0";
  rs.range = range;
  client.set_responsibility_policy("foo",rs);
}
*/

static void setRespPol(StorageEngineClient &client,
											 const NameSpace &ns,
											 const vector<string>& vec) {
	vector<RecordSet> rsvec;
	vector<string>::const_iterator it;
	for (it = vec.begin();it != vec.end();++it) {
		RecordSet rs;
		rs.type = RST_RANGE;
		rs.__isset.range = true;
		RangeSet range;
		range.__isset.start_key = true;
		range.__isset.end_key = true;
		range.__isset.offset = false;
		range.__isset.limit = false;
		range.start_key = *it;
		++it;
		range.end_key = *it;
		rs.range = range;
		rsvec.push_back(rs);
	}
	client.set_responsibility_policy(ns,rsvec);
}

static void printPutUsage() {
  printf("invalid put, put is used as:\n");
  printf("put namespace key value\n");
}

static void printTaSUsage() {
  printf("invalid testandset, testandset is used as:\n");
  printf("testandset namespace key new_value [old_value] [prefix_size]\n");
  printf("(if no old_value is specified the test is that no value currently exists at key)\n");
}

static void printGetUsage() {
  printf("invalid get, get is used as:\n");
  printf("get namespace key\n");
}

static void printFilterUsage() {
  printf("invalid filter, filter is used as:\n");
  printf("filter namespace pattern\n");
}

static void printRangeUsage() {
  printf("invalid range, range is used as:\n");
  printf("range namespace start_key end_key [offset] [limit]\n");
}

static void printCountUsage() {
  printf("invalid count, count is used as:\n");
  printf("count namespace start_key end_key [offset] [limit]\n");
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
shared_ptr<TTransport> gtsp;

void ex_program(int sig) {
  if (histFile[0] != '\0');
  write_history(histFile);
	gtsp->close();
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

  unsigned int s;
  FILE* f = fopen("/dev/urandom","r");
  fread(&s,sizeof(unsigned int),1,f);
  fclose(f);
  srand(s);


  const char* host = (optind>=argc)?THRIFT_HOST:argv[optind];
  //cout << "Connecting to: "<<host<<endl;
  if (timing)
    cout << "Will print timing info"<<endl;
  shared_ptr<TTransport> socket(new TSocket(host, port));
  shared_ptr<TTransport> transport(new TFramedTransport(socket));
  //shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  StorageEngineClient client(protocol);

	gtsp = transport;
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
      else
				continue;
      line = new string(l);
      free(l);
      vector<string> v = strsplit(*line);
      string cmd = v[0];

      if (cmd == "quit" || cmd == "exit")
				break;

      else if (cmd == "help") {
				cout << "Avaliable commands (type any command with no args for usage of that command):"<<endl<<
					" put\t\tput a record"<<endl<<
					" putrange\t\tput a range of keys"<<endl<<
					" testandset\tput a record conditionally"<<endl<<
					" get\t\tget a record"<<endl<<
					" getall\t\tget all records in a particular namespace"<<endl<<
					" range\t\tget a sequential range of records between two keys"<<endl<<
					" ruby\t\tget a set of records that match a ruby function"<<endl<<
					" remote\t\tremove a key"<<endl<<
					" removeRange\tremove a sequential range of records between two keys"<<endl<<
					" copy\t\tcopy a set of data from one node to another"<<endl<<
					" sync\t\tsync a set of data between two nodes"<<endl<<
					" quit/exit\tquit the program"<<endl<<
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

      else if (cmd == "testandset") {
				if (v.size() < 4 ||
						v.size() > 6) {
					printTaSUsage();
					continue;
				}
				//cout << "doing: put |"<<v[1]<<"| |"<<v[2]<<"| |"<<v[3]<<"|"<<endl;
				Record r;
				r.key = v[2];
				r.__isset.key = true;
				r.value = v[3];
				r.__isset.value = true;
				ExistingValue ev;
				if (v.size() >= 5) {
					ev.value = v[4];
					ev.__isset.value = true;
				} else
					ev.__isset.value = false;
				if (v.size() == 6) {
					ev.prefix = atoi(v[5].c_str());
					ev.__isset.prefix = true;
				} else
					ev.__isset.prefix = false;
				start_timing();
				try {
					test_and_set(client,v[1],r,ev);
				} catch(TestAndSetFailure tsf) {
					if (tsf.__isset.currentValue)
						cout << "Test and set failed. Current value: "<<tsf.currentValue<<endl;
					else
						cout << "Test and set failed. Key does not currently exist"<<endl;
				}
				end_timing();
				continue;
      }

      else if (cmd == "putrange") {
				if (v.size() != 4) {
					cout << "Invalid putrange, use as: putrange namespace startkey endkey"<<endl;
					continue;
				}
				int s = atoi(v[2].c_str());
				int e = atoi(v[3].c_str());
				start_timing();
				putRange(client,v[1],s,e);
				end_timing();
				continue;
      }

      else if (cmd == "putrand") {
				if (v.size() != 5) {
					cout << "Invalid putrand, use as: putrand namespace startkey endkey count"<<endl;
					continue;
				}
				int s = atoi(v[2].c_str());
				int e = atoi(v[3].c_str());
				int c = atoi(v[4].c_str());
				start_timing();
				putRandom(client,v[1],s,e,c);
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

      else if (cmd == "getall") {
				if (v.size() != 2) {
					cout << "Invalid getall, getall is used as: getall namespace"<<endl;
					continue;
				}
				try {
					vector<Record> recs;
					start_timing();
					getall(client,recs,v[1]);
					end_timing();
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
								v.size()>5?atoi(v[5].c_str()):0);
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

      else if (cmd == "filter") {
				if (v.size() != 3) {
					printFilterUsage();
					continue;
				}
				try {
					vector<Record> recs;
					start_timing();
					filter(client,recs,v[1],v[2]);
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
					end_timing();
				} catch (TException e) {
					cout << "[Exception]: "<<e.what()<<endl;
        }
      }

      else if (cmd == "count") {
				if (v.size() < 2) {
					printCountUsage();
					continue;
				}
				try {
					int32_t c;
					start_timing();
					c = count(client,v[1],
										v.size()<3?NULL:&v[2],
										v.size()<4?NULL:&v[3],
										v.size()>4?atoi(v[4].c_str()):0,
										v.size()>6?atoi(v[5].c_str()):0);
					end_timing();
					cout << c << endl;
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

      else if (cmd == "sync") {
				if (v.size() != 6) {
					printf("invalid sync.  do: sync host:port namespace start_key end_key greater/func [func]\n");
					continue;
				}
				try {
					if (v[5] == "greater") {
						start_timing();
						syncRangeGreater(client,v[1],v[2],v[3],v[4]);
						end_timing();
						cout << "Done"<<endl;
					} else {
						cout << "Only support greater for the moment"<<endl;
					}
				} catch(TException e) {
					cout << "[Exception]: "<<e.what()<<endl;
				}
      }
			
			/*
			else if (cmd == "resp") {
				instResp(client);
				continue;
			}
			*/
			else if (cmd == "setRespPol") {
				if (v.size() <= 2) {
					printf("usage: setRespPol namespace start_key1 end_key1 start_key2 end_key2...\n");
					continue;
				}
				if (v.size()%2 != 0) {
					printf("Start keys must all match up with end keys\n");
					continue;
				}
				vector<string> vec;
				vector<string>::iterator it;
				it = v.begin();
				++it; ++it; // skip command and ns
				for (;it != v.end();++it) 
					vec.push_back(*it);
				setRespPol(client,v[1],vec);
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
