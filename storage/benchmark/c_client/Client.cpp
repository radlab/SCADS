#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
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

#define start_timer() gettimeofday(&start_time,NULL)
#define end_timer() { \
    gettimeofday(&end_time,NULL); \
    timersub(&end_time,&start_time,&diff_time); \
    printf("%ld.%.6ld\t%ld.%.6ld", start_time.tv_sec, start_time.tv_usec, diff_time.tv_sec, diff_time.tv_usec); \
  }

int main(int argc, char** argv) {
	char *host;
	int port;
	int c;
	
	//defaults
	host = "localhost";
	port = 9090;
	
	while ((c = getopt(argc, argv, "h:p:")) != -1) {
		switch (c) {
			case 'h':
				host = optarg;
				break;
			case 'p':
				port = atoi(optarg);
				break;
			case '?':
				if (optopt == 'h') {
					fprintf (stderr, "Option -%c requires an argument.\n", optopt);
				} else if (isprint (optopt)) {
					fprintf (stderr, "Unknown option `-%c`.\n", optopt);
				} else {
					fprintf (stderr, "Unknown option character `\\x%x`.\n", optopt);
				}
				return 1;
			default:
				abort();
		}
	}
	
	shared_ptr<TTransport> socket(new TSocket(host, port));
	shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	StorageClient client(protocol);

	//TODO: Remove magic numbers (!including in scanf length limiters!)
	char cmd[16]; //get, put
	char table[512];
	char key[512];
	char value[4096];
	int success;
	Record r;
	struct timeval start_time, end_time, diff_time;
	
	transport->open();
	while (EOF != scanf("%15s", cmd)) {
		success = -1;
		if (!strcmp(cmd,"get")) {
			scanf("%511s %511s", table, key);
			start_timer();
			client.get(r, table, key);
			end_timer();
			printf("\tget\t%s\t%s\t=>\t", table, key);
			cout << r.value << "\n";
		} else if (!strcmp(cmd,"put")) {
			scanf("%511s %511s %4095s", table, key, value);
			r.key = key;
			r.value = value;
			r.__isset.value = true;	//TODO: Fix cargo-cult
			start_timer();
			success = client.put(table, r);
			end_timer();
			printf("\tput\t%s\t%s\t%s\t=>\t", table, key, value);
			cout << success << "\n";
		} else if (!strcmp(cmd, "quit") or !strcmp(cmd, "exit")) {
			transport->close();
			exit(0);
	  } else {
			printf("Error: Unrecognized cmd %s\n", cmd);
			exit(1);
		}
	}
}
