#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "../gen-cpp/Storage.h"

#include <iostream>

using namespace std;
using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;

using namespace SCADS;

using namespace boost;

int main(int argc, char** argv) {
  shared_ptr<TTransport> socket(new TSocket("localhost", 9090));
  shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  StorageClient client(protocol);
  
  int op = -1; // 0 = get, 1 = put, 2 = getall, 3 = getrange

  if (argc < 2) {
    fprintf(stderr,"Too few args.  Need [get|getall|put] [key] [value|startkey] [endkey]\n");
    exit(1);
  }

  if (!strcmp(argv[1],"get"))
    op = 0;
  if (!strcmp(argv[1],"put"))
    op = 1;
  if (!strcmp(argv[1],"getall"))
    op = 2;
  if (!strcmp(argv[1],"getrange"))
    op = 3;

  if (op == -1) {
    fprintf(stderr,"Invalid op: %s\n",argv[1]);
    exit(1);
  }

  if (op == 1 && argc < 4) {
    fprintf(stderr,"Put needs arg to put\n");
    exit(1);
  }

  if (op == 3 && argc < 4) {
    fprintf(stderr,"getrange needs start and end keys\n");
    exit(1);
  }

  Record r;
  if (op == 1) {
    r.key = string(argv[2]);
    r.value = string(argv[3]);
  }

  try {
    transport->open();

    switch (op) {
    case 0:
      client.get(r,"my_NS",string(argv[2]));
      cout << "Key:\t"<<argv[2]<<"\nValue:\t"<<r.value<<endl;
      break;
    case 1:
      if (client.put("my_NS",r))
	cout << "Put okay"<<endl;
      else
	cout << "Put failed"<<endl;
      break;
    case 2: {
      std::vector<Record> vals;
      RecordSet rs;
      rs.type = ALL;
      client.get_set(vals,"my_NS",rs);
      std::vector<Record>::iterator it;
      for (it = vals.begin();it!=vals.end();it++) 
	cout << "\nKey:\t"<<((*it).key)<<"\nValue:\t"<<((*it).value)<<endl;	
    }
      break;
    case 3: {
      std::vector<Record> vals;
      RecordSet rs;
      rs.type = RANGE;
      RangeSet range;
      range.offset = 0;
      range.limit = 10;
      range.start_key = string(argv[2]);
      range.end_key = string(argv[3]);
      rs.range = range;
      client.get_set(vals,"my_NS",rs);
      std::vector<Record>::iterator it;
      for (it = vals.begin();it!=vals.end();it++) 
	cout << "\nKey:\t"<<((*it).key)<<"\nValue:\t"<<((*it).value)<<endl;	
    }
      break;
    default:
      cout << "Nothing to do"<<endl;
    }


    transport->close();
  } catch (TException &tx) {
    printf("ERROR: %s\n", tx.what());
  }

}
