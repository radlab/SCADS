#include <cerrno>
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <signal.h>
#include <sys/time.h>
#include "mhash.h"

#include "MerkleDB.h"

#define print_hex(buf, len) for (int i = 0; i < len ; i++) { printf("%X%X", (0x0F & (((char *)buf)[i]) >> 4), (0x0F & (((char *)buf)[i])));}
#define start_timer() gettimeofday(&start_time,NULL)
#define end_timer() { \
    gettimeofday(&end_time,NULL); \
    timersub(&end_time,&start_time,&diff_time); \
    printf("%ld.%.6ld\n", diff_time.tv_sec, diff_time.tv_usec); \
  }
//printf("%ld.%.6ld\t%ld.%.6ld\n", start_time.tv_sec, start_time.tv_usec, diff_time.tv_sec, diff_time.tv_usec);
using namespace std;
using namespace SCADS;

struct timeval start_time, end_time, diff_time;

MerkleDB * mdb;
unsigned int rows;
unsigned int max_keylength;
char * keybuf;
unsigned int data_size;
char * databuf;
int flush_period;
unsigned int unflushed;
char* env_dir;
char* ns;

void usage(const char* prgm) {
  fprintf(stderr, "Usage: %s [-r ROWS] [-d DIRECTORY] [-n NAMESPACE] [-k MAX_KEYLENGTH] [-f FLUSH_PERIOD] [-s DATA_SIZE]\n\
Starts the BerkeleyDB storage layer.\n\n\
	-n NAMESPACE\tNamespace to put merkle db in.\n\
	-r ROWS\tNumber of rows to insert\n\
	-k MAX_KEYLENGTH\tMaximum length of key\n\
	-s DATA_SIZE\tSize of each row's data item\n\
	-f FLUSH_PERIOD\tNumber of updates to queue before calling flushp\n\
								 \t(-1 signifies never flush)\n\
  -d DIRECTORY\tStore data files in directory DIRECTORY\n\
              \tDefault: .\n",
	  prgm);
}

void ex_program(int sig) {
  cout << "\n\nShutting down."<<endl;
	mdb->close();
  exit(0);
}

int chkdir(const char* d) {
  struct stat buffer;
  if (stat(d,&buffer)) {
    switch (errno) {
    case ENOENT: {
      cout<<d<<" doesn't exist, attempting to create"<<endl;
      if (mkdir(d,S_IRWXU | S_IRGRP |  S_IXGRP)) {
	perror("Could not create dir");
	return -1;
      }
      stat(d,&buffer);
      break;
    }
    default: {
      perror("Couldn't stat");
      return -1;
    }
    }
  }
  if (!S_ISDIR(buffer.st_mode)) {
    cout <<d<<" is not a directory"<<endl;
    return -1;
  }
  return 0;
}

int chkdirs(const char* d) {
  int ret;
  ret = chkdir(d);
  if (ret)
    return ret;
  ostringstream oss;
  oss <<d<<"/merkle";
  return chkdir(oss.str().c_str());
}

void parseArgs(int argc, char* argv[]) {
  int opt;
	rows = 10;
	max_keylength = 20;
	data_size = 1000;
	flush_period = -1;
	unflushed = 0;
  env_dir = 0;

  while ((opt = getopt(argc, argv, "r:d:k:s:f:h")) != -1) {
    switch (opt) {
    case 'r':
      rows = atoi(optarg);
      break;
    case 'd':
      env_dir = (char*)malloc(sizeof(char)*(strlen(optarg)+1));
      strcpy(env_dir,optarg);
      break;
    case 'k':
			max_keylength = atoi(optarg);
      break;
    case 's':
			data_size = atoi(optarg);

      break;
    case 'f':
      flush_period = atoi(optarg);
      break;
		case 'n':
			ns = (char *)malloc(sizeof(char)*(strlen(optarg)+1));
			strcpy(ns, optarg);
			break;
    case 'h':
    default: /* '?' */
      usage(argv[0]);
      exit(EXIT_FAILURE);
    }
  }

	keybuf = (char *)malloc(sizeof(char)*max_keylength+1);
	databuf = (char *)malloc(sizeof(char)*data_size);

  if (!env_dir) {
    cerr << "Warning: -d not specified, running in local dir"<<endl;
    env_dir = (char*)malloc(strlen(".")+1);
    strcpy(env_dir,".");
  }
	if (!ns) {
		cerr << "Warning: -n not specified, running in namespace \"default\""<<endl;
		ns = (char *)malloc(strlen("default")+1);
		strcpy(ns, "default");
	}
}

int bench_hash() {
	int keylen;
	MHASH td;
	srand(13424);
		start_timer();

  td = mhash_init(MERKLEDB_HASH_FUNC);
  if (td == MHASH_FAILED) {
    std::cerr << "HASH Failed";
    return -1;
  }
	for (int i = 0; i < rows; i++) {
		keylen = (rand() % max_keylength) + 1;
		for (int j = 0; j < keylen; j++) {
//			keybuf[j] = ((char) (rand() % ('z' - '0' + 1) + '0'));
			keybuf[j] = (rand() % 94) + 33; //printable
//			keybuf[j] = (rand() % 255) + 1; //unprintable (but not null)
		}
		keybuf[keylen] = '\0';

//		printf("going to enqueue key:(\"%s\"\t\t\t", keybuf);
//		print_hex(keybuf, key.size);
//		std::cout << "\t:\t" << key.size << ")" << endl;
		mhash(td, keybuf, keylen);
	}

	mhash_end(td);
	end_timer();
}

int run() {
	int keylen;
	DBT key, data;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.flags = DB_DBT_MALLOC;	//foh
	data.flags = DB_DBT_MALLOC; //foh
	key.data = keybuf;
	data.data = &databuf;
	data.size = data_size;
	srand(13424);
	start_timer();
	for (int i = 0; i < rows; i++) {
		keylen = (rand() % max_keylength) + 1;
		for (int j = 0; j < keylen; j++) {
//			keybuf[j] = ((char) (rand() % ('z' - '0' + 1) + '0'));
			keybuf[j] = (rand() % 94) + 33; //printable
//			keybuf[j] = (rand() % 255) + 1; //unprintable (but not null)
		}
		keybuf[keylen] = '\0';
		key.size = keylen;

//		printf("going to enqueue key:(\"%s\"\t\t\t", keybuf);
//		print_hex(keybuf, key.size);
//		std::cout << "\t:\t" << key.size << ")" << endl;
		mdb->enqueue(&key, &data);
		if ((flush_period > 0) and ((i % flush_period) == 0)) {
			mdb->flushp();
		}
	}
	end_timer();
	start_timer();
	mdb->flushp();
	end_timer();
	std::cout << endl << "Printing tree"<<endl;
	//mdb->print_tree();
}

int main(int argc, char **argv) {
  char buf[] = "abcdef";
  getc(stdin);
  parseArgs(argc,argv);
  if(chkdirs(env_dir)) {
    exit(-1);
  }

#ifdef DEBUG
  cout << "Running in debug mode"<<endl;
#endif

  u_int32_t env_flags = 0;
  int ret;
  u_int32_t gb;
  DB_ENV* db_env;

  ret = db_env_create(&db_env, 0);
  if (ret != 0) {
    fprintf(stderr, "Error creating env handle: %s\n", db_strerror(ret));
    exit(-1);
  }

  env_flags =
    DB_CREATE |     /* If the environment does not exist, create it. */
    DB_INIT_LOCK |  /* Multiple threads might write */
    DB_INIT_MPOOL|  /* Initialize the in-memory cache. */
    DB_THREAD |
    DB_PRIVATE;

  ret = db_env->set_lk_detect(db_env,DB_LOCK_DEFAULT);

  ret = db_env->open(db_env,      /* DB_ENV ptr */
		     env_dir,    /* env home directory */
		     env_flags,  /* Open flags */
		     0);         /* File mode (default) */
  if (ret != 0) {
    fprintf(stderr, "Environment open failed: %s\n", db_strerror(ret));
    exit(-1);
  }

	mdb = new MerkleDB(ns, db_env);
  signal(SIGINT, ex_program);
  signal(SIGTERM, ex_program);

	std::cout << "Hashing alone" <<endl;
	bench_hash();

	std::cout << "Building Tree" <<endl;
	run();



  printf("done.\n");
  return 0;
}
