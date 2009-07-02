#include <cerrno>
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <signal.h>
#include <sys/time.h>
#include "mhash.h"

#include "MerkleDB.h"

using namespace std;
using namespace SCADS;

unsigned int rows;
unsigned int rows2;
char * directory;
char * output_file;
unsigned int max_keylength;
unsigned int data_size;

char * keybuf;
char * databuf;
char * env_dir;
char nspace[] = "default";

void usage(const char* prgm) {
  fprintf(stderr, "Usage: %s [-r ROWS] [-q ROWS_SEED2] [-d DIRECTORY] [-f OUTPUT_FILE] [-k MAX_KEYLENGTH] [-d DATA_SIZE]\n\
Starts the BerkeleyDB storage layer.\n\n\
	-r ROWS\tNumber of rows to insert\n\
	-s Of above row, how many to insert with data-seed 2\n\
	-k MAX_KEYLENGTH\tMaximum length of key\n\
	-s DATA_SIZE\tSize of each row's data item\n\
	-f FLUSH_PERIOD\tNumber of updates to queue before calling flushp\n\
								 \t(-1 signifies never flush)\n",
	  prgm);
}



void parseArgs(int argc, char* argv[]) {
  int opt;
	rows = 4000000;
	max_keylength = 20;
	data_size = 256;
  env_dir = 0;
	output_file = 0;

  while ((opt = getopt(argc, argv, "r:q:d:f:k:s:h")) != -1) {
    switch (opt) {
    case 'r':
      rows = atoi(optarg);
      break;
		case 'q':
			rows2 = atoi(optarg);
			break;
    case 'd':
      env_dir = (char*)malloc(sizeof(char)*(strlen(optarg)+1));
      strcpy(env_dir,optarg);
      break;
		case 'f':
			output_file = (char *)malloc(sizeof(char)*(strlen(optarg)+1));
			strcpy(output_file, optarg);
			break;
    case 'k':
			max_keylength = atoi(optarg);
      break;
    case 's':
			data_size = atoi(optarg);
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
  if (!output_file) {
    cerr << "Warning: -f not specified, exiting"<<endl;
		exit(1);
  }
}

void build_random_db(DB * db, MerkleDB * mdb, int rows, int key_offset, int key_seed, int data_seed) {
	char keybuf[128];
	DBT key;
	memset(&key, 0, sizeof(DBT));
	key.flags = DB_DBT_MALLOC;
	key.data = keybuf;

	char databuf[2048];
	DBT data;
	memset(&data, 0, sizeof(DBT));
	data.flags = DB_DBT_MALLOC;
	data.data = databuf;

	//Make data
	srand(data_seed);
	int datalen = data_size;
	//std::std::cout << "data:" << datalen << std::endl;
	//change 10 chars to make data different
	int pos;
	for (int k = 0; k < datalen; k++) {
		databuf[k] = ((char) ((rand() % 24) + 65));
	}
	databuf[datalen] = '\0';
	data.size = datalen;

	int keylen;
	int flushp = 10000;
	for (int i = 0; i < rows; i++) {
		//Make key
		srand(key_seed+i+key_offset);
		keylen = (rand() % 64) + 1;
		//std::std::cout << "key:" << keylen << ",";
		for (int j = 0; j < keylen; j++) {
			keybuf[j] = ((char) ((rand() % 24) + 65));
		}
		keybuf[keylen] = '\0';
		key.size = keylen;

		//Mix up the data just a little bit
		srand(data_seed + i);
		for (int k = 0; k < 5; k++) {
			databuf[(rand() % datalen)] = ((char) ((rand() % 24) + 65));
		}
		db->put(db, NULL, &key, &data, 0);
		mdb->enqueue(&key, &data);
		if (flushp <= 0) {
			flushp--;
			mdb->flushp();
		}
	}
}

int main(int argc, char **argv) {
  parseArgs(argc,argv);

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

	ret = db_env->set_cachesize(db_env, 1, 0, 0);
  if (ret != 0) {
   std::cerr << "Could not set cache size"<<std::endl;
    exit(-1);
  }

  ret = db_env->open(db_env,      /* DB_ENV ptr */
		     ".",    /* env home directory */
		     env_flags,  /* Open flags */
		     0);         /* File mode (default) */
  if (ret != 0) {
    fprintf(stderr, "Environment open failed: %s\n", db_strerror(ret));
    exit(-1);
  }

	DB * db;
	db_create(&db, db_env, 0);
	db->open(db, NULL, output_file, NULL, DB_BTREE, DB_CREATE, 0);

	MerkleDB * mdb;
	mdb = new MerkleDB(nspace, db_env);

	int keyseed = 123321;
	int dataseed1 = 1231233;
	int dataseed2 = 682283;

	std::cout << "Build random tree with " << rows << " rows with dataseed " << dataseed1 << endl;
	build_random_db(db, mdb, rows, 0, keyseed, dataseed1);
	mdb->flushp();

	std::cout << "Build random tree with " << rows2 << " rows with dataseed " << dataseed2 << endl;
	build_random_db(db, mdb, rows2, rows, keyseed, dataseed2);
	mdb->flushp();

	db->close(db, 0);
	mdb->close();
  printf("done.\n");
  return 0;
}
