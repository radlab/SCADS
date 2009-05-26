#include <cerrno>
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <signal.h>
#include <sys/time.h>
#include "mhash.h"

#include "db.h"

#define dbt_string(dbt) std::string((char*)(dbt)->data,(dbt)->size)
#define print_hex(buf, len) for (int i = 0; i < len ; i++) { printf("%X%X", (0x0F & (((char *)buf)[i]) >> 4), (0x0F & (((char *)buf)[i])));}

using namespace std;

char * file;
char* env_dir;
int data_limit;

void usage(const char* prgm) {
  fprintf(stderr, "Usage: %s [-d DIRECTORY] [-f FILE] [-l DATA_LIMIT]\n\
Starts the BerkeleyDB storage layer.\n\n",
	  prgm);
}

void parseArgs(int argc, char* argv[]) {
  int opt;
  env_dir = 0;
	file = 0;
	data_limit = 10;

  while ((opt = getopt(argc, argv, "d:f:h")) != -1) {
    switch (opt) {
    case 'd':
      env_dir = (char*)malloc(sizeof(char)*(strlen(optarg)+1));
      strcpy(env_dir,optarg);
      break;
		case 'f':
			file = (char *)malloc(sizeof(char)*(strlen(optarg)+1));
			strcpy(file, optarg);
			break;
		case 'l':
			data_limit = atoi(optarg);
			break;
    case 'h':
    default: /* '?' */
      usage(argv[0]);
      exit(EXIT_FAILURE);
    }
  }

  if (!env_dir) {
    cerr << "Warning: -d not specified, running in local dir"<<endl;
    env_dir = (char*)malloc(strlen(".")+1);
    strcpy(env_dir,".");
  }
  if (!file) {
    cerr << "Warning: -f not specified, exiting"<<endl;
		exit(1);
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
	db->open(db, NULL, file, NULL, DB_BTREE, DB_CREATE, 0);

	DBC * cursorp;
  db->cursor(db, NULL, &cursorp, 0);
	DBT key, data;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.flags = DB_DBT_MALLOC;
	data.flags = DB_DBT_MALLOC;

	ret = cursorp->get(cursorp, &key, &data, DB_FIRST);
	while (ret == 0) {
		if (data.size > data_limit) {
			data.size = data_limit;
		}
		std::cout << "'" << dbt_string(&key) << "'" << "\t=>\t"; 
		print_hex(&data.data, data.size);
		std::cout << endl;
		free(key.data);
		free(data.data);
		ret = cursorp->get(cursorp, &key, &data, DB_NEXT);
	}
	cursorp->close(cursorp);

	db->close(db, 0);
  printf("done.\n");
  return 0;
}
