#include "KeyLocker.h"

#include <cerrno>
#include <iostream>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

using namespace std;
using namespace SCADS;

inline void chkLock(int rc, const char* key, const string action) {
  switch (rc) {
  case 0: // success
    return;
  case EINVAL:
    cerr << "Couldn't lock "<<key<< " for "<<action<<":\n\t"<<
      " The value specified by rwlock does not refer to  an  initialized read-write lock object."<<endl;
    exit(EXIT_FAILURE);
  case EAGAIN:
    cerr << "Couldn't lock "<<key<< " for "<<action<<":\n\t"<<
      "The  read  lock could not be acquired because the maximum number of read locks for rwlock has been exceeded."<<endl;
    exit(EXIT_FAILURE);
  case EDEADLK:
    cerr << "Couldn't lock "<<key<< " for "<<action<<":\n\t"<<
      "The current thread already owns the read-write lock for writing or reading."<<endl;
  case EPERM:
    cerr << "WARNING: tried to unlock "<<key<< " for "<<action<<" but the lock is not held."<<endl;
    return; // hopefully this isn't the end of the world
  default:
    cerr << "An unknown error in locking: "<<key<<" for "<<action<<endl;
    exit(EXIT_FAILURE);
  }
}


KeyLocker::
KeyLocker(unsigned int _numLocks) :
	numLocks(_numLocks)
{
	unsigned int i;
	locks = (pthread_rwlock_t*)malloc(numLocks*sizeof(pthread_rwlock_t));
	for(i = 0;i < numLocks;i++)
		if (pthread_rwlock_init(locks+i, NULL)) {
			perror("Could not create a key lock");
			exit(EXIT_FAILURE);
		}
}

/* Hash using the djb2 algorithm, first reported by
 * Dan Bernstein many years ago in comp.lang.c */
unsigned int KeyLocker::
keyBucket(const char* key, int len) {
	unsigned long hash = 5381;
	int c,i=0;
	while(i<len) {
		c = key[i++];
		hash = ((hash << 5) + hash) + c; // hash*33 + c
	}
	return (hash%numLocks);
}

bool KeyLocker::
readLockKey(const char* key, int len) {
	int bucket = keyBucket(key,len);
#ifdef DEBUG
	cout << "Read locking "<<string(key,len)<<" bucket: "<<bucket<<endl;
#endif
  int rc = pthread_rwlock_rdlock(locks+bucket);
	chkLock(rc,key,"reading");
	return (rc==0);
}

bool KeyLocker::
writeLockKey(const char* key, int len) {
	int bucket = keyBucket(key,len);
#ifdef DEBUG
	cout << "Write locking "<<string(key,len)<<" bucket: "<<bucket<<endl;
#endif
  int rc = pthread_rwlock_wrlock(locks+bucket);
	chkLock(rc,key,"writing");
	return (rc==0);
}

bool KeyLocker::
unlockKey(const char* key, int len) {
	int bucket = keyBucket(key,len);
#ifdef DEBUG
	cout << "Unlocking "<<string(key,len)<<" bucket: "<<bucket<<endl;
#endif
  int rc = pthread_rwlock_unlock(locks+bucket);
	chkLock(rc,key,"unlocking");
	return (rc==0);
}
