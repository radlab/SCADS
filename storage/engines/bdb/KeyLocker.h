#ifndef KEYLOCKER_H
#define KEYLOCKER_H

#include <pthread.h>

#define KEYLOCK_HASH_FUNC MHASH_TIGER128

using namespace std;


namespace SCADS {

class KeyLocker {
private:
	unsigned int numLocks;
	pthread_rwlock_t* locks;
	unsigned int keyBucket(const char* ,int);

public:	
	KeyLocker(unsigned int);

public:
	bool readLockKey(const char*,int);
	bool writeLockKey(const char*,int);
	bool unlockKey(const char*,int);
};

}

#endif //KEYLOCKER_H
