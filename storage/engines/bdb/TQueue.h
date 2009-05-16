#ifndef TQUEUE_H
#define TQUEUE_H

#include <sys/time.h>
#include <time.h>
#include <iostream>

#include "pthread.h"

using namespace std;


template <class T>
class TQueue {

private:
  T* qptr;
  T* qend;
  T* read_ptr;
  T* write_ptr;

  pthread_cond_t dq_cond,eq_cond;
  pthread_mutex_t dq_tex,eq_tex;

public:
  TQueue(int s = 256) {
    int size = (s > 10 && s < 2048)?s:256;

    if (pthread_mutex_init(&dq_tex,NULL))
      perror("Could not create dq_tex");
    if (pthread_cond_init(&dq_cond,NULL)) 
      perror("Could not create dq_cond");
    if (pthread_mutex_init(&eq_tex,NULL))
      perror("Could not create eq_tex");
    if (pthread_cond_init(&eq_cond,NULL)) 
      perror("Could not create eq_cond");

    qptr = new T[size];
    qend = qptr+size-1;
    write_ptr = qptr;
    read_ptr = qptr;
  }

  ~TQueue() { 
    delete [] qptr; 
  }
  
  bool enqueue(const T& te) {
    struct timeval t;
    T* next = 
      (write_ptr == qend)?
      qptr:
      write_ptr+1;
    while (next==read_ptr) { // queue is full
#ifdef DEBUG
      cout << "queue is full, waiting"<<endl;
#endif
      gettimeofday(&t,NULL);
      t.tv_usec+=50000;
      (void)pthread_mutex_lock(&eq_tex);
      int ret = pthread_cond_timedwait(&eq_cond, &eq_tex,(const timespec*)(&t));
      (void)pthread_mutex_unlock(&eq_tex);
    }

    *write_ptr = te;
    
    write_ptr = next;
    //(void)pthread_mutex_lock(&dq_tex);
    int ret = pthread_cond_signal(&dq_cond);
    //(void)pthread_mutex_unlock(&dq_tex);
  }

  T& dequeue() {
    struct timeval t;
    while (read_ptr == write_ptr) {
#ifdef DEBUG
      cerr << "Empty queue, gonna wait"<<endl;
#endif
      gettimeofday(&t,NULL);
      t.tv_usec+=50000;
      (void)pthread_mutex_lock(&dq_tex);
      int ret = pthread_cond_timedwait(&dq_cond, &dq_tex,(const timespec*)(&t));
      (void)pthread_mutex_unlock(&dq_tex);
    }
    T* ret = read_ptr;
    if (read_ptr == qend)
      read_ptr = qptr;
    else
      read_ptr++;

    //(void)pthread_mutex_lock(&eq_tex);
    int r = pthread_cond_signal(&eq_cond);
    //(void)pthread_mutex_unlock(&eq_tex);
    return *ret;
  }


};


#endif // TQUEUE_H
