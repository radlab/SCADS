#ifndef SCADSSEUTIL_H
#define SCADSSEUTIL_H

/** BDB Utility functions **/
#define return_with_error(error) std::cerr << db_strerror(error) << "\n"; return error;
#define return_with_success() return 0;
#define dbt_string(dbt) std::string((char*)(dbt)->data,(dbt)->size)
#define close_if_not_null(db) if ((db) != NULL) { (db)->close((db), 0); }

/** General Utility Functions **/

//Print contents of buffer in hexadecimal.
/* Note: Prints from high address on left to low address on right.
 * this makes integers print "correctly" 2 => 0002, but is the 
 * opposite of what is usually done.
 */
void print_hex(void * buf, int len) {
	for (int i = len - 1; i >= 0; i--) {
		printf("%02X", ((unsigned char *)buf)[i]);
	}
}

#define prepare_timer() struct timeval start_time, end_time, diff_time;
#define start_timer() gettimeofday(&start_time,NULL)
#define end_timer() { \
    gettimeofday(&end_time,NULL); \
    timersub(&end_time,&start_time,&diff_time); \
    printf("%ld.%.6ld\n", diff_time.tv_sec, diff_time.tv_usec); \
  }

#define min(i1, i2) ((i1) < (i2) ? (i1) : (i2))
#define max(i1, i2) ((i1) > (i2) ? (i1) : (i2))
#endif //SCADSSEUTIL_H