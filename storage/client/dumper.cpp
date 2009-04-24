#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <arpa/inet.h>

#define MAXDATASIZE 256 // max number of bytes we can get at once 
#define VERSTR "SCADSBDB0.1"

// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa)
{
  if (sa->sa_family == AF_INET) {
    return &(((struct sockaddr_in*)sa)->sin_addr);
  }

  return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

int main(int argc, char *argv[])
{
  int sockfd, numbytes;  
  char buf[MAXDATASIZE];
  struct addrinfo hints, *servinfo, *p;
  int rv;
  char s[INET6_ADDRSTRLEN];
  int nslen;

  if (argc != 4) {
    fprintf(stderr,"usage: %s hostname port namespace\n",argv[0]);
    exit(1);
  }

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((rv = getaddrinfo(argv[1], argv[2], &hints, &servinfo)) != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
    return 1;
  }

  // loop through all the results and connect to the first we can
  for(p = servinfo; p != NULL; p = p->ai_next) {
    if ((sockfd = socket(p->ai_family, p->ai_socktype,
			 p->ai_protocol)) == -1) {
      perror("client: socket");
      continue;
    }

    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(sockfd);
      perror("client: connect");
      continue;
    }

    break;
  }

  if (p == NULL) {
    fprintf(stderr, "client: failed to connect\n");
    return 2;
  }

  inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr),
            s, sizeof s);
  printf("client: connecting to %s\n", s);

  freeaddrinfo(servinfo); // all done with this structure

  
  
  if ((numbytes = recv(sockfd, buf, 11, 0)) == -1) {
    perror("Error receiving version string: ");
    return 2;
  }
  
  buf[numbytes] = '\0';
  
  if (strncmp(VERSTR,buf,11)) {
    fprintf(stderr,"Version strings didn't match");
    return 2;
  }

  // now send dump op
  nslen = 2;
  if (send(sockfd,&nslen,1,MSG_MORE) == -1) {
    perror("Error sending dump op: ");
    return 2;
  }

  // now send our namespace
  nslen = strlen(argv[3]);
  if (send(sockfd,&nslen,4,MSG_MORE) == -1) {
    perror("Error sending namespace length: ");
    return 2;
  }
  printf("Sending namespace: %s\n",argv[3]);
  if (send(sockfd,argv[3],nslen,MSG_MORE) == -1) {
    perror("Error sending namespace: ");
    return 2;
  }

  // now get all our key/value pairs

  for (;;) {
    int len;
    if ((numbytes = recv(sockfd, buf, 4, 0)) == -1) {
      perror("recv");
      exit(1);
    }
   
    if (numbytes != 4) {
      fprintf(stderr,"Couldn't get four bytes for length\n");
      return 2;
    }
    memcpy(&len,buf,4);

    if (len == 0) {
      printf("Done\n");
      break;
    }

    if (len >= MAXDATASIZE) {
      fprintf(stderr,"Data size too big\n");
      return 2;
    }

    numbytes = 0;
    while (numbytes < len) {
      int r;
      if ((r = recv(sockfd, buf, len-numbytes, 0)) == -1) {
	perror("recv");
	exit(1);
      }
      numbytes+=r;
    }
   
    if (numbytes != len) {
      fprintf(stderr,"Couldn't get all bytes for data value\n");
      return 2;
    }
    buf[len]='\0';

    printf("read: %s\n",buf);
  }
  close(sockfd);

  return 0;
}
