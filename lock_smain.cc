#include "rpc.h"
#include <arpa/inet.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include "lock_server.h"

#include "jsl_log.h"

// Main loop of lock_server

static void
force_exit(int) {
    exit(0);
}

int
main(int argc, char *argv[])
{
  int count = 0;

  // Force the lock_server to exit after 20 minutes
  signal(SIGALRM, force_exit);
  alarm(20 * 60);

  setvbuf(stdout, NULL, _IONBF, 0);
  setvbuf(stderr, NULL, _IONBF, 0);

  srandom(getpid());

  if(argc != 2){
    fprintf(stderr, "Usage: %s port\n", argv[0]);
    exit(1);
  }

  char *count_env = getenv("RPC_COUNT");
  if(count_env != NULL){
    count = atoi(count_env);
  }

  //jsl_set_debug(2);

#ifndef RSM
  lock_server ls;
  rpcs server(atoi(argv[1]), count);  //atoi(argv[1]) == port, count == PRC_COUNT, or 0. Init a rpcs
  server.reg(lock_protocol::stat, &ls, &lock_server::stat); // regerist the rpcs.   
  //lock_protocol::stat : Unsigned int, &ls : lock_server, &lock_server::stat: a function 

  /*******************************************************************/
  server.reg(lock_protocol::acquire, &ls, &lock_server::acquire);
  server.reg(lock_protocol::release, &ls, &lock_server::release);
  /*******************************************************************/
  
#endif


  while(1)
    sleep(1000);
}
