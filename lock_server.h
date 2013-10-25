#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <map>
#include <pthread.h>

class lock_server {

    protected:
        int nacquire;
        enum lock_state { LOCKFREE, LOCKED };
        typedef int lock_state;
        typedef std::map<lock_protocol::lockid_t, lock_state> LockMapT;
        LockMapT lock_map;
        pthread_mutex_t lock_map_mutex;
        pthread_cond_t lock_map_cond;


    public:
        lock_server();
        ~lock_server();
        lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
        lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
        lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);

};

#endif 
