#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <map>


class lock_server_cache {
    private:
        int nacquire;
    protected:
        enum xxlock_status { LOCKFREE, LOCKED, REVOKING, RETRYING};
        typedef int lock_status;
        struct lock_cache_bean{
            lock_status status;
            std::string owner_client_id;
            std::string retrying_client_id;
            std::list<std::string> waiting_client_ids;
        };
        typedef std::map<lock_protocol::lockid_t, lock_cache_bean*> LockMapT;
        LockMapT lock_map;
        struct client_bean{
            std::string client_id;
            lock_protocol::lockid_t lid;
        };
        std::list<client_bean> retry_list;
        std::list<client_bean> revoke_list;

        pthread_mutex_t lock_map_mutex;
        pthread_mutex_t retry_mutex;
        pthread_mutex_t release_mutex;

        pthread_cond_t lock_map_cond;
        pthread_cond_t retry_cond;
        pthread_cond_t release_cond;

        lock_cache_bean* get_lock_bean(lock_protocol::lockid_t lid);

    public:
        lock_server_cache();
        ~lock_server_cache();
        lock_protocol::status stat(lock_protocol::lockid_t, int &);
        int acquire(lock_protocol::lockid_t, std::string id, int &);
        int release(lock_protocol::lockid_t, std::string id, int &);
        void release_loop();
        void retry_loop();
};

#endif
