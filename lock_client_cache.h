// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
    public:
        virtual void dorelease(lock_protocol::lockid_t) = 0;
        virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
    private:
        class lock_release_user *lu;
        int rlock_port;
        std::string hostname;
        std::string id;
    protected:
        enum xxlock_status {NONE, FREE, LOCKED, ACQUIRING, RELEASING};
        typedef int lock_status;

        struct lock_cache_bean{
            lock_status status;
            pthread_mutex_t lock_mutex;
            pthread_cond_t lock_cond;
            pthread_cond_t revoke_cond;

            bool is_doflush;
        };
        typedef std::map<lock_protocol::lockid_t, lock_cache_bean*> LockMapClientT;
        LockMapClientT lock_map;
        pthread_mutex_t lock_map_mutex;
        pthread_cond_t lock_map_cond;

        pthread_mutex_t release_mutex;
        pthread_mutex_t retry_mutex;

        pthread_cond_t release_cond;
        pthread_cond_t retry_cond;

        std::list<lock_protocol::lockid_t> revoke_list;
        std::list<lock_protocol::lockid_t> retry_list;

        lock_cache_bean* get_lock_bean(lock_protocol::lockid_t);


    public:
        lock_client_cache(std::string xdst, class lock_release_user *l = 0);
        virtual ~lock_client_cache() {};
        lock_protocol::status acquire(lock_protocol::lockid_t);
        lock_protocol::status release(lock_protocol::lockid_t);
        rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                int &);
        rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                int &);

        void retry_loop();
        void release_loop();
};


#endif
