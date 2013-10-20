#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>


lock_server::lock_server():
  nacquire (0)
{
    pthread_mutex_init(&lock_map_mutex, NULL);
    pthread_cond_init(&lock_map_cond, NULL);
}

lock_server::~lock_server()
{
    pthread_mutex_destroy(&lock_map_mutex);
    pthread_cond_destroy(&lock_map_cond);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
    lock_protocol::status ret = lock_protocol::OK;
    printf("stat request from clt %d\n", clt);
    r = nacquire;
    return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
    lock_protocol::status ret = lock_protocol::OK;

    pthread_mutex_lock(&lock_map_mutex);
    if (lock_map.count(lid) > 0) {
        while (LOCKFREE != lock_map[lid]) 
            pthread_cond_wait(&lock_map_cond, &lock_map_mutex);
    }
    lock_map[lid] = LOCKED;
    pthread_mutex_unlock(&lock_map_mutex);

    return ret;

}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&lock_map_mutex);

    if ((lock_map.count(lid) > 0) && (lock_map[lid] == LOCKED)) {
        lock_map[lid] = LOCKFREE;
        pthread_cond_broadcast(&lock_map_cond);
        pthread_mutex_unlock(&lock_map_mutex);
    }
    else {
        pthread_mutex_unlock(&lock_map_mutex);
        ret = lock_protocol::NOENT;
    }

    return ret;
}
