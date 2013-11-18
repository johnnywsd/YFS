// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


static void* release_thread(void *x)
{
    lock_client_cache *lcc = (lock_client_cache*) x; 
    lcc->release_loop();
    return 0;
}
static void* retry_thread(void *x)
{
    lock_client_cache *lcc = (lock_client_cache*) x; 
    lcc->retry_loop();
    return 0;
}

lock_client_cache::lock_client_cache(std::string xdst, 
        class lock_release_user *_lu)
: lock_client(xdst), lu(_lu)
{
    rpcs *rlsrpc = new rpcs(0);
    rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

    const char *hname;
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlsrpc->port();
    id = host.str();



    VERIFY( pthread_mutex_init(&lock_map_mutex,NULL) == 0 );
    VERIFY( pthread_mutex_init(&retry_mutex,NULL) == 0 );
    VERIFY( pthread_mutex_init(&release_mutex,NULL) == 0 );

    VERIFY( pthread_cond_init(&lock_map_cond,NULL) == 0 );
    VERIFY( pthread_cond_init(&retry_cond,NULL) == 0 );
    VERIFY( pthread_cond_init(&release_cond,NULL) == 0 );

    pthread_t pt_release;
    pthread_t pt_retry;
    int flag_1, flag_2;

    flag_1 = pthread_create(&pt_release,NULL, &release_thread, (void*)this);
    flag_2 = pthread_create(&pt_retry,NULL, &retry_thread, (void*)this);

    VERIFY (flag_1 == 0);
    VERIFY (flag_2 == 0);

}

lock_client_cache::lock_cache_bean*
lock_client_cache::get_lock_bean(lock_protocol::lockid_t lid)
{
    lock_cache_bean* ret;
    pthread_mutex_lock(&lock_map_mutex);
    if(lock_map.find(lid) != lock_map.end())
    {
        ret = lock_map[lid];
        //tprintf("lock_client_cache::get_lock_bean FOUND, lid:%llu, status:%d\n",
                //lid, ret->status);
    }
    else
    {
        //tprintf("lock_client_cache::get_lock_bean NOT found, create new, lid:%llu\n", lid);
        ret = new lock_cache_bean();
        int flag_1, flag_2, flag_3;
        flag_1 = pthread_mutex_init(&ret->lock_mutex,NULL);
        flag_2 = pthread_cond_init(&ret->lock_cond,NULL);
        flag_3 = pthread_cond_init(&ret->revoke_cond,NULL);
        ret->status = NONE;
        lock_map[lid] = ret;
    }
    pthread_mutex_unlock(&lock_map_mutex);
    return ret;

}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
    int r;
    int ret = lock_protocol::OK;
    lock_cache_bean* lcb;
    lcb = get_lock_bean(lid);
    pthread_mutex_lock( &lcb->lock_mutex );

    bool acquire_succeed = false;

    while(true)
    {
        if (lcb->status == NONE)
        {
            //tprintf("lock_client_cache::acquire lcb->status == NONE, lid:%llu\n", lid);
            lcb->status = ACQUIRING;
            ret = cl->call(lock_protocol::acquire, lid, id, r);

            //tprintf("lock_client_cache::acquire ret:%d\n", ret);
            if (ret == lock_protocol::OK )
            {
                lcb->status = LOCKED;
                acquire_succeed = true;
                break;
            }
            else if ( ret == lock_protocol::RETRY )
            {
                //tprintf("lock_client_cache::acquire ret=RETRY, lid:%llu\n", lid);
                pthread_cond_wait(&lcb->lock_cond, &lcb->lock_mutex);
                if (lcb->status == FREE)
                {
                    lcb->status = LOCKED;
                    acquire_succeed = true;
                    break;
                }
                else
                {
                    continue;
                }
            }
        }
        else if (lcb->status == FREE)
        {
            lcb->status = LOCKED;
            acquire_succeed = true;
            break;
        }
        else
        {
            //It should be RELEASING
            //
            //tprintf("lock_client_cache::acquire else, lid:%llu, status:%d\n",
                    //lid, lcb->status);
            pthread_cond_wait(&lcb->lock_cond, &lcb->lock_mutex);
            //tprintf("lock_client_cache::acquire else,after wait, lid:%llu", lid);
            if (lcb->status == FREE)
            {
                lcb->status = LOCKED;
                acquire_succeed = true;
                break;
            }
            else
                continue;
        }
    }

    pthread_mutex_unlock(&lcb->lock_mutex);

    //tprintf("lock_client_cache::acquire END\n");
    if (acquire_succeed)
    {
        tprintf("lock_client_cache::acquire, Acquired, id:%s,\t lid:%llu\n", id.c_str(), lid);
    }
    return lock_protocol::OK;

}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
    bool release_succeed = false;
    tprintf("lock_client_cache::release BEGIN,id:%s\t, lid:%llu\n",id.c_str(), lid);
    lock_cache_bean* lcb = get_lock_bean(lid);
    pthread_mutex_lock(&lcb->lock_mutex);
    if (lcb->status == LOCKED)
    {
        //tprintf("lock_client_cache::release lcb->status==LOCKED, lid:%llu\n",lid);
        tprintf("lock_client_cache::release lcb->status==LOCKED, id%s,\t lid:%llu\n",id.c_str(), lid);
        lcb->status = FREE;
        release_succeed = true;
        pthread_cond_signal(&lcb->lock_cond);
    }
    else if (lcb->status == RELEASING)
    {
        tprintf("lock_client_cache::release lcb->status==RELEASING,id%s,\t lid:%llu\n",id.c_str(), lid);
        pthread_cond_signal(&lcb->revoke_cond);
    }
    else
    {
        tprintf("Status unknown: %d\n", lcb->status);
    }
    pthread_mutex_unlock(&lcb->lock_mutex);

    //tprintf("lock_client_cache::release END,lid:%llu, status:%d\n",
            //lid, lcb->status);
    if (release_succeed)
    {
        tprintf("lock_client_cache::release, Released, id:%s,\t lid:%llu\n", id.c_str(), lid);
    }
    else
    {
        tprintf("lock_client_cache::release, NOT release, id:%s,\t lid:%llu, "
                "status:%d\n",
                id.c_str(), lid, lcb->status);
    }
    return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
        int &)
{
    int ret = rlock_protocol::OK;
    pthread_mutex_lock(&release_mutex);
    revoke_list.push_back(lid);
    pthread_cond_signal(&release_cond);
    pthread_mutex_unlock(&release_mutex);
    return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
        int &)
{
    int ret = rlock_protocol::OK;
    pthread_mutex_lock(&retry_mutex);
    retry_list.push_back(lid);
    pthread_cond_signal(&retry_cond);
    pthread_mutex_unlock(&retry_mutex);
    return ret;
}

void
lock_client_cache::release_loop(void)
{
    int r;
    int ret;
    while(true)
    {
        pthread_mutex_lock(&release_mutex);
        pthread_cond_wait(&release_cond,&release_mutex);
        tprintf("lock_client_cache::release_loop got wait\n");
        while(!revoke_list.empty())
        {
            lock_protocol::lockid_t lid = revoke_list.front();
            revoke_list.pop_front();
            lock_cache_bean* lcb = get_lock_bean(lid);
            tprintf("lock_client_cache::release_loop, in while,"
                    "id%s,\t lid:%llu, status:%d\n", id.c_str(), lid, lcb->status);
            pthread_mutex_lock(&lcb->lock_mutex);
            if (lcb->status == LOCKED)
            {
                lcb->status = RELEASING;
                pthread_cond_wait(&lcb->revoke_cond,&lcb->lock_mutex);
                tprintf("lock_client_cache::release_loop, in while, got lcb->revoke_cond,"
                        "id%s,\t lid:%llu, status:%d\n", id.c_str(), lid, lcb->status);
            }
            ret = cl->call(lock_protocol::release, lid, id, r);
            if (ret == lock_protocol::OK)
            {
                lcb->status = NONE;
                pthread_cond_signal(&lcb->lock_cond);
                tprintf("lock_client_cache::release_loop, Revoked,"
                        "id%s,\t lid:%llu, status:%d\n", id.c_str(), lid, lcb->status);
            }
            else
            {
                tprintf("lock_client_cache::release_loop, FAILED, ret:%d\n", ret);
            }
            pthread_mutex_unlock(&lcb->lock_mutex);
        }
        pthread_mutex_unlock(&release_mutex);
    }
}

void
lock_client_cache::retry_loop(void)
{
    int r;
    int ret;
    while(true)
    {
        pthread_mutex_lock(&retry_mutex);
        pthread_cond_wait(&retry_cond,&retry_mutex);
        while(!retry_list.empty())
        {
            lock_protocol::lockid_t lid = retry_list.front();
            retry_list.pop_front();
            lock_cache_bean* lcb = get_lock_bean(lid);
            pthread_mutex_lock(&lcb->lock_mutex);
            ret = cl->call(lock_protocol::acquire, lid, id, r);
            if (ret == lock_protocol::OK)
            {
                lcb->status = FREE;
                pthread_cond_signal(&lcb->lock_cond);
            }
            else
            {
                tprintf("lock_client_cache::retry_loop, FAILED, ret:%d\n", ret);
            }
            pthread_mutex_unlock(&lcb->lock_mutex);
        }
        pthread_mutex_unlock(&retry_mutex);
    }

}



