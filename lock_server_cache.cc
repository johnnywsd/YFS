// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

static void* release_thread(void *x)
{
    lock_server_cache *lsc = (lock_server_cache*) x; 
    lsc->release_loop();
    return 0;
}
static void* retry_thread(void *x)
{
    lock_server_cache *lsc = (lock_server_cache*) x; 
    lsc->retry_loop();
    return 0;
}

lock_server_cache::lock_cache_bean*
lock_server_cache::get_lock_bean(lock_protocol::lockid_t lid)
{
    lock_cache_bean* ret;
    if(lock_map.find(lid) != lock_map.end())
    {
        ret = lock_map[lid];
    }
    else
    {
        ret = new lock_cache_bean();
        lock_map[lid] = ret;
    }
    return ret;
}

lock_server_cache::lock_server_cache():
    nacquire(0)
{
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
lock_server_cache::~lock_server_cache()
{
    VERIFY( pthread_mutex_destroy(&lock_map_mutex) == 0 );
    VERIFY( pthread_mutex_destroy(&retry_mutex) == 0 );
    VERIFY( pthread_mutex_destroy(&release_mutex) == 0 );

    VERIFY( pthread_cond_destroy(&lock_map_cond) == 0 );
    VERIFY( pthread_cond_destroy(&retry_cond) == 0 );
    VERIFY( pthread_cond_destroy(&release_cond) == 0 );
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  lock_cache_bean* lcb;
  lcb = get_lock_bean(lid);

  tprintf("lock_server_cache::acquire, begin, lid:%llu, id:%s\n, status:%d\n",
          lid, id.c_str(), lcb->status);

  if ( (lcb->status == LOCKFREE) || 
      (lcb->status == RETRYING && 
       lcb->retrying_client_id == id) )
  {
      if(lcb->status != LOCKFREE)
      {
          lcb->waiting_client_ids.pop_front();
      }

      lcb->status = LOCKED;
      lcb->owner_client_id = id;
      if( lcb-> waiting_client_ids.size() > 0)
      {
          lcb->status = REVOKING;
          pthread_mutex_lock(&release_mutex);
          client_bean cb;
          cb.client_id = id;
          cb.lid = lid;
          revoke_list.push_back(cb);
          pthread_cond_signal(&release_cond);
          pthread_mutex_unlock(&release_mutex);
      }
      tprintf("lock_server_cache::acquire, Acquired, lid:%llu, id:%s\n", lid, id.c_str());
  }
  else
  {
      lcb->waiting_client_ids.push_back(id);
      if(lcb->status == LOCKED)
      {
          tprintf("lock_server_cache::acquire, status:LOCKED, lid:%llu, id:%s\n",
                  lid, id.c_str());
          lcb->status = REVOKING;
          pthread_mutex_lock(&release_mutex);
          client_bean cb;
          cb.client_id = lcb->owner_client_id;
          cb.lid = lid;
          revoke_list.push_back(cb);
          pthread_cond_signal(&release_cond);
          pthread_mutex_unlock(&release_mutex);
      }
      ret = lock_protocol::RETRY;
  }
  pthread_mutex_unlock(&lock_map_mutex);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
    lock_protocol::status ret = lock_protocol::OK;
    lock_cache_bean* lcb;
    pthread_mutex_lock(&lock_map_mutex);

    tprintf("lock_server_cache::release, begin, id:%s,\t lid:%llu, status:%d\n ",
            id.c_str(), lid, lcb->status);

    lcb = get_lock_bean(lid);
    lcb->status = LOCKFREE;
    if (lcb->waiting_client_ids.size() > 0 )
    {
        lcb->status = RETRYING;
        client_bean cb;
        cb.client_id = lcb->waiting_client_ids.front();
        cb.lid = lid;
        lcb->retrying_client_id = cb.client_id;
        pthread_mutex_lock(&retry_mutex);
        retry_list.push_back(cb);
        pthread_cond_signal(&retry_cond);
        pthread_mutex_unlock(&retry_mutex);
    }
    pthread_mutex_unlock(&lock_map_mutex);
    tprintf("lock_server_cache::release, Released, id:%s, lid:%llu, status:%d\n ",
            id.c_str(), lid, lcb->status);
    return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

void
lock_server_cache::release_loop(void)
{
    int r;
    rlock_protocol::status r_ret;
    while(true)
    {
        pthread_mutex_lock(&release_mutex);
        pthread_cond_wait(&release_cond,&release_mutex);
        while (!revoke_list.empty())
        {
            client_bean cb = revoke_list.front();
            revoke_list.pop_front();
            handle hl = (cb.client_id);
            if (hl.safebind())
            {
                tprintf("lock_server_cache::release_loop, try to revoke, id:%s,\t lid:%llu\n",
                        cb.client_id.c_str(), cb.lid);
                r_ret = hl.safebind()->call(rlock_protocol::revoke, cb.lid, r);
            }
            else
            {
                tprintf("Revoke RPC failed. NOT safebind");
            }
            if(r_ret == rlock_protocol::OK)
            {
                tprintf("lock_server_cache::release_loop, Revoked, id:%s,\t lid:%llu\n",
                        cb.client_id.c_str(), cb.lid);
            }
            if (r_ret != rlock_protocol::OK)
            {
                tprintf("Revoke RPC failed, return: %d", r_ret);
            }
        }
        pthread_mutex_unlock(&release_mutex);
    }
}
void 
lock_server_cache::retry_loop(void)
{
    int r;
    rlock_protocol::status r_ret;
    while (true)
    {
        pthread_mutex_lock(&retry_mutex);
        pthread_cond_wait(&retry_cond,&retry_mutex);
        while (!retry_list.empty())
        {
            client_bean cb = retry_list.front();
            retry_list.pop_front();
            handle hl(cb.client_id);
            if (hl.safebind())
            {
                r_ret = hl.safebind()->call(rlock_protocol::retry, cb.lid, r);
            }
            else
            {
                tprintf("Retry RPC failed. NOT safebind");
            }
            if (r_ret != rlock_protocol::OK)
            {
                tprintf("Retry RPC failed, return: %d", r_ret);
            }
        }
        pthread_mutex_unlock(&retry_mutex);
    }
}

