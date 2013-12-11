// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


static void*
release_thread(void *x)
{
  lock_server_cache_rsm *lsc = (lock_server_cache_rsm *) x;
  //lock_server_cache *lsc = (lock_server_cache*) x; 
  lsc->release_loop();
  return 0;
}

static void*
retry_thread(void *x)
{
  lock_server_cache_rsm *lsc = (lock_server_cache_rsm *) x;
  //lock_server_cache *lsc = (lock_server_cache*) x; 
  lsc->retry_loop();
  return 0;
}


lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) 
  : rsm (_rsm)
{
  //pthread_t th;
  //int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  //VERIFY (r == 0);
  //r = pthread_create(&th, NULL, &retrythread, (void *) this);
  //VERIFY (r == 0);
  VERIFY( pthread_mutex_init(&lock_map_mutex,NULL) == 0 );
  VERIFY( pthread_mutex_init(&retry_mutex,NULL) == 0 );
  VERIFY( pthread_mutex_init(&release_mutex,NULL) == 0 );

  VERIFY( pthread_cond_init(&lock_map_cond,NULL) == 0 );
  VERIFY( pthread_cond_init(&retry_cond,NULL) == 0 );
  VERIFY( pthread_cond_init(&release_cond,NULL) == 0 );

  pthread_t pt_release;
  pthread_t pt_retry;
  int flag_1, flag_2;

  rsm->set_state_transfer(this);

  flag_1 = pthread_create(&pt_release,NULL,
      &release_thread, (void*)this);
  flag_2 = pthread_create(&pt_retry,NULL,
      &retry_thread, (void*)this);

  VERIFY (flag_1 == 0);
  VERIFY (flag_2 == 0);

}

lock_server_cache_rsm::~lock_server_cache_rsm()
{
  VERIFY( pthread_mutex_destroy(&lock_map_mutex) == 0 );
  VERIFY( pthread_mutex_destroy(&retry_mutex) == 0 );
  VERIFY( pthread_mutex_destroy(&release_mutex) == 0 );

  VERIFY( pthread_cond_destroy(&lock_map_cond) == 0 );
  VERIFY( pthread_cond_destroy(&retry_cond) == 0 );
  VERIFY( pthread_cond_destroy(&release_cond) == 0 );
}

lock_server_cache_rsm::lock_cache_bean*
lock_server_cache_rsm::get_lock_bean(lock_protocol::lockid_t lid)
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

//void
//lock_server_cache_rsm::revoker()
//{

  //// This method should be a continuous loop, that sends revoke
  //// messages to lock holders whenever another client wants the
  //// same lock
//}

void
lock_server_cache_rsm::release_loop(void)
{
  // Originally it called  lock_server_cache_rsm::revoker()
  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  int r;
  lock_protocol::xid_t xid = 0;
  rlock_protocol::status r_ret;
  while(true)
  {
    pthread_mutex_lock(&release_mutex);
    pthread_cond_wait(&release_cond,&release_mutex);
    while (!revoke_list.empty())
    {
      client_bean cb = revoke_list.front();
      revoke_list.pop_front();
      //handle hl = (cb.client_id);
      if (rsm->amiprimary())
      {
        handle hl(cb.client_id);
        if (hl.safebind())
        {
          tprintf("lock_server_cache::release_loop, try to revoke,"
              "id:%s,\t lid:%016llx\n",
              cb.client_id.c_str(), cb.lid);
          r_ret = hl.safebind()->call(
              rlock_protocol::revoke,
              cb.lid,
              xid,
              r
              );
        }
        else
        {
          tprintf("Revoke RPC failed. NOT safebind");
        }
        if(r_ret == rlock_protocol::OK)
        {
          tprintf("lock_server_cache::release_loop,"
              " Revoked, id:%s,\t lid:%016llx\n",
              cb.client_id.c_str(), cb.lid);
        }
        if (r_ret != rlock_protocol::OK)
        {
          tprintf("Revoke RPC failed, return: %d", r_ret);
        }
      }
      else
      {
      }
    }
    pthread_mutex_unlock(&release_mutex);
  }
}

//void
//lock_server_cache_rsm::retryer()
//{

  //// This method should be a continuous loop, waiting for locks
  //// to be released and then sending retry messages to those who
  //// are waiting for it.
//}

void 
lock_server_cache_rsm::retry_loop(void)
{
  // Originally it called  lock_server_cache_rsm::retryer()
  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
  int r;
  lock_protocol::xid_t xid = 0;
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
      if (rsm->amiprimary())
      {
        if (hl.safebind())
        {
          r_ret = hl.safebind()->call(
              rlock_protocol::retry,
              cb.lid,
              xid,
              r);
        }
        else
        {
          tprintf("Retry RPC failed. NOT safebind");
        }
        if (r_ret == rlock_protocol::OK)
        {
          tprintf("lock_server_cache::retry_loop, Retried,"
              "id:%s, lid: %016llx\n", cb.client_id.c_str(), cb.lid);
        }
        else
        {
          tprintf("Retry RPC failed, return:%d", r_ret);
        }
      }
    }
    pthread_mutex_unlock(&retry_mutex);
  }
}

//int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id, 
             //lock_protocol::xid_t xid, int &)
//{
  //lock_protocol::status ret = lock_protocol::OK;
  //return ret;
//}

int
lock_server_cache_rsm::acquire(
    lock_protocol::lockid_t lid, std::string id,
    lock_protocol::xid_t xid, int &)
{
  pthread_mutex_lock(&lock_map_mutex);
  lock_protocol::status ret = lock_protocol::OK;
  lock_cache_bean* lcb;
  lcb = get_lock_bean(lid);

  tprintf("lock_server_cache::acquire,"
      " begin, lid:%016llx, id:%s, status:%d\n",
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
    lock_cache_bean->xid = xid;
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
    ret = lock_protocol::OK;
    tprintf("lock_server_cache::acquire,"
        " Acquired, lid:%016llx, id:%s\n", lid, id.c_str());
  }
  else
  {
    lcb->waiting_client_ids.push_back(id);
    if(lcb->status == LOCKED)
    {
      tprintf("lock_server_cache::acquire,"
          "status:LOCKED, lid:%016llx, id:%s\n",
          lid, id.c_str());
      lcb->status = REVOKING;
      pthread_mutex_lock(&release_mutex);
      client_bean cb;
      cb.client_id = lcb->owner_client_id;
      cb.lid = lid;
      revoke_list.push_back(cb);
      pthread_mutex_unlock(&release_mutex);
      pthread_cond_signal(&release_cond);
    }
    ret = lock_protocol::RETRY;
  }
  pthread_mutex_unlock(&lock_map_mutex);
  return ret;
}

//int 
//lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
         //lock_protocol::xid_t xid, int &r)
//{
  //lock_protocol::status ret = lock_protocol::OK;
  //return ret;
//}

int 
lock_server_cache_rsm::release(lock_protocol::lockid_t lid,
    std::string id,
    lock_protocol::xid_t xid, int &r)
{
  pthread_mutex_lock(&lock_map_mutex);

  lock_protocol::status ret = lock_protocol::OK;
  lock_cache_bean* lcb;
  lcb = get_lock_bean(lid);
  lcb->status = LOCKFREE;

  tprintf("lock_server_cache::release, begin, id:%s, lid:%016llx, status:%d\n",
      id.c_str(), lid, lcb->status);

  if (lcb->waiting_client_ids.size() > 0 )
  {
    lcb->status = RETRYING;
    client_bean cb;
    cb.client_id = lcb->waiting_client_ids.front();
    cb.lid = lid;
    lcb->retrying_client_id = cb.client_id;
    pthread_mutex_lock(&retry_mutex);
    retry_list.push_back(cb);
    pthread_mutex_unlock(&retry_mutex);
    pthread_cond_signal(&retry_cond);
  }
  pthread_mutex_unlock(&lock_map_mutex);
  tprintf("lock_server_cache::release, Released, let next waiter retry"
    "id:%s, lid:%016llx, status:%d\n ", id.c_str(), lid, lcb->status);
  return ret;
}

std::string
lock_server_cache_rsm::marshal_state()
{
  std::ostringstream ost;
  std::string r;
  return r;
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  printf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

