
// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>


lock_server::lock_server():
  nacquire (0)
{
	pthread_mutex_init(&mutex_lock_map, NULL);
   pthread_mutex_init(&mutex_acquire, NULL);
   pthread_cond_init(&cond_lock_map, NULL);
  
}
lock_server::~lock_server()
{
   pthread_mutex_destroy(&mutex_lock_map);
   pthread_mutex_destroy(&mutex_acquire);
   pthread_cond_destroy(&cond_lock_map);
  
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;

  // /****************************************************/

  printf("stat request from clt %d\n", clt);

  r = nacquire;
  return ret;
}


/*Added by Shouda **************************************************/
lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r){

    printf("lock_server::acquire very begining, lid:%016llx\n", lid);
	//pthread_mutex_lock(&mutex_acquire);
    pthread_mutex_lock(&mutex_lock_map);
    printf("lock_server::acquire after pthread_mutex_lock, lid:%016llx\n", lid);
	lock_protocol::status ret = 0;
	std::map<lock_protocol::lockid_t, int>::iterator flag= lock_map.find(lid);
	// std::map<lock_protocol::lockid_t, pthread_cond_t*>::iterator flag_cond = cond_map.find(lid);

    if(flag != lock_map.end())
    {	
        printf("lock_server::acquire FOUND in lock_map, lid:%016llx\n", lid);
        //Found
    	pthread_cond_t* p_cond = cond_map[lid];
        pthread_cond_wait(p_cond, &mutex_lock_map);
        //pthread_cond_wait(&cond_lock_map, &mutex_lock_map);

    	// ret = lock_protocol::RETRY;
    	lock_map[lid] = clt; 
    	ret = lock_protocol::OK;
    	// cond_map.erase(lid);
    	// delete p_cond;
    }
    else
    {
        printf("lock_server::acquire NOT FOUND in lock_map, lid:%016llx\n", lid);
        //pthread_mutex_lock(&mutex_lock_map);

    	pthread_cond_t* p_cond = new pthread_cond_t();
    	pthread_cond_init(p_cond, NULL);
    	cond_map[lid] = p_cond;
    	lock_map[lid] = clt; 
    	ret = lock_protocol::OK;
    }

    pthread_mutex_unlock(&mutex_lock_map);
    //pthread_mutex_unlock(&mutex_acquire);
	
	return ret;

}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r){
	lock_protocol::status ret = 0;

    pthread_mutex_lock(&mutex_lock_map);

	std::map<lock_protocol::lockid_t, int>::iterator flag= lock_map.find(lid);
	// std::map<lock_protocol::lockid_t, pthread_cond_t*>::iterator flag_cond = cond_map.find(lid);
    
    //pthread_cond_t* p_cond;
	if(flag != lock_map.end())
	{//Found

        pthread_cond_t* p_cond = cond_map[lid];
		p_cond = cond_map[lid];
		
		lock_map.erase(lid);
        //cond_map.erase(lid);
		ret = lock_protocol::OK;
		
        pthread_cond_signal(p_cond);
        //
        //pthread_cond_broadcast(p_cond);
        //pthread_cond_broadcast(&cond_lock_map);
    
		
	}

    pthread_mutex_unlock(&mutex_lock_map);
  //pthread_cond_signal(p_cond);
	
	return ret;
}


