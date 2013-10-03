// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"
#include <pthread.h>

class extent_server {

 private:
    pthread_mutex_t ext_mutex;
    struct extent_struct {
       std::string data;
       extent_protocol::attr file_attr;
     };

    typedef std::map<extent_protocol::extentid_t, extent_struct *> extent_storage_map;
    extent_storage_map extent_storage;
 public:
  extent_server();
  ~extent_server();

  int put(extent_protocol::extentid_t id, std::string, int offset, int& r);
  int get(extent_protocol::extentid_t id, int offset, unsigned int size, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
};

#endif 







