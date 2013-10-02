// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  // You fill this in for Lab 2.
  ScopedLock ml(&ext_mutex);
  extent_struct* ext_obj; 
  if(extent_storage.find(id) != extent_storage.end())
  {
      //found the file with id
      ext_obj = extent_storage[id];
  }
  else
  {
      //NOT found the file with id
      ext_obj = new extent_struct();
      extent_storage[id] = ext_obj;
  }
  ext_obj->data = buf;

  ext_obj->file_attr.mtime = time(NULL);
  ext_obj->file_attr.atime = time(NULL);
  ext_obj->file_attr.size = ext_obj->data.size();
  
  //return extent_protocol::IOERR;
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  // You fill this in for Lab 2.
  ScopedLock ml(&ext_mutex);
  extent_struct* ext_obj; 
  if(extent_storage.find(id) != extent_storage.end())
  {
      //found the file with id
      ext_obj = extent_storage[id];
      ext_obj->file_attr.atime = time(NULL);
      buf = ext_obj->data;
      printf("extent_server::get found the file of %01611x \n", id);
      return extent_protocol::OK;
  }
  else
  {
      //NOT found the file with id
      printf("extent_server::get NOT found the file of %01611x \n", id);
      return extent_protocol::NOENT;
  }
  //return extent_protocol::IOERR;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  ScopedLock ml(&ext_mutex);
  extent_struct* ext_obj;
  if (this->extent_storage.find(id) != extent_storage.end()){
    printf("extent_server::getattr find the key of %016llx \n", id);
    ext_obj = extent_storage[id];
    a.size = ext_obj->file_attr.size;
    a.atime = ext_obj->file_attr.atime;
    a.mtime = ext_obj->file_attr.mtime;
    a.ctime = ext_obj->file_attr.ctime;
  }
  else{
    a.size = 0;
    a.atime = 0;
    a.mtime = 0;
    a.ctime = 0;
  }
  
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  // You fill this in for Lab 2.
  ScopedLock ml(&ext_mutex);
  extent_struct* ext_obj;
  if (this->extent_storage.find(id) != extent_storage.end()){
    printf("extent_server::remove find the key of %016llx \n", id);
    ext_obj = extent_storage[id];
    delete ext_obj;
    extent_storage.erase(id);
    return extent_protocol::OK;
  }
  else
  {
      return extent_protocol::NOENT;
  }
  return extent_protocol::IOERR;
}

