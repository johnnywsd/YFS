// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
 int init_ret = pthread_mutex_init(&ext_mutex,0);   
 printf("ext_mutex init return: %d, (0 means succeed)", init_ret);
}
extent_server::~extent_server() {
 int destroy_ret = pthread_mutex_destroy(&ext_mutex);   
 printf("ext_mutex destroy return: %d, (0 means succeed)", destroy_ret);
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int offset, int &r)
{
  printf("extent_server::put start \n");
  // You fill this in for Lab 2.
  ScopedLock ml(&ext_mutex);
  extent_struct* ext_obj; 
  if(this->extent_storage.find(id) != extent_storage.end())
  {
      //found the file with id
      ext_obj = extent_storage[id];
      if(offset > ext_obj->data.size()){ 
          ext_obj->data.resize(offset,'\0');
          ext_obj->data.append(buf);
      }
      else
      {
          //ext_obj->data.resize(offset);
          //ext_obj->data.append(buf);
          if( buf.empty())
          {
              ext_obj->data.resize(offset);
          }
          else
          {
              ext_obj->data.replace(offset,buf.size(),buf);
          }
      }
      printf("extent_server::put find id \n");
  }
  else
  {
      //NOT found the file with id
      ext_obj = new extent_struct();
      extent_storage[id] = ext_obj;
      printf("extent_server::put not find id, create new,id:%016llx \n", id);
  }
  //ext_obj->data = buf;

  ext_obj->file_attr.mtime = time(NULL);
  ext_obj->file_attr.ctime = time(NULL);
  ext_obj->file_attr.atime = time(NULL);
  ext_obj->file_attr.size = ext_obj->data.size();
  extent_storage[id] = ext_obj; 
  //return extent_protocol::IOERR;
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, int offset, unsigned int size, std::string &buf)
{
  // You fill this in for Lab 2.
  ScopedLock ml(&ext_mutex);
  extent_struct* ext_obj; 
  if(this->extent_storage.find(id) != extent_storage.end())
  {
      //found the file with id
      ext_obj = extent_storage[id];
      ext_obj->file_attr.atime = time(NULL);
      if(offset <= ext_obj->file_attr.size && offset>=0)
      {
        buf = ext_obj->data;
        buf = buf.substr(offset,size);
      }
      else
      {
        buf = "\0";
      }
      printf("extent_server::get found the file of %016lx data: %s\n", id, buf.c_str());
      return extent_protocol::OK;
  }
  else
  {
      //NOT found the file with id
      printf("extent_server::get NOT found the file of %016lx \n", id);
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
    printf("extent_server::getattr find the key of %016lx \n", id);
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
    printf("extent_server::remove find the key of %016lx \n", id);
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

