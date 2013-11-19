// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "tprintf.h"

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, int offset, unsigned int size, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  //ret = cl->call(extent_protocol::get, eid, offset, size, buf); //remove in lab5
  ScopedLock ml(&extent_cache_map_mutex);
  extent_bean* bean;
  while(true)
  {
    if ( extent_cache_map.find(eid) == extent_cache_map.end() )
    {
      tprintf("extent_client::get, not found in cache, eid:%016llx\n",
          eid);
      ret = load_from_extent_server(eid);
      if (ret == extent_protocol::OK )
      {
        continue;
      }
      else
      {
        break;
      }
    }
    else
    {
      //Find in cache
      tprintf("extent_client::get, Found in cache, eid:%016llx\n",
          eid);
      bean = extent_cache_map[eid];
      int file_attr_size = (int)bean->ext_attr.size;
      if(offset <= file_attr_size && offset >= 0)
      {
        buf = bean->data;
        buf = buf.substr(offset,size);
      }
      else if(offset < 0)
      {
        buf = bean->data;
      }
      else
      {
        // offset > file_attr_size
        buf = '\0';
      }
      break;
    }
  }
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  //ret = cl->call(extent_protocol::getattr, eid, attr); //remove in lab5
  ScopedLock ml(&extent_cache_map_mutex);
  extent_bean* bean;
  while(true)
  {
    if(extent_cache_map.find(eid) == extent_cache_map.end())
    {
      tprintf("extent_client::getattr, not found in cache\n");
      ret = load_from_extent_server(eid);
      if ( ret == extent_protocol::OK )
      {
        continue;
      }
      else
      {
        break;
      }
    }
    else 
    {
      //extent_cache_map.find(eid) != extent_cache_map.end()
      bean = extent_cache_map[eid];
      attr = bean->ext_attr;
      ret = extent_protocol::OK;
      break;
    }
  }
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf, int offset)
{
  extent_protocol::status ret = extent_protocol::OK;
  //int r;  //removed in lab5
  //ret = cl->call(extent_protocol::put, eid, buf, offset, r); //removed in lab5

  tprintf("extent_client::put, BEGIN, eid:%016llx, offset:%d, buf:%s\n",
      eid, offset, buf.c_str());
  ScopedLock ml(&extent_cache_map_mutex);
  while(true)
  {
    if( extent_cache_map.find(eid) == extent_cache_map.end() )
    {
      tprintf("extent_client::put, NOT found it in cache,"
          "eid:%016llx\n", eid);
      ret = load_from_extent_server(eid);
      if ( ret == extent_protocol::OK )
      {
        continue;
      }
      else
      {
        extent_bean* bean = new extent_bean();
        extent_cache_map[eid] = bean;
        tprintf("extent_client::put, NOT found it both in cache and server,"
            "eid:%016llx, ret:%d\n", eid, ret);
        continue;
      }
    }
    else
    {

      extent_bean* bean = extent_cache_map[eid];
      extent_bean* ext_obj = bean;
      tprintf("extent_client::put, Found it in cache,"
          "eid:%016llx, data:%s\n", eid, bean->data.c_str());

      int data_size = (int) (ext_obj->data.size());
      if(offset > data_size)
      { 
        ext_obj->data.resize(offset,'\0');
        ext_obj->data.append(buf);
      }
      else if (offset < 0)
      {
        printf("extent_client::put, offset<1, eid:%016llx, data:%s\n",
            eid,buf.c_str());
        ext_obj->data = buf;
      }
      else 
      {
        if( buf.empty())
        {
          ext_obj->data.resize(offset);
        }
        else
        {
          ext_obj->data.replace(offset,buf.size(),buf);
        }
      }
      ext_obj->ext_attr.mtime = time(NULL);
      ext_obj->ext_attr.ctime = time(NULL);
      ext_obj->ext_attr.atime = time(NULL);
      ext_obj->ext_attr.size = ext_obj->data.size();
      
      ext_obj->is_dirty = true;
      dirty_set.insert(eid);
      //return extent_protocol::IOERR;
      ret = extent_protocol::OK;
      break;
    }
  }
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  //int r; //removed in lab5
  //ret = cl->call(extent_protocol::remove, eid, r); //remove in lab5

  ScopedLock ml(&extent_cache_map_mutex);
  dirty_set.insert(eid);
  tprintf("extent_client::remove, REMOVED eid:%016llx\n", eid);
  delete (extent_cache_map[eid]);
  extent_cache_map.erase(eid);

  return ret;
}

extent_protocol::status 
extent_client::load_from_extent_server(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = 0;
  extent_protocol::attr ext_attr;
  ret = cl->call(extent_protocol::getattr, eid, ext_attr);
  if (ret == extent_protocol::OK)
  {
    extent_bean* bean = new extent_bean();
    bean->ext_attr = ext_attr;
    bean->is_dirty = false;

    tprintf("extent_client::load_from_extent_server, get attr SUCCEED,"
        "eid:%016llx, ext_attr.size:%u\n", eid, ext_attr.size);
    ret = cl->call(extent_protocol::get, eid, 0, ext_attr.size, bean->data);
    if ( ret == extent_protocol::OK )
    {
      tprintf("extent_client::load_from_extent_server, get data SUCCEED,"
          "eid:%016llx, data:%s\n", eid,bean->data.c_str());
      extent_cache_map[eid] = bean;
      return ret;
    }
    else
    {
      tprintf("extent_client::load_from_extent_server, get data FAILED,"
          "eid:%016llx, ret:%d\n", eid, ret );
      return ret;
    }
  }
  else
  {
    tprintf("extent_client::load_from_extent_server, FAILED,"
        "eid:%016llx, ret:%d\n", eid, ret );
  }
  return ret;
}

extent_protocol::status 
extent_client::flush(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = 0;
  int r=0;
  ScopedLock ml(&extent_cache_map_mutex);
  tprintf("extent_client::flush, Begin got lock,"
      "eid:%016llx\n", eid);
  if( extent_cache_map.find(eid) != extent_cache_map.end() )
  {
    tprintf("extent_client::flush, find it in cache,"
        "eid:%016llx\n", eid);
    extent_bean *bean = extent_cache_map[eid];
    if ( bean->is_dirty )
    {
      tprintf("extent_client::flush, is_dirty=true,"
          "eid:%016llx, data:%s\n", eid, bean->data.c_str());
      ret = cl->call(extent_protocol::put, eid, bean->data, -1, r);
    }
    else
    {
      tprintf("extent_client::flush, is_dirty=false,"
          "eid:%016llx\n", eid);
      ret = extent_protocol::OK;
    }
    delete(extent_cache_map[eid]);
    extent_cache_map.erase(eid);
    dirty_set.erase(eid);
  }
  else
  {
    tprintf("extent_client::flush, NOT find it in cache,"
        "eid:%016llx\n", eid);
    if ( dirty_set.find(eid) != dirty_set.end() )
    {
      // It must be deleted.
      tprintf("extent_client::flush, NOT find it in cache, but it's in dirt_set,"
        "eid:%016llx\n", eid);
      ret = cl->call(extent_protocol::remove, eid, r);
      dirty_set.erase(eid);
    }
    else
    {
      tprintf("extent_client::flush, NOT find it in cache, and it's NOT in dirt_set,"
        "eid:%016llx\n", eid);
    }
  }
  return ret;
}


