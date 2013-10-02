// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iterator>
#include <cstdlib>

const char yfs_client::DELIMITER = ';'; 
const char yfs_client::SUB_DELIMITER = ','; 

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  //lcc = new lock_client(lock_dst);

}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  //It is i2n
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}


yfs_client::inum 
yfs_client::generate_inum(bool is_file)
{
  yfs_client::inum new_inum;
  long range = 0x7FFFFFFF;
  long rand_num = (rand() * range) / RAND_MAX;
  if (is_file) 
    new_inum = rand_num | 0x0000000080000000;
  else
    new_inum = rand_num & 0x000000007FFFFFFF;
  printf("new_inum %016llx \n",new_inum);
  return new_inum;
}


yfs_client::status
yfs_client::lookup(inum inum_p, const char* name, inum& inum_c)
{
  std::string buf;
  if( ec->get(inum_p, buf) == extent_protocol::OK ){
    //Parent Found
	std::stringstream all_entry_str_stream(buf);
	std::string entry_str;

    std::string target_name_str(name);
    //segment : inum_str, name_str
	while(std::getline(all_entry_str_stream, entry_str, yfs_client::DELIMITER))
	{
	   //seglist.push_back(segment);
        std::string inum_str;
        std::string name_str;
        std::stringstream entry_str_stream(entry_str);
        std::getline(entry_str_stream, inum_str,yfs_client::SUB_DELIMITER);
        std::getline(entry_str_stream, name_str,yfs_client::SUB_DELIMITER);
        if (target_name_str == name_str)
        {
            printf("yfs_client::lookup %01611x parent directory found the filename %s \n", inum_p, target_name_str.c_str());
            inum_c = n2i(inum_str);
            return yfs_client::OK;
        }
	}
    printf("yfs_client::lookup %01611x parent directory NOT found the filename %s \n", inum_p, target_name_str.c_str());
    return yfs_client::NOENT;

  }else{

    return yfs_client::NOENT;
  }
}


yfs_client::status
yfs_client::createfile(inum inum_p, const char* name, inum& inum_c)
{ 
  yfs_client::status ret = yfs_client::OK;
  std::string all_entry_str;
  //Check if this file already exists in inum_p 
  if(ec->get(inum_p, all_entry_str) == extent_protocol::OK)
  {
    //Parent exists
    //all_entry_str: inum_str, name_str; inum_str, name_str

    bool flag_file_found = false;
    std::stringstream all_entry_str_stream(all_entry_str);
	std::string entry_str;

    std::string target_name_str(name);
	while(std::getline(all_entry_str_stream, entry_str, yfs_client::DELIMITER))
	{
        std::string inum_str;
        std::string name_str;
        std::stringstream entry_str_stream(entry_str);
        std::getline(entry_str_stream, inum_str,yfs_client::SUB_DELIMITER);
        std::getline(entry_str_stream, name_str,yfs_client::SUB_DELIMITER);
        if (target_name_str == name_str)
        {
            printf("yfs_client::createfile %01611x parent directory found the filename %s \n", inum_p, target_name_str.c_str());
            flag_file_found = true;
            break;
        }
	}

    std::string buf2;
    if(flag_file_found == false)
    {
        //File does not exist, able to create new file.
        inum new_inum = generate_inum(false); 
        all_entry_str.append(filename(new_inum)+ yfs_client::SUB_DELIMITER + std::string(name) + yfs_client::DELIMITER );

        //add the new file
        if( ec->put(new_inum, std::string(""))!= extent_protocol::OK)
        {
            printf("yfs_client::createfile IOERR 1 > ");
            return yfs_client::IOERR;
        }
        
        //change the parent folder info
        if( ec->put(inum_p, all_entry_str)!= extent_protocol::OK)
        {
            printf("yfs_client::createfile IOERR 2> ");
            return yfs_client::IOERR;
        }
    }
    else
    {
      printf("yfs_client::createfile %01611x parent directory exist, but file %s also exist > \n", inum_p, name);
    }
  }
  else
  {
      //Parent directory does not exist.
      printf("yfs_client::createfile %01611x parent directory does not exist \n", inum_p);
      return yfs_client::NOENT;
  }
}

yfs_client::status
yfs_client::createroot()
{
    yfs_client::inum inum_root = 0x00000001;
    std::string all_entry_str = filename(inum_root) + SUB_DELIMITER + std::string("root") + DELIMITER;
    if(ec->put(inum_root, all_entry_str) == extent_protocol::OK)
    {
        return yfs_client::OK;
    }
    else
    {
        return yfs_client::IOERR;
    }
}
