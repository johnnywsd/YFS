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
  // You modify this function for Lab 3
  // - hold and release the file lock

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
  // You modify this function for Lab 3
  // - hold and release the directory lock

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
  printf("getdir Release %016llx, RETURN %d\n", inum, r);
  return r;
}



yfs_client::inum 
yfs_client::generate_inum(bool is_file)
{
  printf("yfs_client::generate_inum\n");
  yfs_client::inum new_inum;
  long range = 0x7FFFFFFF;
  //long rand_num = (rand() * range);
  long rand_num = (rand() % range);
  //long rand_num = (rand() * range) / RAND_MAX;
  if (is_file) 
    new_inum = rand_num | 0x0000000080000000;
  else
    new_inum = rand_num & 0x000000007FFFFFFF;
    //new_inum = rand_num & 0xFFFFFFFF7FFFFFFF;
  printf("new_inum %016llx \n",new_inum);
  return new_inum;
}


yfs_client::status
yfs_client::lookup(inum inum_p, const char* name, inum& inum_c)
{
  std::string buf;
  printf("yfs_client::lookup\n" );
  extent_protocol::status flag_get = ec->get(inum_p,0,SIZE_MAX, buf) ;
  printf("yfs_client::lookup, flag_get:%d\n", flag_get);
  printf("yfs_client::lookup, buf: %s\n", buf.c_str());
  if( flag_get == extent_protocol::OK ){
    //Parent Found
    printf("yfs_client::lookup, parent %016lx, parent found\n", inum_p);
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
            inum_c = n2i(inum_str);
            printf("yfs_client::lookup, parent: %016llx, name:%s, FILE FOUND\n", inum_p, name_str.c_str());
            return yfs_client::OK;
        }
	}
    printf("yfs_client::lookup %016lx parent directory NOT found the filename %s \n", inum_p, target_name_str.c_str());
    return yfs_client::NOENT;

  }else{

    printf("yfs_client::lookup, parent %016lx , parent NOT found\n", inum_p);
    return yfs_client::NOENT;
  }
}


yfs_client::status 
yfs_client::readdir(inum inum_p,std::vector<dirent>& dir_entries)
{
  printf("yfs_client::readdir\n");
  std::string buf;
  if( ec->get(inum_p, 0, SIZE_MAX, buf) == extent_protocol::OK ){
    //Parent Found
	std::stringstream all_entry_str_stream(buf);
	std::string entry_str;

    //segment : inum_str, name_str
	while(std::getline(all_entry_str_stream, entry_str, yfs_client::DELIMITER))
	{
        dirent ent;
        std::string inum_str;
        std::string name_str;
        std::stringstream entry_str_stream(entry_str);
        std::getline(entry_str_stream, inum_str,yfs_client::SUB_DELIMITER);
        std::getline(entry_str_stream, name_str,yfs_client::SUB_DELIMITER);
        ent.name = name_str;
        ent.inum = n2i(inum_str);
        dir_entries.push_back(ent);
        printf("yfs_client::readdir %016lx parent directory found the filename %s \n", inum_p, name_str.c_str());
	}
    printf("yfs_client::readdir %016lx parent directory read finished \n", inum_p); 
    return yfs_client::OK;

  }else{
    //parent not found
    return yfs_client::NOENT;
  }
}


yfs_client::status
yfs_client::createfile_helper(inum inum_p, const char* name, inum& inum_c, bool is_file)
{ 
  printf("yfs_cient::createfile_helper\n");
  yfs_client::status ret = yfs_client::OK;
  std::string all_entry_str;
  //Check if this file already exists in inum_p 
  if(ec->get(inum_p,0, SIZE_MAX, all_entry_str) == extent_protocol::OK)
  {
    //Parent exists
    //all_entry_str: inum_str, name_str; inum_str, name_str

    bool flag_file_found = false;
    std::stringstream all_entry_str_stream(all_entry_str);
	std::string entry_str;
    printf("yfs_client::createfile_helper, parent:%016lx directory's all_entry_str: %s \n", inum_p, all_entry_str.c_str());

    std::string target_name_str(name);
	while(std::getline(all_entry_str_stream, entry_str, yfs_client::DELIMITER))
	{
        std::string inum_str;
        std::string name_str;
        std::stringstream entry_str_stream(entry_str);
        std::getline(entry_str_stream, inum_str,yfs_client::SUB_DELIMITER);
        std::getline(entry_str_stream, name_str,yfs_client::SUB_DELIMITER);
        printf("yfs_client::createfile_helper, while loop, parent:%016lx, Checking: inum:%016lx, inum_str:%s,filename:%s,target_name_str:%s \n",
                inum_p, n2i(inum_str), inum_str.c_str(),name_str.c_str(), target_name_str.c_str());
        if (target_name_str == name_str)
        {
            printf("yfs_client::createfile_helper while loop, FOUND, %016lx parent directory found the filename %s \n", inum_p, target_name_str.c_str());
            inum_c = n2i(inum_str);
            flag_file_found = true;
            break;
        }
	}

    std::string buf2;
    if(flag_file_found == false)
    {
        //File does not exist, able to create new file.
        //inum new_inum = generate_inum(true); 
        inum new_inum = generate_inum(is_file); 
        inum_c = new_inum;
        extent_protocol::status e_ret;
        all_entry_str.append(filename(new_inum)+ yfs_client::SUB_DELIMITER + std::string(name) + yfs_client::DELIMITER );
        //printf("yfs_client::createfile::all_entry_str:%s\n",all_entry_str.c_str());
        printf("yfs_client::createfile_helper, new_inum: %016llx\n",new_inum);

        //add the new file
        std::string buf_newfile = "";
        //if(!is_file)
        //{
            //buf_newfile = 
        //}
        e_ret = ec->put(new_inum, buf_newfile, -1);
        printf("yfs_client::createfile_helper, ec-put, new_inum: %016llx, buf_newfile:%s\n", new_inum, buf_newfile.c_str());
        if(e_ret != extent_protocol::OK)
        {
            printf("yfs_client::createfile_helper IOERR 1 > ");
            return yfs_client::IOERR;
        }
        
        //change the parent folder info
        e_ret = ec->put(inum_p, all_entry_str, -1);
        printf("yfs_client::createfile_helper, ec-put, inum_p: %016llx, all_entry_str:%s\n", inum_p, all_entry_str.c_str());
        if( e_ret != extent_protocol::OK)
        {
            printf("yfs_client::createfile_helper IOERR 2> ");
            return yfs_client::IOERR;
        }
        return yfs_client::OK;
    }
    else
    {
      printf("yfs_client::createfile_helper %016lx parent directory exist, but file %s also exist > \n", inum_p, name);
      return yfs_client::EXIST;
    }
  }
  else
  {
      //Parent directory does not exist.
      printf("yfs_client::createfile_helper %016lx parent directory does not exist \n", inum_p);
      return yfs_client::NOENT;
  }
}

yfs_client::status
yfs_client::createfile(inum inum_p, const char* name, inum& inum_c)
{
    printf("yfs_cient::createfile\n");
    yfs_client::status ret =  createfile_helper(inum_p, name, inum_c, true);
    printf("yfs_client::createfile pass\n");
    return ret;
}

yfs_client::status
yfs_client::createfile(inum inum_p, const char* name, inum& inum_c, bool is_file)
{
    printf("yfs_cient::createfile\n");
    yfs_client::status ret =  createfile_helper(inum_p, name, inum_c, is_file);
    printf("yfs_client::createfile pass\n");
    return ret;
}

yfs_client::status
yfs_client::mkdir(inum inum_p, const char* name, inum &inum_c)
{
   printf("yfs_client::mkdir inum_p:%016lx, inum_c:%016lx, name:%s\n", inum_p, inum_c, name);
   yfs_client::status ret =  createfile_helper(inum_p, name, inum_c, false);
   printf("yfs_client::mkdir pass! inum_p:%016lx, inum_c:%016lx, name:%s\n", inum_p, inum_c, name);
   return ret;

}

yfs_client::status
yfs_client::createroot()
{
    printf("yfs_cient::createroot \n");
    yfs_client::inum inum_root = 0x00000001 & 0x000000007FFFFFFF;
    //printf("in createroot 1 \n");
    //std::string all_entry_str = filename(inum_root) + SUB_DELIMITER + std::string("root") + DELIMITER;
    std::string all_entry_str = "";
    //printf("in createroot 2 \n");
    extent_protocol::status flag = ec->put(inum_root, all_entry_str, -1);
    //printf("in createroot 3 \n");

    if(flag == extent_protocol::OK)
    {
        printf("yfs_client::createroot succeed \n");
        return yfs_client::OK;
    }
    else
    {
        printf("yfs_client::createroot failed \n");
        return yfs_client::IOERR;
    }
}

yfs_client::status 
yfs_client::setattr(inum inum, fileinfo& finfo)
{
  int r = OK;
  std::string file_buf;
  printf("yfs_client::setattr %016llx, finfo.size %lld \n", inum, finfo.size);
  if (ec->get(inum,0, SIZE_MAX, file_buf)!= extent_protocol::OK)
  {
      return yfs_client::IOERR;
  }
 
  unsigned long long new_size = finfo.size;
  if (new_size <= 0) 
     file_buf = "";
  else {
     file_buf.resize(new_size);
  }

  if (ec->put(inum,file_buf, new_size) != extent_protocol::OK) {
    return yfs_client::IOERR;
  }
  
  return yfs_client::OK; 
}
yfs_client::status
yfs_client::read(inum inu, off_t offset, size_t size, std::string &buf)
{
   printf("yfs_client::read %016lx, off %ld size %u \n", inu, offset, size);
   if (ec->get(inu, (int)offset, (unsigned int)size, buf) != extent_protocol::OK) {
       return yfs_client::IOERR;
   }
   printf("yfs_client::read %016lx, off %ld size %u, buff:%s \n", inu, offset, size, buf.c_str());
    
   return yfs_client::OK;
}

yfs_client::status
yfs_client::write(inum inu, off_t offset, size_t size, const char* buf)
{
   std::string buf_f;
   buf_f.append(buf, (unsigned int)size);
   printf("yfs_client::write %016lx off:%ld size:%u, buf:%s \n", inu, offset, size, buf_f.c_str());   
   if (ec->put(inu, buf_f, (int)offset) != extent_protocol::OK) {
      return yfs_client::IOERR;
   }

   return yfs_client::OK;
}

yfs_client::status
yfs_client::unlink(inum inum_p, const char* name)
{
    //printf("yfs_client::unlink, parent: %016llx, name:%s, start\n", inum_p, name);
    yfs_client::status ret;
    extent_protocol::status e_ret;
    

    //Check whether this name exist in parent
    bool found = false;
	std::string entry_str;
    std::string target_name_str(name);
    std::string new_all_entry_str = "";
    inum target_inum;

    std::string buf_p;
    e_ret = ec->get(inum_p, 0, SIZE_MAX, buf_p );
    printf("yfs_client::unlink, ec->get(%016llx,...), result buf_p: %s, ret:%d\n", inum_p, buf_p.c_str(), ret);
	std::stringstream all_entry_str_stream(buf_p);
    if (e_ret != extent_protocol::OK)
    {
        ret = yfs_client::NOENT;
        //return ret;
        goto release;
    }


    //printf("yfs_client::unlink, befor while loop: %016llx\n", inum_p );
    //segment : inum_str, name_str
	while(std::getline(all_entry_str_stream, entry_str, yfs_client::DELIMITER))
	{
        //printf("yfs_client::unlink, just in while loop: %016llx\n", inum_p );

	   //seglist.push_back(segment);
        std::string inum_str;
        std::string name_str;
        std::stringstream entry_str_stream(entry_str);
        std::getline(entry_str_stream, inum_str,yfs_client::SUB_DELIMITER);
        std::getline(entry_str_stream, name_str,yfs_client::SUB_DELIMITER);
        printf("yfs_client::unlink, search loop: parent:%016llx, name:%s, target_name_str:%s\n", 
                inum_p, name_str.c_str(), target_name_str.c_str());
        if (target_name_str == name_str)
        {
            target_inum = n2i(inum_str);
            printf("yfs_client::unlink, parent: %016llx, name:%s,target_inum:%016llx, FILE FOUND\n", 
                    inum_p, name_str.c_str(), target_inum);
            found = true;
        }
        else if(!inum_str.empty())
        {
            new_all_entry_str += entry_str + yfs_client::DELIMITER;
        }
	}
    if (found)
    {

        printf("yfs_client::unlink, Found target_inum: %016llx, name:%s \n", target_inum, target_name_str.c_str());
        if( isdir(target_inum) ) 
        {
            
            printf("yfs_client::unlink, is_dir, parent: %016llx,target_inum:%016llx, name:%s, it is dir! \n",
                    inum_p, target_inum, target_name_str.c_str());
            ret = yfs_client::IOERR;
            goto release;
        }
        printf("yfs_client::unlink, parent: %016llx, name:%s, FILE FOUND, before ec->put\n", inum_p, target_name_str.c_str());
        e_ret = ec->put(inum_p, new_all_entry_str, -1);
        if (e_ret !=extent_protocol::OK)
        {
            printf("yfs_client::unlink, ec->put error. parent: %016llx, new_all_entry_str:%s\n", inum_p, new_all_entry_str.c_str());
            ret = yfs_client::IOERR;
            goto release;
        }
        printf("yfs_client::unlink, parent: %016llx, new_all_entry_str:%s ,ec->put succeed, target_name_str:%s\n", inum_p,new_all_entry_str.c_str(), target_name_str.c_str());

        e_ret = ec->remove(target_inum);
        if (e_ret !=extent_protocol::OK)
        {
            printf("yfs_client::unlink, ec->remove error. parent: %016llx, new_all_entry_str:%s", inum_p, new_all_entry_str.c_str());
            ret = yfs_client::IOERR;
            goto release;
        }
        printf("yfs_client::unlink, parent: %016llx, name:%s, FILE FOUND\n,  ec->remove succeed", inum_p, target_name_str.c_str());


    }
    else
    {
        ret = yfs_client::NOENT;
        goto release;
    }

    ret = yfs_client::OK;

release:
    return ret;
}

