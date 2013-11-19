#ifndef yfs_client_h
#define yfs_client_h
#define SIZE_MAX ((unsigned int)(-1))

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include "lock_client.h"
#include <vector>
//#include <climits>
#include "lock_protocol.h"
#include "lock_client_cache.h"

class lock_release_user_impl : public lock_release_user
{
  private:
    class extent_client* ec;
  public:
    lock_release_user_impl(extent_client* ec);
    virtual ~lock_release_user_impl(){};
    void dorelease(lock_protocol::lockid_t lid);
};

class yfs_client {
  extent_client *ec;
  //lock_client *lc;
  lock_client_cache *lc;
  lock_release_user_impl *lu;
  static const char DELIMITER; 
  static const char SUB_DELIMITER; 
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  static inum generate_inum(bool is_file);
  static inum generate_inum(bool is_file, inum inum_p);
  status createfile_helper(inum inum_p, const char* name, inum& inum_c, bool isfile);
 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  status lookup(inum inum_p, const char* name, inum& inum_c);
  status createfile(inum inum_p, const char* name, inum& inum_c);
  status createroot();
  status readdir(inum inum_p,std::vector<dirent>& dir_entries);
  status setattr(inum inum, fileinfo& finfo);
  status read(inum inu, int offset, long size, std::string& buf);
  status write(inum inu, int offset, long size, const char* buf);

  status mkdir(inum inum_p, const char* name, inum &inum_c);
  status createfile(inum inum_p, const char* name, inum &inum_c, bool is_file);
  status unlink(inum inum_p, const char* name);

  status getattr(inum inu, extent_protocol::attr& a);

};
#endif 
