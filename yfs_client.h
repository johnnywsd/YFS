#ifndef yfs_client_h
#define yfs_client_h
#define SIZE_MAX ((unsigned int)(-1))

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include "lock_client.h"
#include <vector>
//#include <climits>


class yfs_client {
  extent_client *ec;
  lock_client *lcc;
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
  status read(inum inu, off_t offset, size_t size, std::string& buf);
  status write(inum inu, off_t offset, size_t size, const char* buf);


};
#endif 
