#include <boost/python.hpp>
using namespace boost::python;

#include <stdio.h>
#include <string.h>
#include <db.h>
#include <string>
#include <vector>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

bool buildDict(const char* dictdb_path,const char* out_data_path,const char* out_index_path);
class DbFileFinder {
private:
	int fd_data;
	int fd_index;
	char *addr_data;
	unsigned int *addr_index;
	unsigned int *addr_f_index;
	//unsigned int *z
	size_t index_count;
	struct stat sb_data, sb_index;

	inline char* getString(unsigned int index) {
		return (addr_data + *(addr_index + index));
	}
public:
	DbFileFinder(const char* out_data_path,const char* out_index_path);
	~DbFileFinder();
	void PrintAll();
	const char* findString(const char* findword);
};
BOOST_PYTHON_MODULE(worddict)
{
    class_<DbFileFinder>("DbFileFinder",init<const char*,const char*>())
      .def("PrintAll",&DbFileFinder::PrintAll)
      .def("findString",&DbFileFinder::findString);
    def("buildDict",buildDict);
}

int buildDBFile(const char* dictdb_path,const char* listpath) {
    DB_ENV *env = NULL;
    int outfile=0;
    int res = 0;
    DBT key, value;
    DB *db = NULL;
    DBC* dbc = NULL;
    
    memset(&key, 0, sizeof(key));
    memset(&value, 0, sizeof(value));
    
    res = db_env_create(&env, 0);
    if(res)
      goto SHUTDOWN;
    res = env->open(env, dictdb_path,
		    DB_CREATE | DB_INIT_MPOOL | DB_INIT_CDB, 0);
    if(res)
      goto SHUTDOWN;
    
    res = db_create(&db, env, 0);
    if(res)
      goto SHUTDOWN;
    res = db->open(db, 0, "maindb.db", "main", DB_BTREE, DB_RDONLY, 0666);
    if(res)
      goto SHUTDOWN;

    key.flags = DB_DBT_REALLOC;
    value.flags = DB_DBT_REALLOC;

    res = db->cursor(db, NULL, &dbc, 0);
    if(res)
      goto SHUTDOWN;
    unlink(listpath);
    outfile = open(listpath, O_RDWR | O_CREAT, 0666);
    if(outfile==0)
    {
      res=errno;
      goto SHUTDOWN;
    }
    while (dbc->get(dbc, &key, &value, DB_NEXT) == 0) {
	write(outfile, key.data, key.size);
	write(outfile, "\0", 1);
    }
SHUTDOWN:
    if(outfile)
      close(outfile);
    if(dbc)
      dbc->close(dbc);
    if(key.data)
      free(key.data);
    if(value.data)
      free(value.data);
    if(db)
      db->close(db, 0);
    if(env)
      env->close(env, 0);
    return res;
}
int buildDBIndex(const char* listpath,const char* indexpath) {
  int fd=0;
  char *addr=NULL;
  int res=0;
  struct stat sb;
  
  std::vector<unsigned int> poslist;
  fd = open(listpath, O_RDONLY);
  if(fd)
  {
    res = fstat(fd, &sb);
    addr = (char*) mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (addr) {
      char *word_head = addr;
      for (char *faddr = addr, *fend = addr + sb.st_size; faddr < fend; faddr++) {
	if (*faddr == '\0') {
	  poslist.push_back(word_head - addr);
	  word_head = faddr + 1;
	}
      }
      res = munmap(addr, sb.st_size);
    }
    res = close(fd);
  }
  
  unlink(indexpath);
  fd = open(indexpath, O_RDWR | O_CREAT, 0666);
  if(fd)
  {
    unsigned int index_count = poslist.size();
    unsigned int *index_list = (unsigned int *) malloc(
		    sizeof(unsigned int) * index_count);
    if(index_list)
    {
      for (unsigned int i = 0; i < index_count; i++) {
	*(index_list + i) = poslist[i];
      }
      res = write(fd, &index_count, sizeof(index_count));
      res = write(fd, index_list, sizeof(unsigned int) * index_count);
      free(index_list);
    }
    res = close(fd);
  }
  return 0;
}
bool buildDict(const char* dictdb_path,const char* out_data_path,const char* out_index_path)
{
  if(buildDBFile(dictdb_path,out_data_path))
    return false;
  if(buildDBIndex(out_data_path,out_index_path))
    return false;
  return true;
}

DbFileFinder::DbFileFinder(const char* out_data_path,const char* out_index_path) {
  int res;

  fd_data = open(out_data_path, O_RDONLY);
  res = fstat(fd_data, &sb_data);
  addr_data = (char*) mmap(NULL, sb_data.st_size, PROT_READ, MAP_SHARED,
		  fd_data, 0);

  fd_index = open(out_index_path, O_RDONLY);
  res = errno;
  res = fstat(fd_data, &sb_index);
  addr_f_index = (unsigned int*) mmap(NULL, sb_index.st_size, PROT_READ,
		  MAP_SHARED, fd_index, 0);
  index_count = *addr_f_index;
  addr_index = addr_f_index + 1;
}
DbFileFinder::~DbFileFinder() {
  int res;
  if (addr_data) {
    res = munmap(addr_data, sb_data.st_size);
  }
  res = close(fd_data);

  if (addr_index)
    res = munmap(addr_f_index, sb_index.st_size);
  res = close(fd_index);
}

void DbFileFinder::PrintAll() {
  for (unsigned int i = 0; i < index_count; i++) {
    const char *pos = getString(i);
    printf("%s\n", pos);
  }
}
const char* DbFileFinder::findString(const char* findword) {
  unsigned int head = 0;
  unsigned int tail = index_count - 1;
  while (true) {
    if (head == tail) {
      if (head>0)
      {
	if(getString(head+1) == strstr(getString(head+1), findword))
	  return getString(head+1);
	else if(getString(head-1) == strstr(getString(head-1), findword))
	  return getString(head-1);
      }
      return NULL;
    }
    unsigned int mid = (tail - head) / 2 + head;
    const char *midstr = getString(mid);
    int res = strcmp(midstr, findword);
    if (res == 0)
      return midstr;
    else if (res <0) {
      head = mid + 1;
    } else if (res >0) {
      tail = mid - 1;
    }
  }
}