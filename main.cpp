#include <db.h>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string>
#include <boost/python.hpp>
using namespace boost::python;

int buildDict(const char* dictdb_path,const char* out_path,const int dbenv_open_flag);
class DbFileFinder {
private:
	int fd_index;
	int fd_value_index;
	char *addr_data;
	unsigned int *addr_index;
	unsigned int *addr_f_index;
	
	char *addr_value_data;
	unsigned int * addr_value_index;
	unsigned int * addr_f_value_index;
	//unsigned int *z
	size_t index_count;
	size_t value_index_count;
	struct stat sb_index,sb_value_index;

	inline char* getString(unsigned int index) {
	  return (addr_data + *(addr_index + index));
	}
	int last_foundpos;
public:
	DbFileFinder(const char* index_path);
	~DbFileFinder();
	void PrintAll();
	const char* findString(const char* findword);
	const char* lastFoundString();
	std::string lastFoundValue();
};
BOOST_PYTHON_MODULE(worddict)
{
    class_<DbFileFinder>("DbFileFinder",init<const char*>())
      .def("PrintAll",&DbFileFinder::PrintAll)
      .def("findString",&DbFileFinder::findString)
      .def("lastFoundString",&DbFileFinder::lastFoundString)
      .def("lastFoundValue",&DbFileFinder::lastFoundValue);
    def("buildDict",buildDict);
}

int buildDBFile(const char* dictdb_path,const char* datapath,const char* valuedatapath,const int dbenv_open_flag,
		std::vector<unsigned int> &poslist,std::vector<unsigned int> &valueposlist) {
    DB_ENV *env = NULL;
    int outfile=0;
    int outvaluefile=0;
    int res = 0;
    DBT key, value;
    DB *db = NULL;
    DBC* dbc = NULL;
    unsigned int last_pos=0;
    unsigned int last_valuepos=0;
    
    memset(&key, 0, sizeof(key));
    memset(&value, 0, sizeof(value));
    
    res = db_env_create(&env, 0);
    if(res)
      goto SHUTDOWN;
    res = env->open(env, dictdb_path,
		    dbenv_open_flag, 0);
    if(res)
    {
      printf("env open error %s\n",db_strerror(res));
      goto SHUTDOWN;
    }
    
    res = db_create(&db, env, 0);
    if(res)
      goto SHUTDOWN;
    res = db->open(db, 0, "maindb.db", "main", DB_BTREE, DB_RDONLY, 0666);
    if(res)
    {
       printf("db open error %s\n",db_strerror(res));
       goto SHUTDOWN;
    }

    memset(&key,0,sizeof(key));
    memset(&value,0,sizeof(value));
    key.flags = DB_DBT_REALLOC;
    value.flags = DB_DBT_REALLOC;

    res = db->cursor(db, NULL, &dbc, 0);
    if(res)
    {
       printf("cursor open error %s\n",db_strerror(res));
      goto SHUTDOWN;
    }
    unlink(datapath);
    unlink(valuedatapath);
    outfile = open(datapath, O_RDWR | O_CREAT, 0666);
    if(outfile==0)
    {
      res=errno;
      goto SHUTDOWN;
    }
    outvaluefile = open(valuedatapath,O_RDWR|O_CREAT,0666);
    if(outvaluefile==0)
    {
      res=errno;
      goto SHUTDOWN;
    }
    
    while (true) {
	res=dbc->get(dbc, &key, &value, DB_NEXT);
	if(res)
	{
	  printf("cursor next fail %s\n",db_strerror(res));
	  res=0;
	  break;
	}
	 else
	{
	  poslist.push_back(last_pos);
	  size_t wd=write(outfile, key.data, key.size);
	  last_pos+=wd;
	  wd=write(outfile, "\0", 1);
	  last_pos+=wd;
	  
	  valueposlist.push_back(last_valuepos);
	  wd=write(outvaluefile,value.data,value.size);
	  last_valuepos+=wd;
	}
    }
    valueposlist.push_back(last_valuepos);
SHUTDOWN:
    if(outfile)
      close(outfile);
    if(outvaluefile)
      close(outvaluefile);
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
int writeDBIndex(const char* indexpath,const char* datapath,std::vector<unsigned int> &poslist) {
  int fd=0;
  int fdData=0;
  int res=0;
  struct stat sb;

  unlink(indexpath);
  fd = open(indexpath, O_RDWR | O_CREAT, 0666);
  fdData = open(datapath,O_RDONLY,0666);
  if(fd && fdData)
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
      
      char buffer[1024];
      while(true)
      {
	int rcount=read(fdData,buffer,sizeof(buffer));
	if(rcount==0)
	  break;
	else if(rcount==-1)
	{
	  printf("read data fail %d\n",errno);
	  break;
	}
	int wcount= write(fd,buffer,rcount);
	if(wcount!=rcount)
	{
	  if(wcount==-1)
	    printf("write data fail %d\n",errno);
	  else
	    printf("only write %d byte\n",wcount);
	  break;
	}
      }
    }
    res = close(fd);
    res = close(fdData);
  }
  return 0;
}

int buildDict(const char* dictdb_path,const char* out_path,const int dbenv_open_flag)
{
  mkdir(out_path,0777);
  char out_data_path[256];
  char out_index_path[256];
  char out_value_data_path[256];
  char out_value_index_path[256];
  strcpy(out_data_path,out_path);
  strcpy(out_index_path,out_path);
  strcpy(out_value_data_path,out_path);
  strcpy(out_value_index_path,out_path);
  strcat(out_data_path,"/data");
  strcat(out_index_path,"/index");
  strcat(out_value_data_path,"/value_data");
  strcat(out_value_index_path,"/value_index");
  std::vector<unsigned int> poslist;
  std::vector<unsigned int> valueposlist;
  if(buildDBFile(dictdb_path,out_data_path,out_value_data_path,dbenv_open_flag,poslist,valueposlist))
    return 1;
  printf("buildDBFile fin\n");
  if(writeDBIndex(out_index_path,out_data_path,poslist))
    return 2;
  printf("writeDBIndex index fin\n");
  if(writeDBIndex(out_value_index_path,out_value_data_path,valueposlist))
    return 3;
  printf("writeDBIndex value fin\n");
  unlink(out_data_path);
  unlink(out_value_data_path);
  return 0;
}

DbFileFinder::DbFileFinder(const char* index_path) {
  int res;
  char out_index_path[256];
  char out_value_index_path[256];
  strcpy(out_index_path,index_path);
  strcpy(out_value_index_path,index_path);
  strcat(out_index_path,"/index");
  strcat(out_value_index_path,"/value_index");
  
  fd_index = open(out_index_path, O_RDONLY);
  if(fd_index==-1)
  {
    printf("open indexfile fail %d\n",errno);
    return;
  }
  res = fstat(fd_index, &sb_index);
  printf("index size= %d\n",sb_index.st_size);
  addr_f_index = (unsigned int*) mmap(NULL, sb_index.st_size, PROT_READ,
		  MAP_SHARED, fd_index, 0);
  if(addr_f_index==MAP_FAILED)
  {
    printf("map index fail %d\n",errno);
    return;
  }
  index_count = *addr_f_index;
  addr_index = addr_f_index + 1;
  addr_data=(char*)(addr_index+index_count);
  
  fd_value_index = open(out_value_index_path,O_RDONLY);
  if(fd_value_index==-1)
  {
    printf("open valueindexfile fail %d\n",errno);
    return;
  }
  res=fstat(fd_value_index,&sb_value_index);
  printf("value size=%d\n",sb_value_index.st_size);
  addr_f_value_index=(unsigned int*)mmap(NULL,sb_value_index.st_size,PROT_READ,MAP_SHARED,fd_value_index,0);
  if(addr_f_value_index==MAP_FAILED)
  {
    printf("map valueindex fail %d\n",errno);
    return;
  }
  value_index_count=*addr_f_value_index;
  addr_value_index=addr_f_value_index+1;
  addr_value_data=(char*)(addr_value_index+value_index_count);
}
DbFileFinder::~DbFileFinder() {
  int res;
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
int starts_with(const char * string, const char * prefix)
{
  if(string==NULL || prefix==NULL)
    return 0;
    while(*prefix)
    {
        if(*prefix != *string)
            return 0;
	prefix++;
	string++;
    }

    return 1;
}
const char* DbFileFinder::findString(const char* findword) {
  unsigned int head = 0;
  unsigned int tail = index_count - 1;
  last_foundpos=-1;
  while (true) {
    if (tail<head)
      return NULL;
    if (head == tail) {
      const char* pre=NULL;
      if(head>0)
	pre=getString(head-1);
      const char* aft=NULL;
      if(head<index_count-1)
	aft=getString(head+1);
      const char* imd=getString(head);
      if(starts_with(imd, findword))
      {
	last_foundpos=head;
	return imd;
      }
      else if(starts_with(aft, findword))
      {
	last_foundpos=head+1;
	return aft;
      }
      else if(starts_with(pre, findword))
      {
	last_foundpos=head-1;
	return pre;
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
const char* DbFileFinder::lastFoundString()
{
  if(last_foundpos==-1)
    return NULL;
  else
    return getString(last_foundpos);
}
std::string DbFileFinder::lastFoundValue()
{
  if(last_foundpos==-1)
    return "";
  else
  {
    unsigned int startpos=*(addr_value_index+last_foundpos);
    unsigned int endpos=*(addr_value_index+last_foundpos+1);
    char *startaddr=(addr_value_data + startpos);
    return std::string(startaddr,endpos-startpos);
  }
}