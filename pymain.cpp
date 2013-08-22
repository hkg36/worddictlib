#include <Python.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <algorithm>
#include <vector>

typedef struct {
    PyObject_HEAD
    int fd_index;
    int fd_value_index;
    char *addr_data;
    unsigned int *addr_index;
    unsigned int *addr_f_index;
    
    char *addr_value_data;
    unsigned int * addr_value_index;
    unsigned int * addr_f_value_index;
    size_t index_count;
    size_t value_index_count;
    struct stat sb_index,sb_value_index;
    int last_foundpos;
} DbFileFinder;

static PyObject *DbFileFinder_new(PyTypeObject *type, PyObject *args, PyObject *kw) {
    DbFileFinder *self = (DbFileFinder *)type->tp_alloc(type, 0);
    self->fd_index=0;
    self->fd_value_index=0;
    self->addr_data=nullptr;
    self->addr_index=nullptr;
    self->addr_f_index=nullptr;
    self->addr_value_data=nullptr;
    self->addr_value_index=nullptr;
    self->addr_f_value_index=nullptr;
    self->index_count=0;
    self->value_index_count=0;
    self->last_foundpos=-1;
    memset(&self->sb_index,0,sizeof(self->sb_index));
    memset(&self->sb_value_index,0,sizeof(self->sb_value_index));
    return (PyObject *)self;
}
static void DbFileFinder_dealloc(DbFileFinder *self) {
  int res;
  if (self->addr_f_index)
    res = munmap(self->addr_f_index, self->sb_index.st_size);
  res = close(self->fd_index);
  if(self->addr_f_value_index)
    res =munmap(self->addr_f_value_index,self-> sb_value_index.st_size);
  res=close(self->fd_value_index);
  self->ob_type->tp_free(self);
}

static int DbFileFinder_init(DbFileFinder *self, PyObject *args) {
    char* index_path=nullptr;
    if (!PyArg_ParseTuple(args, "s", &index_path)) {
        return -1;
    }
    
    int res;
    char out_index_path[256];
    char out_value_index_path[256];
    strcpy(out_index_path,index_path);
    strcpy(out_value_index_path,index_path);
    strcat(out_index_path,"/index");
    strcat(out_value_index_path,"/value_index");

    self->fd_index = open(out_index_path, O_RDONLY);
    if(self->fd_index==-1)
    {
      self->fd_index=0;
      printf("open indexfile fail %d\n",errno);
      return 1;
    }
    res = fstat(self->fd_index, &self->sb_index);
    printf("index size= %d\n",self->sb_index.st_size);
    self->addr_f_index = (unsigned int*) mmap(NULL,self-> sb_index.st_size, PROT_READ,
		    MAP_SHARED,self-> fd_index, 0);
    if(self->addr_f_index==MAP_FAILED)
    {
      self->addr_f_index=nullptr;
      printf("map index fail %d\n",errno);
      return 1;
    }
    self->index_count = *self->addr_f_index;
    self->addr_index = self->addr_f_index + 1;
    self->addr_data=(char*)(self->addr_index+self->index_count);

    self->fd_value_index = open(out_value_index_path,O_RDONLY);
    if(self->fd_value_index==-1)
    {
      self->fd_value_index=0;
      printf("open valueindexfile fail %d\n",errno);
      return 1;
    }
    res=fstat(self->fd_value_index,&self->sb_value_index);
    printf("value size=%d\n",self->sb_value_index.st_size);
    self->addr_f_value_index=(unsigned int*)mmap(NULL,self->sb_value_index.st_size,PROT_READ,MAP_SHARED,self->fd_value_index,0);
    if(self->addr_f_value_index==MAP_FAILED)
    {
      self->addr_f_value_index=nullptr;
      printf("map valueindex fail %d\n",errno);
      return 1;
    }
    self->value_index_count=*self->addr_f_value_index;
    self->addr_value_index=self->addr_f_value_index+1;
    self->addr_value_data=(char*)(self->addr_value_index+self->value_index_count);
  
    return 0;
}
inline char* getString(DbFileFinder *self,unsigned int index) {
  return (self->addr_data + *(self->addr_index + index));
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
static PyObject* DbFileFinder_findString(DbFileFinder *self, PyObject *args)
{
  char* findword=nullptr;
  if (!PyArg_ParseTuple(args, "s", &findword)) {
    Py_RETURN_NONE;
  }
  unsigned int head = 0;
  unsigned int tail = self->index_count - 1;
  self->last_foundpos=-1;
  while (true) {
    if (tail<head)
      Py_RETURN_NONE;
    if (head == tail) {
      const char* pre=NULL;
      if(head>0)
	pre=getString(self,head-1);
      const char* aft=NULL;
      if(head<self->index_count-1)
	aft=getString(self,head+1);
      const char* imd=getString(self,head);
      if(starts_with(imd, findword))
      {
	 self->last_foundpos=head;
	return Py_BuildValue("s",imd);
      }
      else if(starts_with(aft, findword))
      {
	 self->last_foundpos=head+1;
	return Py_BuildValue("s",aft);
      }
      else if(starts_with(pre, findword))
      {
	 self->last_foundpos=head-1;
	return Py_BuildValue("s",pre);
      } 
      Py_RETURN_NONE;
    }
    unsigned int mid = (tail - head) / 2 + head;
    const char *midstr = getString(self,mid);
    int res = strcmp(midstr, findword);
    if (res == 0)
    {
      self->last_foundpos=mid;
      return Py_BuildValue("s",midstr);
    }
    else if (res <0) {
      head = mid + 1;
    } else if (res >0) {
      tail = mid - 1;
    }
  }
}
static PyObject* DbFileFinder_lastFoundValue(DbFileFinder *self)
{
  if(self->last_foundpos==-1)
    Py_RETURN_NONE;
  else
  {
    unsigned int startpos=*(self->addr_value_index+self->last_foundpos);
    unsigned int endpos=*(self->addr_value_index+self->last_foundpos+1);
    char *startaddr=(self->addr_value_data + startpos);
    return Py_BuildValue("s#",startaddr,endpos-startpos);
  }
}
static PyObject* DbFileFinder_lastFoundString(DbFileFinder *self)
{
  if(self->last_foundpos==-1)
    Py_RETURN_NONE;
  else
  {
    return Py_BuildValue("s",getString(self,self->last_foundpos));
  }
}
PyMethodDef DbFileFinder_methods[] = {
    { "findString", (PyCFunction)DbFileFinder_findString, METH_VARARGS,
          "find string in dict" },
    {"lastFoundValue",(PyCFunction)DbFileFinder_lastFoundValue,0,
	  "get value of last found key"},
    {"lastFoundString",(PyCFunction)DbFileFinder_lastFoundString,0,
	  "get last found key"},
    { NULL, NULL, 0, NULL }
};
static PyTypeObject DbFileFinderType = {
    PyObject_HEAD_INIT(NULL)
    0,                             /* ob_size */
    "worddict2.DbFileFinder",             /* tp_name */
    sizeof(DbFileFinder), /* tp_basicsize */
    0,                             /* tp_itemsize */
    (destructor)DbFileFinder_dealloc,   /* tp_dealloc */
    0,                             /* tp_print */
    0,                             /* tp_getattr */
    0,                             /* tp_setattr */
    0,                             /* tp_compare */
    0,                             /* tp_repr */
    0,                             /* tp_as_number */
    0,                             /* tp_as_sequence */
    0,                             /* tp_as_mapping */
    0,                             /* tp_hash */
    0,                             /* tp_call */
    0,                             /* tp_str */
    0,                             /* tp_getattro */
    0,                             /* tp_setattro */
    0,                             /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,            /* tp_flags */
    "DbFileFinder.",    /* tp_doc */
    0,                             /* tp_traverse */
    0,                             /* tp_clear */
    0,                             /* tp_richcompare */
    0,                             /* tp_weaklistoffset */
    0,                             /* tp_iter */
    0,                             /* tp_iternext */
    DbFileFinder_methods,               /* tp_methods */
    0,                             /* tp_members */
    0,                             /* tp_getset */
    0,                             /* tp_base */
    0,                             /* tp_dict */
    0,                             /* tp_descr_get */
    0,                             /* tp_descr_set */
    0,                             /* tp_dictoffset */
    (initproc)DbFileFinder_init,        /* tp_init */
    0,                             /* tp_alloc */
    DbFileFinder_new,                   /* tp_new */
    0,                             /* tp_free */
};
static PyObject* version(PyObject* Self, PyObject* Argvs)
{
  return Py_BuildValue("iii",1,1,0);
}
struct KVCell
{
  const char* key;
  const char* value;
  size_t value_size;
  KVCell(const char* k,const char* v,size_t v_len)
  {
    key=k;
    value=v;
    value_size=v_len;
  }
};
struct KVCellCmp
{
  bool operator ()(const KVCell &a,const KVCell &b)
  {
    return strcmp(a.key,b.key)<0;
  }
};
int buildDBFile(const char* datapath,const char* valuedatapath,std::vector<KVCell> &kvlist,
		std::vector<unsigned int> &poslist,std::vector<unsigned int> &valueposlist);
int writeDBIndex(const char* indexpath,const char* datapath,std::vector<unsigned int> &poslist);
static PyObject* buildDict(PyObject* Self, PyObject* args)
{
  const char* out_path=NULL;
  PyObject * listObj=NULL;
  if (! PyArg_ParseTuple( args, "sO!", &out_path,&PyList_Type, &listObj)) return NULL;
  std::vector<KVCell> kvlist;
  size_t lsize= PyList_Size(listObj);
  printf("list size:%u\n",lsize);
  for(size_t i=0;i<lsize;i++)
  {
    PyObject* pair = PyList_GetItem(listObj, i);
    if(pair->ob_type!=&PyTuple_Type)
    {
      PyErr_SetString(PyExc_TypeError,"list item not a Tuple");
      return NULL;
    }
    if(PyTuple_Size(pair)<2)
    {
      PyErr_SetString(PyExc_RuntimeError,"size of tuple object must bigger then 2");
      return NULL;
    }
    const char* key=NULL;
    const char* value=NULL;
    size_t value_size=0;
    key=PyString_AsString(PyTuple_GetItem(pair,0));
    if(key==NULL)
      return NULL;
    value=PyString_AsString(PyTuple_GetItem(pair,1));
    if(value==NULL)
      return NULL;
    value_size=PyString_Size(PyTuple_GetItem(pair,1));
    kvlist.push_back(KVCell(key,value,value_size));
  }
  
  std::sort(kvlist.begin(),kvlist.end(),KVCellCmp());
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
  if(buildDBFile(out_data_path,out_value_data_path,kvlist,poslist,valueposlist))
    return Py_BuildValue("i",1);
  printf("buildDBFile fin\n");
  if(writeDBIndex(out_index_path,out_data_path,poslist))
    return Py_BuildValue("i",2);
  printf("writeDBIndex index fin\n");
  if(writeDBIndex(out_value_index_path,out_value_data_path,valueposlist))
    return Py_BuildValue("i",3);
  printf("writeDBIndex value fin\n");
  unlink(out_data_path);
  unlink(out_value_data_path);
  
  return Py_BuildValue("i",0);
}

int buildDBFile(const char* datapath,const char* valuedatapath,std::vector<KVCell> &kvlist,
		std::vector<unsigned int> &poslist,std::vector<unsigned int> &valueposlist) {
    int outfile=0;
    int outvaluefile=0;
    int res = 0;
    unsigned int last_pos=0;
    unsigned int last_valuepos=0;
    
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
    
    for(std::vector<KVCell>::iterator i=kvlist.begin();i!=kvlist.end();i++){
	poslist.push_back(last_pos);
	size_t wd=write(outfile, i->key,strlen(i->key));
	last_pos+=wd;
	wd=write(outfile, "\0", 1);
	last_pos+=wd;
	
	valueposlist.push_back(last_valuepos);
	wd=write(outvaluefile,i->value,i->value_size);
	last_valuepos+=wd;
    }
    valueposlist.push_back(last_valuepos);
SHUTDOWN:
    if(outfile)
      close(outfile);
    if(outvaluefile)
      close(outvaluefile);
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

static PyMethodDef root_methods[] = {
  {"version", version, METH_VARARGS, "get version"},
  {"buildDict",buildDict,METH_VARARGS,"build dict file"},
    { NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC initworddict2() {
    PyObject *m;
    if (PyType_Ready(&DbFileFinderType) < 0) {
        return;
    }
    m = Py_InitModule3("worddict2", root_methods, "worddict2 module.");
    Py_INCREF(&DbFileFinderType);
    PyModule_AddObject(m, "DbFileFinder", (PyObject *)&DbFileFinderType);
}