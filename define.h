#include <db.h>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string>
#include <string.h>
#include <stdlib.h>

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