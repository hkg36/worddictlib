#include "define.h"

int main(int argc,const char**argv)
{
  DbFileFinder finder("/app_data/chinese_decode/dbindex");
  const char* res=finder.findString("中华人民");
  printf("%s\n",res);
  return 0;
}