#include "define.h"

#include <boost/python.hpp>
using namespace boost::python;

BOOST_PYTHON_MODULE(worddict)
{
    class_<DbFileFinder>("DbFileFinder",init<const char*>())
      .def("PrintAll",&DbFileFinder::PrintAll)
      .def("findString",&DbFileFinder::findString)
      .def("lastFoundString",&DbFileFinder::lastFoundString)
      .def("lastFoundValue",&DbFileFinder::lastFoundValue);
    def("buildDict",buildDict);
}