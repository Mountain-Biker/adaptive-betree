#include "backing_store.hpp"
#include <iostream>
#include <ext/stdio_filebuf.h>
#include <unistd.h>
#include <cassert>
#include <inttypes.h>
#include <fstream>
#include <cstdlib> 
#include <unistd.h> 

/////////////////////////////////////////////////////////////
// Implementation of the one_file_per_object_backing_store //
/////////////////////////////////////////////////////////////
one_file_per_object_backing_store::one_file_per_object_backing_store(std::string rt)
  : root(rt)
{}

//allocate space for a new version of an object
//requires that version be >> any previous version
//logic for this is now handled by the swap space
void one_file_per_object_backing_store::allocate(uint64_t obj_id, uint64_t version) {
  //uint64_t id = nextid++;
  std::string filename = get_filename(obj_id, version);
  std::fstream dummy(filename, std::fstream::out); 
  dummy.flush();

  assert(dummy.good());
  //return id;
}


//delete the file associated with an specific version of a node
void one_file_per_object_backing_store::deallocate(uint64_t obj_id, uint64_t version) {
  std::string filename = get_filename(obj_id, version);
  assert(unlink(filename.c_str()) == 0);
}

//return filestream corresponding to an item. Needed for deserialization.
std::iostream * one_file_per_object_backing_store::get(uint64_t obj_id, uint64_t version) {
  __gnu_cxx::stdio_filebuf<char> *fb = new __gnu_cxx::stdio_filebuf<char>;
  
  std::string filename = get_filename(obj_id, version);
  
  fb->open(filename, std::fstream::in | std::fstream::out);
  std::fstream *ios = new std::fstream;
  ios->std::ios::rdbuf(fb);
  ios->exceptions(std::fstream::badbit | std::fstream::failbit | std::fstream::eofbit);
  assert(ios->good());
  
  return ios;
}

//push changes from iostream and close.
void one_file_per_object_backing_store::put(std::iostream *ios)
{
  ios->flush();
  __gnu_cxx::stdio_filebuf<char> *fb = (__gnu_cxx::stdio_filebuf<char> *)ios->rdbuf();
  fsync(fb->fd());

  delete ios;
  delete fb;
}

// kosumi: filename
//Given an object and version, return the filename corresponding to it.
std::string one_file_per_object_backing_store::get_filename(uint64_t obj_id, uint64_t version){

  return root + "/" + std::to_string(obj_id) + "_" + std::to_string(version);

}
