#include "swap_space.hpp"


//Methods to serialize/deserialize different kinds of objects.
//You shouldn't need to touch these.
void serialize(std::iostream &fs, serialization_context &context, uint64_t x)
{
  fs << x << " ";
  assert(fs.good());
}

void deserialize(std::iostream &fs, serialization_context &context, uint64_t &x)
{
  fs >> x;
  assert(fs.good());
}

void serialize(std::iostream &fs, serialization_context &context, int64_t x)
{
  fs << x << " ";
  assert(fs.good());
}

void deserialize(std::iostream &fs, serialization_context &context, int64_t &x)
{
  fs >> x;
  assert(fs.good());
}

// kosumi: serialization of (size, val)
void serialize(std::iostream &fs, serialization_context &context, std::string x)
{
  fs << x.size() << ",";
  assert(fs.good());
  fs.write(x.data(), x.size());
  assert(fs.good());
}

void deserialize(std::iostream &fs, serialization_context &context, std::string &x)
{
  size_t length;
  char comma;
  fs >> length >> comma;
  assert(fs.good());
  char *buf = new char[length];
  assert(buf);
  fs.read(buf, length);
  assert(fs.good());
  x = std::string(buf, length);
  delete buf;
}

bool swap_space::cmp_by_last_access(swap_space::object *a, swap_space::object *b) {
  return a->last_access < b->last_access;
}

swap_space::swap_space(backing_store *bs, uint64_t n) :
  backstore(bs),
  max_in_memory_objects(n),
  objects(),
  lru_pqueue(cmp_by_last_access) // Ang: pass a function cmp_by_last_access() to the set lru_pqueue which makes this set a priority queue;
{}

//construct a new object. Called by ss->allocate() via pointer<Referent> construction
//Does not insert into objects table - that's handled by pointer<Referent>()
swap_space::object::object(swap_space *sspace, serializable * tgt) {
  target = tgt;
  id = sspace->next_id++;
  version = 0;
  is_leaf = false;
  refcount = 1;
  last_access = sspace->next_access_time++;
  target_is_dirty = true;
  pincount = 0;
}

swap_space::object::object(){
  target = nullptr;
  id = -1; // object id 
  version = -1;
  is_leaf = false;
  refcount = -1;
  last_access = -1;
  target_is_dirty = false;
  pincount = -1;
}

//set # of items that can live in ss.
void swap_space::set_cache_size(uint64_t sz) {
  assert(sz > 0);
  max_in_memory_objects = sz;
  maybe_evict_something();
}

//write an object that lives on disk back to disk
//only triggers a write if the object is "dirty" (target_is_dirty == true)
void swap_space::write_back(swap_space::object *obj)
{
  // std::cout << "In write_back(), obj->id: " << obj->id << std::endl;
  assert(objects.count(obj->id) > 0);

  debug(std::cout << "Writing back " << obj->id
	<< " (" << obj->target << ") "
	<< "with last access time " << obj->last_access << std::endl);

  // This calls _serialize on all the pointers in this object,
  // which keeps refcounts right later on when we delete them all.
  // In the future, we may also use this to implement in-memory
  // evictions, i.e. where we first "evict" an object by
  // compressing it and keeping the compressed version in memory.
  serialization_context ctxt(*this);
  std::stringstream sstream;
  serialize(sstream, ctxt, *obj->target);
  obj->is_leaf = ctxt.is_leaf;

  if (obj->target_is_dirty) {
    std::string buffer = sstream.str();

    //modification - ss now controls BSID - split into unique id and version.
    //version increments linearly based uniquely on this version counter.

    uint64_t new_version_id = obj->version+1;

    backstore->allocate(obj->id, new_version_id);
    std::iostream *out = backstore->get(obj->id, new_version_id);
    // printf("In write_back function:\n");
    // printf("obj_id: %" PRIu64 "\n", obj->id);
    // printf("version: %" PRIu64 "\n", new_version_id);
    out->write(buffer.data(), buffer.length());
    backstore->put(out);


    // version 0 is the flag that the object exists only in memory.
    // if (obj->version > 0)
    //   backstore->deallocate(obj->id, obj->version);
    obj->version = new_version_id;
    obj->target_is_dirty = false;
  }
}


//attempt to evict an unused object from the swap space
//objects in swap space are referenced in a priority queue
//pull objects with low counts first to try and find an object with pincount 0.
void swap_space::maybe_evict_something(void)
{
  while (current_in_memory_objects > max_in_memory_objects) {
    object *obj = NULL;
    for (auto it = lru_pqueue.begin(); it != lru_pqueue.end(); ++it)
      if ((*it)->pincount == 0) {
        obj = *it;
        break;
      }
      
    if (obj == NULL)
      return;
    lru_pqueue.erase(obj);

    write_back(obj);
    
    delete obj->target; // obj->target is a serializable pointer, set this to NULL means this object is not in memory;
    obj->target = NULL;
    current_in_memory_objects--;
  }
}

// Ang : copy a file from sourcePath to destinationPath
bool swap_space::copy_file(std::string sourcePath, std::string destinationPath) {
    std::ifstream sourceFile(sourcePath, std::ios::binary);
    std::ofstream destinationFile(destinationPath, std::ios::binary);

    if (!sourceFile || !destinationFile) {
        return false; // Error in opening source or destination file
    }

    destinationFile << sourceFile.rdbuf();
    return true;
}


// kosumi: modify swap_space::maybe_evict_something to flush the whole tree
// Ang : flush the in memory betree nodes to disk, the node files can be found at destinationDirectory
// in this project the path of destinationDirectory is tmpdir_backup
void swap_space::flush_whole_tree(std::string destinationDirectory) {
  object *obj = NULL;
  // std::cout << "In flush whole tree()" << std::endl;
  // print_objects_id();
  // print_lru_pqueue_id();

  for (auto it = lru_pqueue.begin(); it != lru_pqueue.end(); ++it) {
    obj = *it;  
    if (obj == NULL){
      return;
    }
      
    // Ang :Cannot erase object in the for loop, it will change the size of lru_pqueue
    // and lead to segment fault;
    // lru_pqueue.erase(obj); 
    write_back(obj);

    //Ang: I need to copy the node file from tmpdir to tmpdir_backup, 
    // as I found for some unknow reason some of the node files stored in the tmpdir directory will disappear after the whole program finish
    // when I used gdb to debug this problem I found some destructor functions were called before the main() exit
    // this might be the reason.
    std::string sourcePath = backstore->get_filename(obj->id, obj->version);
    std::string destinationPath = destinationDirectory + "/" + std::to_string(obj->id) + "_" + std::to_string(obj->version);
    copy_file(sourcePath, destinationPath);

    delete obj->target; // Ang : obj->target is a serializable pointer, set this to NULL means this object is not in memory;
    obj->target = NULL;
    current_in_memory_objects--;
  
  }
  // clear all the entries in lru_pqueue;
  lru_pqueue.clear();
}

// the root node of betree should be the node with the largest object->id
// because when the root of betree split it will get a new object->id which is bigger than previous nodes
// std::string swap_space::get_betree_root_name(uint64_t root_id){
//   std::cout << "In swap_space::get_betree_root_name()" << std::endl;
//   uint64_t root_version = 0;

//   // object *obj = NULL;
//   for (auto it = objects.begin(); it != objects.end(); ++it) {
//     if (it->first == root_id) {
//       root_version = it->second->version;
//     }
//   }

//   std::cout << "root id: " << std::to_string(root_id) << std::endl;
//   std::cout << "root version: " << std::to_string(root_version) << std::endl;

//   return std::to_string(root_id) + "_" + std::to_string(root_version);
  
// }

// flush swap_space.objects from memory to disk
void swap_space::serialize_objects(std::string filePath) {
  if (filePath.empty()) {
    filePath = "ss_objects.txt";
  }

  std::ofstream serialized_objects_stream(filePath, std::ofstream::out); // Open in overwrite mode
  if (serialized_objects_stream) {
    for (auto it = objects.begin(); it != objects.end(); it++) {
      // std::cout << "In serialize_objects(), obj->id: " << it->first << std::endl;
      serialized_objects_stream << "obj_id " << it->first << std::endl;
      serialized_objects_stream << "object->id " << it->second->id << std::endl;
      serialized_objects_stream << "object->version " << it->second->version << std::endl;
      serialized_objects_stream << "object->is_leaf " << it->second->is_leaf << std::endl;
      serialized_objects_stream << "object->refcount " << it->second->refcount << std::endl;
      serialized_objects_stream << "object->last_access " << it->second->last_access << std::endl;
      serialized_objects_stream << "object->target_is_dirty " << it->second->target_is_dirty << std::endl;
      serialized_objects_stream << "object->pincount " << it->second->pincount << std::endl;
    }
  } else {
    std::cerr << "Error creating the file: serialized_objects." << std::endl;
  }

  serialized_objects_stream.close();
}

// Deserialize objects from a file and load them into memory
void swap_space::deserialize_objects(std::string filePath) {
    if (filePath.empty()) {
        filePath = "ss_objects.txt";
    }

    std::ifstream deserialized_objects_stream(filePath);
    
    if (deserialized_objects_stream) {
        objects.clear(); // Clear existing objects in memory
        
        std::string line;
        int current_obj_id = -1; // Track the current object's ID
        object* current_object = nullptr;

        while (std::getline(deserialized_objects_stream, line)) {
            // Parse lines from the file
            std::istringstream iss(line);
            std::string token;
            iss >> token;

            if (token == "obj_id") {
                iss >> current_obj_id;
                // std::cout << "In deserialize_objects(), current_obj_id: " << current_obj_id << std::endl;
                current_object = new object();
            }
            else if (current_object) {
                if (token == "object->id") {
                    iss >> current_object->id;
                    // std::cout << "In deserialize_objects(), current_object->id: " << current_object->id << std::endl;
                }
                else if (token == "object->version") {
                    iss >> current_object->version;
                    // std::cout << "In deserialize_objects(), current_object->version: " << current_object->version << std::endl;
                }
                else if (token == "object->is_leaf") {
                    iss >> current_object->is_leaf;
                }
                else if (token == "object->refcount") {
                    iss >> current_object->refcount;
                }
                else if (token == "object->last_access") {
                    iss >> current_object->last_access;
                }
                else if (token == "object->target_is_dirty") {
                    iss >> current_object->target_is_dirty;
                }
                else if (token == "object->pincount") {
                    iss >> current_object->pincount;
                }
            }

            if (current_obj_id != -1 && current_object && current_object->pincount != -1) {
                // Store the deserialized object in the objects map
                objects[current_obj_id] = current_object;
                current_obj_id = -1;
                current_object = nullptr;
            }
        }
    } else {
        std::cerr << "Error opening the file for deserialization." << std::endl;
    }
    
    deserialized_objects_stream.close();
}
