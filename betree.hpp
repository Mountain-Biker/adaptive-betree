// A basic B^e-tree implementation templated on types Key and Value.
// Keys and Values must be serializable (see swap_space.hpp).
// Keys must be comparable (via operator< and operator==).
// Values must be addable (via operator+).
// See test.cpp for example usage.

// This implementation represents in-memory nodes as objects with two
// fields:
// - a std::map mapping keys to child pointers
// - a std::map mapping (key, timestamp) pairs to messages
// Nodes are de/serialized to/from an on-disk representation.
// I/O is managed transparently by a swap_space object.


// This implementation deviates from a "textbook" implementation in
// that there is not a fixed division of a node's space between pivots
// and buffered messages.

// In a textbook implementation, nodes have size B, B^e space is
// devoted to pivots and child pointers, and B-B^e space is devoted to
// buffering messages.  Whenever a leaf gets too many messages, it
// splits.  Whenever an internal node gets too many messages, it
// performs a flush.  Whenever an internal node gets too many
// children, it splits.  This policy ensures that, whenever the tree
// needs to flush messages from a node to one of its children, it can
// always move a batch of size at least (B-B^e) / B^e = B^(1-e) - 1
// messages.

// In this implementation, nodes have a fixed maximum size.  Whenever
// a leaf exceeds this max size, it splits.  Whenever an internal node
// exceeds this maximum size, it checks to see if it can flush a large
// batch of elements to one of its children.  If it can, it does so.
// If it cannot, then it splits.

// In-memory nodes may temporarily exceed the maximum size
// restriction.  During a flush, we move all the incoming messages
// into the destination node.  At that point the node may exceed the
// max size.  The flushing procedure then performs further flushes or
// splits to restore the max-size invariant.  Thus, whenever a flush
// returns, all the nodes in the subtree of that node are guaranteed
// to satisfy the max-size requirement.

// This implementation also optimizes I/O based on which nodes are
// on-disk, clean in memory, or dirty in memory.  For example,
// inserted items are always immediately flushed as far down the tree
// as they can go without dirtying any new nodes.  This is because
// flushing an item to a node that is already dirty will not require
// any additional I/O, since the node already has to be written back
// anyway.  Furthermore, it will flush smaller batches to clean
// in-memory nodes than to on-disk nodes.  This is because dirtying a
// clean in-memory node only requires a write-back, whereas flushing
// to an on-disk node requires reading it in and writing it out.

// Ang: Functionality added for checkpoint
// After each upsert operation, it will check if we need to do checkpoint 
// by comparing the checkpoint_counter with checkpoint_granularity.
// When needed to do checkpoint, call checkpoint().
// There are three major steps in the checkpoint process.
// (1) flush all the in memory betree nodes to disk (all node files can be found at DESTINATION_DIRECTORY);
// (2) record the checkpoint information (lsn  and the checkpoint opcode = 3) in log file;
// (3) update loggingFileStatus.txt which contains the path of log file, 
// betree root node name and last checkpoint lsn.

#include <map>
#include <vector>
#include <cassert>
#include <iostream>
#include <cstdint>
#include <cmath>
#include <deque>
#include <ext/stdio_filebuf.h>
#include <sys/stat.h>
#include <dirent.h>
#include "swap_space.hpp"
#include "backing_store.hpp"

template<typename Key, typename Value> 
class betree;

// kosumi log status filename
#define LOGGING_FILE_STATUS "loggingFileStatus.txt"

// Ang: tmpdir_backup
#define DESTINATION_BACKUP_DIRECTORY "tmpdir_backup"
// Ang : swap_space objects serialized data
#define SWAPSPACE_OBJECTS_FILE "ss_objects.txt"


////////////////// Upserts

// Internally, we store data indexed by both the user-specified key
// and a timestamp, so that we can apply upserts in the correct order.
template<class Key>
class MessageKey {
public:
  MessageKey(void) :
    key(),
    timestamp(0)
  {}

  MessageKey(const Key & k, uint64_t tstamp) :
    key(k),
    timestamp(tstamp)
  {}

  static MessageKey range_start(const Key &key) {
    return MessageKey(key, 0);
  }

  static MessageKey range_end(const Key &key)
  {
    return MessageKey(key, UINT64_MAX);
  }

  MessageKey range_start(void) const
  {
    return range_start(key);
  }

  MessageKey range_end(void) const {
    return range_end(key);
  }

  void _serialize(std::iostream &fs, serialization_context &context) const {
    fs << timestamp << " ";
    serialize(fs, context, key);
  } 

  void _deserialize(std::iostream &fs, serialization_context &context) {
    fs >> timestamp;
    deserialize(fs, context, key);
  }

  Key key;
  uint64_t timestamp;
};

template<class Key>
bool operator<(const MessageKey<Key> & mkey1, const MessageKey<Key> & mkey2) {
  return mkey1.key < mkey2.key ||
		     (mkey1.key == mkey2.key && mkey1.timestamp < mkey2.timestamp);
}

template<class Key>
bool operator<(const Key & key, const MessageKey<Key> & mkey) {
  return key < mkey.key;
}

template<class Key>
bool operator<(const MessageKey<Key> & mkey, const Key & key) {
  return mkey.key < key;
}

template<class Key>
bool operator==(const MessageKey<Key> &a, const MessageKey<Key> &b) {
  return a.key == b.key && a.timestamp == b.timestamp;
}
  

// The three types of upsert.  An UPDATE specifies a value, v, that
// will be added (using operator+) to the old value associated to some
// key in the tree.  If there is no old value associated with the key,
// then it will add v to the result of a Value obtained using the
// default zero-argument constructor.
#define INSERT (0)
#define UPDATE (1)
#define DELETE (2)
#define CHECKPOINT_OPCODE (4)

template<class Value>
class Message {
public:
  Message(void) :
    opcode(INSERT),
    val()
  {}

  Message(int opc, const Value &v) :
    opcode(opc),
    val(v)
  {}
  
  void _serialize(std::iostream &fs, serialization_context &context) {
    fs << opcode << " ";
    serialize(fs, context, val);
  } 

  void _deserialize(std::iostream &fs, serialization_context &context) {
    fs >> opcode;
    deserialize(fs, context, val);
  }

  int opcode;
  Value val;
};

template <class Value>
bool operator==(const Message<Value> &a, const Message<Value> &b) {
  return a.opcode == b.opcode && a.val == b.val;
}

// Measured in messages.
#define DEFAULT_MAX_NODE_SIZE (1ULL<<18)
// #define DEFAULT_MAX_NODE_SIZE 64

// The minimum number of messages that we will flush to an out-of-cache node.
// Note: we will flush even a single element to a child that is already dirty.
// Note: we will flush MIN_FLUSH_SIZE/2 items to a clean in-memory child.
#define DEFAULT_MIN_FLUSH_SIZE (DEFAULT_MAX_NODE_SIZE / 16ULL)

template<class Key, class Value>
class Op {
    MessageKey<Key> key;
    // Use timestamp as LSN 
    Message<Value> val;

    public: 
    Op(MessageKey<Key> key, Message<Value> val): key(key), val(val) {}

    public: 
    Op() = default;

    public:
    uint64_t get_LSN() {
        return key.timestamp;
    }

    void _serialize(std::iostream &fs, serialization_context &context) {
        key._serialize(fs, context);
        fs << " -> ";
        val._serialize(fs, context);
        // Append to the WAL file instead of serializing the whole thing
    }

    void _deserialize(std::iostream &fs, serialization_context &context) {
        std::string dummy;
        // fs >> dummy;
        // std::cout << "Deserialize key: " << dummy << std::endl;
        deserialize(fs, context, key);
        fs >> dummy;
        // serialize(std::cout, context, key);
        // std::cout << "Arrow: " << dummy << std::endl;
        deserialize(fs, context, val);
        // std::cout << "Decode val" << std::endl;
        // serialize(std::cout, context, val);
    }
};


//return filestream corresponding to an item. Needed for deserialization.
std::iostream * load_log(char* log_file) {
  __gnu_cxx::stdio_filebuf<char> *fb = new __gnu_cxx::stdio_filebuf<char>;
  fb->open(log_file, std::fstream::in | std::fstream::out);

  std::fstream *ios = new std::fstream;
  ios->std::ios::rdbuf(fb);
  // ios->exceptions(std::fstream::badbit | std::fstream::failbit | std::fstream::eofbit);
  assert(ios->good());
  
  return ios;
}


template<class Op>
class Logs: public serializable {
  public:
    std::vector<Op> wal;

    uint64_t lastPersistLSN; // lastPersistLSN is the lsn of the last time we do persist(flush writing ahead log from memory to disk), this also should be the last lsn in the test.logg file
    uint64_t lastCheckpointLSN; // lastCheckpointLSN is the lsn of the last time we do checkpoint
    uint64_t persistence_granularity;
    uint64_t checkpoint_granularity;
    uint64_t log_counter = 1; // count how many times we write a log to wal
    serialization_context context;
    std::fstream outfile;
    std::string log_file_path; // the path of log file, in this project it is the path of test.logg file

    
        Logs(uint64_t pg , uint64_t cg , char* log_file , serialization_context context): 
            lastPersistLSN(0), 
            lastCheckpointLSN(0),
        persistence_granularity(pg),
        checkpoint_granularity(cg),
        context(context)
    {
            if (log_file != nullptr) {
                auto log_data = load_log(log_file);
                this->_deserialize(*log_data, context);
                outfile = std::fstream(log_file, std::ios_base::app);
                log_file_path = log_file;
            } else {
                std::string filename = "test.logg";
                log_file_path = filename;
                std::ofstream myfile;
                myfile.open (filename, std::fstream::app);
                myfile.close();
                outfile = std::fstream("test.logg");
                outfile << "Logs: " << std::endl;
            }
        }
        // Logs() = default;


        void log(Op op) {
            this->wal.push_back(op);
            log_counter++;
        }

        void persist() {
            for (auto& op: wal) {
                auto lsn = op.get_LSN();
                if (lastPersistLSN < lsn) {
                    op._serialize(outfile, context);
                    outfile << std::endl;
                    lastPersistLSN = lsn;
                }
            }
            std::flush(outfile);
            // Ang : we need to clear the records in wal after flushing them to disk
            wal.clear();
        }

  
        // const Op& get_last_checkpoint() {
        //     if (wal.empty()) {
        //         return nullptr;
        //     } else {
        //         return wal[0];
        //     }
        // }

        void _serialize(std::iostream &fs, serialization_context &context) {
            // fs:
            fs << "Logs: " << std::endl;
            for (auto& op: wal) {
                // fs << "  ";
                op._serialize(fs, context);
                fs << std::endl;
            }
            // assert(false);
            // Append to the WAL file instead of serializing the whole thing
        }

        void _deserialize(std::iostream &fs, serialization_context &context) {
            int size = 0;
            std::string dummy;
            fs >> dummy ;
          std::cout << dummy << std::endl;
           while (fs.peek() != EOF)
           // for (int k = 0; k < 2; k++)
            {
                // std::cout << "Peeking:" << fs.peek() << std::endl;
                Op op;
                deserialize(fs, context, op); 
                // fs >> dummy;
                wal.push_back(op);

                // while (fs.peek() == '\n') {
                    // std::cout << "Get rid of newline" << std::endl;
                fs.get();
                    // std::cout << "Get rid of newline done" << std::endl;
                // }
                assert(fs.good());
            }
            lastPersistLSN = wal.back().get_LSN();
          // deserialize(fs, context, child);
          // deserialize(fs, context, child_size);
        }

    
};

template<class Key, class Value> class betree {
private:

  class node;
  // We let a swap_space handle all the I/O.
  typedef typename swap_space::pointer<node> node_pointer;
  class child_info : public serializable {
  public:
    child_info(void)
      : child(),
	    child_size(0)
    {}
    
    child_info(node_pointer child, uint64_t child_size)
      : child(child),
	    child_size(child_size)
    {}

    void _serialize(std::iostream &fs, serialization_context &context) {
      serialize(fs, context, child);
      fs << " ";
      serialize(fs, context, child_size);
    }

    void _deserialize(std::iostream &fs, serialization_context &context) {
      deserialize(fs, context, child);
      deserialize(fs, context, child_size);
    }
    
    node_pointer child;
    uint64_t child_size;
  };
  typedef typename std::map<Key, child_info> pivot_map;
  typedef typename std::map<MessageKey<Key>, Message<Value> > message_map;
    
  class node : public serializable {
  public:

    // Child pointers
    pivot_map pivots;
    message_map elements;

    bool is_leaf(void) const {
      return pivots.empty();
    }

    // Check if a node needs to be split
    bool need_to_split(betree &bet) {

      // return (pivots.size() >= bet.pivot_upper_bound || 
      //           (elements.size() >= bet.message_upper_bound && pivots.size() + elements.size() >= bet.max_node_size));
      
      // the condition of if a node is needed to be split is 
      // based on the paper: An Introduction to Bε-trees and Write-Optimization
      if (is_leaf()) {
        return elements.size() >= bet.max_node_size;
      } else {
        return pivots.size() >= bet.pivot_upper_bound;
      }
    
    }

    // Holy frick-a-moly.  We want to write a const function that
    // returns a const_iterator when called from a const function and
    // a non-const function that returns a (non-const_)iterator when
    // called from a non-const function.  And we don't want to
    // duplicate the code.  The following solution is from
    //         http://stackoverflow.com/a/858893
    // Ang : return the iterator of pivot_map which point to the frist element >= key k
    template<class OUT, class IN>
    static OUT get_pivot(IN & mp, const Key & k) { 
      assert(mp.size() > 0);
      auto it = mp.lower_bound(k);
      if (it == mp.begin() && k < it->first)
	      throw std::out_of_range("Key does not exist "
				"(it is smaller than any key in DB)");
      if (it == mp.end() || k < it->first)
	      --it;
      return it;      
    }

    // Instantiate the above template for const and non-const
    // calls. (template inference doesn't seem to work on this code)
    typename pivot_map::const_iterator get_pivot(const Key & k) const {
      return get_pivot<typename pivot_map::const_iterator,
		       const pivot_map>(pivots, k);
    }

    typename pivot_map::iterator
    get_pivot(const Key & k) {
      return get_pivot<typename pivot_map::iterator, pivot_map>(pivots, k);
    }

    // Return iterator pointing to the first element with mk >= k.
    // (Same const/non-const templating trick as above)
    template<class OUT, class IN>
    static OUT get_element_begin(IN & elts, const Key &k) {
      return elts.lower_bound(MessageKey<Key>::range_start(k));
    }

    typename message_map::iterator get_element_begin(const Key &k) {
      return get_element_begin<typename message_map::iterator,
			       message_map>(elements, k);
    }

    typename message_map::const_iterator get_element_begin(const Key &k) const {
      return get_element_begin<typename message_map::const_iterator,
			       const message_map>(elements, k);
    }

    // Return iterator pointing to the first element that goes to
    // child indicated by it
    typename message_map::iterator
    get_element_begin(const typename pivot_map::iterator it) {
      return it == pivots.end() ? elements.end() : get_element_begin(it->first);
    }

    // Apply a message to ourself.
    // Ang : apply() actually insert the MessageKey<Key>-Message<Value> pair 
    // into the elements of the corresponding node;
    void apply(const MessageKey<Key> &mkey, const Message<Value> &elt,
	       Value &default_value) {
      switch (elt.opcode) {
      case INSERT:
        elements.erase(elements.lower_bound(mkey.range_start()),
                elements.upper_bound(mkey.range_end()));
        elements[mkey] = elt;
        break;

      case DELETE:
        elements.erase(elements.lower_bound(mkey.range_start()),
                elements.upper_bound(mkey.range_end()));
        if (!is_leaf())
          elements[mkey] = elt;
        break;

      case UPDATE:
        {
          // Returns an iterator pointing to the first element in the container 
          // whose key is considered to go after k.
          auto iter = elements.upper_bound(mkey.range_end()); 
          if (iter != elements.begin())
            iter--;
          if (iter == elements.end() || iter->first.key != mkey.key)
            if (is_leaf()) {
              Value dummy = default_value;
              apply(mkey, Message<Value>(INSERT, dummy + elt.val),
                    default_value);
            } else {
              elements[mkey] = elt;
            }
          else {
            assert(iter != elements.end() && iter->first.key == mkey.key);
            if (iter->second.opcode == INSERT) {
              apply(mkey, Message<Value>(INSERT, iter->second.val + elt.val),
                    default_value);	  
            } else {
              elements[mkey] = elt;	      
            }
          }
        }
        break;

      default:
	      assert(0);
      }
    }
    
    // Requires: there are less than MIN_FLUSH_SIZE things in elements
    //           destined for each child in pivots);
    pivot_map split(betree &bet) {
      // std::cout << "In betree split(), pivots size: " << pivots.size() << 
      //   ", messages size: " << elements.size() << std::endl;
      // std::cout << "node total size: " << pivots.size() + elements.size() <<
      //   ", max node size: " << bet.max_node_size << std::endl;
    

      bet.split_counter++;

      // assert(pivots.size() + elements.size() >= bet.max_node_size);
      // This size split does a good job of causing the resulting
      // nodes to have size between 0.4 * MAX_NODE_SIZE and 0.6 * MAX_NODE_SIZE.
      int num_new_leaves =
	      (pivots.size() + elements.size())  / (10 * bet.max_node_size / 24);
      // because the condition of an internal node to be split is : pivots.size() >= pivot_upper_bound,
      // so there is a possible case that, an internal node whose pivots.size() == pivot_upper_bound,
      // but the elements.size() == 0, in this case, the result of num_new_leaves calculated by the above equation is 0
      // it will lead to an error on the calculation of things_per_new_leaf(the denominator is 0).
      // So, we need to check if the num_new_leaves == 0, if yes, assign 2 to it.
      if (num_new_leaves == 0) {
        num_new_leaves = 2;
      }

      int things_per_new_leaf =
	      (pivots.size() + elements.size() + num_new_leaves - 1) / num_new_leaves;

      pivot_map result;
      auto pivot_idx = pivots.begin(); // pivot_idx is an iterator of pivots(pivot_map) of the node to be splited;
      auto elt_idx = elements.begin(); // pivot_idx is an iterator of elements(message_map) of the node to be splited;
      int things_moved = 0;
      for (int i = 0; i < num_new_leaves; i++) {
        if (pivot_idx == pivots.end() && elt_idx == elements.end())
          break;
        node_pointer new_node = bet.ss->allocate(new node);
        result[pivot_idx != pivots.end() ?
              pivot_idx->first :
              elt_idx->first.key] = child_info(new_node,
                  new_node->elements.size() +
                  new_node->pivots.size());
        while(things_moved < (i+1) * things_per_new_leaf &&
              (pivot_idx != pivots.end() || elt_idx != elements.end())) {
          if (pivot_idx != pivots.end()) {
            new_node->pivots[pivot_idx->first] = pivot_idx->second;
            ++pivot_idx;
            things_moved++;
            auto elt_end = get_element_begin(pivot_idx);
            while (elt_idx != elt_end) {
              new_node->elements[elt_idx->first] = elt_idx->second;
              ++elt_idx;
              things_moved++;
            }
          } else {
            // Must be a leaf
            assert(pivots.size() == 0);
            new_node->elements[elt_idx->first] = elt_idx->second;
            ++elt_idx;
            things_moved++;	    
          }
        }
      }
      
      for (auto it = result.begin(); it != result.end(); ++it)
        it->second.child_size = it->second.child->elements.size() +
          it->second.child->pivots.size();
            
      assert(pivot_idx == pivots.end());
      assert(elt_idx == elements.end());
      pivots.clear();
      elements.clear();
      return result;
    }

    node_pointer merge(betree &bet,
		       typename pivot_map::iterator begin,
		       typename pivot_map::iterator end) {
      node_pointer new_node = bet.ss->allocate(new node);
      for (auto it = begin; it != end; ++it) {
        new_node->elements.insert(it->second.child->elements.begin(),
                it->second.child->elements.end());
        new_node->pivots.insert(it->second.child->pivots.begin(),
                it->second.child->pivots.end());
      }
      return new_node;
    }

    void merge_small_children(betree &bet) {
      if (is_leaf())
	      return;

      for (auto beginit = pivots.begin(); beginit != pivots.end(); ++beginit) {
        uint64_t total_size = 0;
        auto endit = beginit;
        while (endit != pivots.end()) {
          // beginit should be endit at here, I think the original code has a mistake
          if (total_size + beginit->second.child_size > 6 * bet.max_node_size / 10)
            break;
          total_size += beginit->second.child_size;
          ++endit;
        }

        // Ang : if the nubmer of nodes can be merged is >= 2
        if (endit != beginit) {
          node_pointer merged_node = merge(bet, beginit, endit);
          for (auto tmp = beginit; tmp != endit; ++tmp) {
            tmp->second.child->elements.clear();
            tmp->second.child->pivots.clear();
          }
          Key key = beginit->first;
          pivots.erase(beginit, endit);
          pivots[key] = child_info(merged_node, merged_node->pivots.size() + merged_node->elements.size());
          beginit = pivots.lower_bound(key);
        }
      }
    }
    
    // Receive a collection of new messages and perform recursive
    // flushes or splits as necessary.  If we split, return a
    // map with the new pivot keys pointing to the new nodes.
    // Otherwise return an empty map.
    pivot_map flush(betree &bet, message_map &elts)
    {
      debug(std::cout << "Flushing " << this << std::endl);
      pivot_map result;

      if (elts.size() == 0) {
        debug(std::cout << "Done (empty input)" << std::endl);
        return result;
      }


      //bool is_leaf(void) const {
      // return pivots.empty();
      // }
      if (is_leaf()) { 
        for (auto it = elts.begin(); it != elts.end(); ++it)
          apply(it->first, it->second, bet.default_value);
        // the original split condition
        // if (elements.size() + pivots.size() >= bet.max_node_size)
        //   result = split(bet);

        // the modified split condition, for a leaf node the split condition is
        // the elements size exceeds the max_node_size
        // originally the root node is a leaf node, so we also need to check the pivots size of a leaf node
        if (pivots.size() > bet.pivot_upper_bound || (pivots.size() + elements.size()) > bet.max_node_size) {
          result = split(bet);
        }

        return result;
      }	

      ////////////// Non-leaf
      
      // Update the key of the first child, if necessary
      Key oldmin = pivots.begin()->first;
      MessageKey<Key> newmin = elts.begin()->first;
      if (newmin < oldmin) {
        pivots[newmin.key] = pivots[oldmin];
        pivots.erase(oldmin);
      }

      // If everything is going to a single dirty child, go ahead
      // and put it there. 
      // For the project, each time we only insert a message_map with size 1, 
      // it means that the message will always go to a single child(in our project).
      auto first_pivot_idx = get_pivot(elts.begin()->first.key);
      auto last_pivot_idx = get_pivot((--elts.end())->first.key);
      if (first_pivot_idx == last_pivot_idx &&
	      first_pivot_idx->second.child.is_dirty()) { //Ang: first_pivot_idx is an iterator of pivot_map
      	// There shouldn't be anything in our buffer for this child,
      	// but lets assert that just to be safe.
        {
          auto next_pivot_idx = next(first_pivot_idx);
          auto elt_start = get_element_begin(first_pivot_idx);
          auto elt_end = get_element_begin(next_pivot_idx); 
          // assert(elt_start == elt_end);
        }
      	pivot_map new_children = first_pivot_idx->second.child->flush(bet, elts);
      	if (!new_children.empty()) {
      	  pivots.erase(first_pivot_idx);
      	  pivots.insert(new_children.begin(), new_children.end());
      	} else {
          first_pivot_idx->second.child_size =
          first_pivot_idx->second.child->pivots.size() +
          first_pivot_idx->second.child->elements.size();
	      }

        if (pivots.size() > bet.pivot_upper_bound || (elements.size() + pivots.size()) > bet.max_node_size) {
          result = split(bet);
        }

      } else {
        // Ang :If the child that the message should be flushed to is not dirty,
        // apply the message in the current node.
        // The apply() function inserts the message in the elements map of the current node.
        for (auto it = elts.begin(); it != elts.end(); ++it)
          apply(it->first, it->second, bet.default_value);

        // Ang : After apply() the message into the current node, 
        // check if the size of the message map of the node is large enough 
        // that some messages need to be flushed to the child nodes

        // Now flush to out-of-core or clean children as necessary
        // the original while loop condition: elements.size() + pivots.size() >= bet.max_node_size
        // while (elements.size() + pivots.size() >= bet.max_node_size) {
        while (elements.size() >= bet.message_upper_bound) {
          // Find the child with the largest set of messages in our buffer
          unsigned int max_size = 0;
          auto child_pivot = pivots.begin();
          auto next_pivot = pivots.begin();
          for (auto it = pivots.begin(); it != pivots.end(); ++it) {
            auto it2 = next(it);
            auto elt_it = get_element_begin(it); 
            auto elt_it2 = get_element_begin(it2); 
            unsigned int dist = distance(elt_it, elt_it2);
            if (dist > max_size) {
              child_pivot = it;
              next_pivot = it2;
              max_size = dist;
            }
          }

          if (!(max_size > bet.min_flush_size ||
          (max_size > bet.min_flush_size/2 &&
          child_pivot->second.child.is_in_memory())))
            break; // We need to split because we have too many pivots
          auto elt_child_it = get_element_begin(child_pivot);
          auto elt_next_it = get_element_begin(next_pivot);
          message_map child_elts(elt_child_it, elt_next_it); // initialize the message map need to be flushed
          pivot_map new_children = child_pivot->second.child->flush(bet, child_elts); // flush child_elts to the child node
          elements.erase(elt_child_it, elt_next_it); // erase the corresponding messages in the current node elements
          if (!new_children.empty()) {  // if the child is split 
            pivots.erase(child_pivot);
            pivots.insert(new_children.begin(), new_children.end());
          } else {
            child_pivot->second.child_size =
              child_pivot->second.child->pivots.size() +
              child_pivot->second.child->elements.size();
          }
        }

        // We have too many pivots to efficiently flush stuff down, so split
        // the original split condition
        // if (elements.size() + pivots.size() > bet.max_node_size) {
        //   result = split(bet);
        // }

        // the modified split condition, for internal node the split condition is
        // either the pivots size exceeds the upper bound 
        // or the overall size of the node exceeds the max_node_size
        if (pivots.size() > bet.pivot_upper_bound || (elements.size() + pivots.size()) > bet.max_node_size) {
          result = split(bet);
        }

      }

      //merge_small_children(bet);
      
      debug(std::cout << "Done flushing " << this << std::endl);
      return result;
    }

    // Ang: flush all the message in the current node and its child nodes downward
    // after the call of force_flush() the elements size of 
    // the current node and its child nodes should be zero.
    pivot_map compulsory_flush(betree &bet)
    {
      debug(std::cout << "Flushing " << this << std::endl);
      pivot_map result;

      // if (elts.size() == 0) {
      //   debug(std::cout << "Done (empty input)" << std::endl);
      //   return result;
      // }


      //bool is_leaf(void) const {
      // return pivots.empty();
      // }
      if (is_leaf()) { 
        // for (auto it = elts.begin(); it != elts.end(); ++it)
        //   apply(it->first, it->second, bet.default_value);
        // if (elements.size() + pivots.size() >= bet.max_node_size)
        //   result = split(bet);
        return result;
      }	

      ////////////// Non-leaf
      
      // Update the key of the first child, if necessary
      // Key oldmin = pivots.begin()->first;
      // MessageKey<Key> newmin = elts.begin()->first;
      // if (newmin < oldmin) {
      //   pivots[newmin.key] = pivots[oldmin];
      //   pivots.erase(oldmin);
      // }

      // If everything is going to a single dirty child, go ahead
      // and put it there. 
      // auto first_pivot_idx = get_pivot(elts.begin()->first.key);
      // auto last_pivot_idx = get_pivot((--elts.end())->first.key);
      // if (first_pivot_idx == last_pivot_idx &&
	    //   first_pivot_idx->second.child.is_dirty()) { //Ang: first_pivot_idx is an iterator of pivot_map
      // 	// There shouldn't be anything in our buffer for this child,
      // 	// but lets assert that just to be safe.
      //   {
      //     auto next_pivot_idx = next(first_pivot_idx);
      //     auto elt_start = get_element_begin(first_pivot_idx);
      //     auto elt_end = get_element_begin(next_pivot_idx); 
      //     assert(elt_start == elt_end);
      //   }
      // 	pivot_map new_children = first_pivot_idx->second.child->flush(bet, elts);
      // 	if (!new_children.empty()) {
      // 	  pivots.erase(first_pivot_idx);
      // 	  pivots.insert(new_children.begin(), new_children.end());
      // 	} else {
      //     first_pivot_idx->second.child_size =
      //     first_pivot_idx->second.child->pivots.size() +
      //     first_pivot_idx->second.child->elements.size();
	    //   }

      // } else {
	
      //   for (auto it = elts.begin(); it != elts.end(); ++it)
      //     apply(it->first, it->second, bet.default_value);

        // Now flush all the message in the current node to out-of-core or clean children compulsively
        // the while loop will not break until elements.size == 0
        // typedef std::__1::map<Key, betree<Key, Value>::child_info> betree<Key, Value>::pivot_map
        // A instance of class child_info contains two attributes, the first one is node_pointer: child, 
        // the second one is uint64_t: child_size
        while (elements.size() > 0) {

          auto child_pivot = pivots.begin();
          auto next_pivot = pivots.begin();
          for (auto it = pivots.begin(); it != pivots.end(); ++it) {
            auto it2 = next(it);
            auto elt_it = get_element_begin(it); 
            auto elt_it2 = get_element_begin(it2); 
            child_pivot = it;
            next_pivot = it2;
            auto elt_child_it = get_element_begin(child_pivot);
            auto elt_next_it = get_element_begin(next_pivot);
            message_map child_elts(elt_child_it, elt_next_it);
            pivot_map new_children = child_pivot->second.child->flush(bet, child_elts);
            elements.erase(elt_child_it, elt_next_it);
            if (!new_children.empty()) {
              pivots.erase(child_pivot);
              pivots.insert(new_children.begin(), new_children.end());
            } else {
              child_pivot->second.child_size =
                child_pivot->second.child->pivots.size() +
                child_pivot->second.child->elements.size();
            }
            
          }

        
        }

        // std::cout << "current node message map size(should be zero after compulsory flush): " << 
        //   elements.size() << std::endl;
        // std::cout << "the pivots size after compulsory flush is: " << pivots.size() << std::endl;

        // We have too many pivots to efficiently flush stuff down, so split
        if (pivots.size() > bet.pivot_upper_bound || (elements.size() + pivots.size()) > bet.max_node_size) {
          result = split(bet);
        }
    //  }

      //merge_small_children(bet);
      
      debug(std::cout << "Done compulsory flushing " << this << std::endl);
      return result;
    }

    std::deque<node_pointer> shorten_node(betree &bet) {
      // std::cout << "the pivots size of current node (before the shortening process): "
      //   << pivots.size() << std::endl;

      // initialize a queue child_node_pointers to store the pointers of 
      // child nodes of the current node after the reconstruction
      std::deque<node_pointer> child_node_pointers;

      if (is_leaf()) { // If the current node is leaf node, return an empty deque.
        return child_node_pointers; 
      }

      // compulsively flush all the messages of child nodes
      for (auto it = pivots.begin(); it != pivots.end(); ) {
        if (it->second.child->is_leaf()) {
          ++it; // Move to the next element in case of a leaf node
          continue;
        }

        pivot_map new_children = it->second.child->compulsory_flush(bet);
        if (!new_children.empty()) {
          it = pivots.erase(it);
          for (const auto& entry : new_children) {
            it = pivots.insert(it, entry);
            ++it; // Move to the next element
        }
        } else {
          it->second.child_size =
            it->second.child->pivots.size() +
            it->second.child->elements.size();
          ++it;
        }
      }

      // update the pivots of the current node, make each of the pivot point to a grand child
      for (auto it = pivots.begin(); it != pivots.end(); it++) {
        // the iterator "it" is the pivot points to the child node 
        // if the child is a leaf node, continue to the next child
        if (it->second.child->is_leaf()) {
          continue;
        }

        pivot_map grand_child_pivots = it->second.child->pivots;
        // insert the grand_child_pivots to the root node.
        if (!grand_child_pivots.empty()) {
          pivots.erase(it);
          pivots.insert(grand_child_pivots.begin(), grand_child_pivots.end());
        }
      }

      // std::cout << "the pivots size of root node (after the shortening process): "
      //   << pivots.size() << std::endl;

      for (auto it = pivots.begin(); it != pivots.end(); it++) {
        child_node_pointers.push_back(it->second.child);
      }

      return child_node_pointers;

    }


    

    Value query(const betree & bet, const Key k) const 
    {
      debug(std::cout << "Querying " << this << std::endl);
      if (is_leaf()) {
        auto it = elements.lower_bound(MessageKey<Key>::range_start(k));
        if (it != elements.end() && it->first.key == k) {
          assert(it->second.opcode == INSERT);
          return it->second.val;
        } else {
          throw std::out_of_range("Key does not exist");
        }
      }

      ///////////// Non-leaf

      // Return iterator pointing to the first element that goes to
      // child indicated by it; get_element_begin() return a message_map iterator
      auto message_iter = get_element_begin(k);
      Value v = bet.default_value;

      if (message_iter == elements.end() || k < message_iter->first)
        // If we don't have any messages for this key, just search
        // further down the tree.
        v = get_pivot(k)->second.child->query(bet, k);
      else if (message_iter->second.opcode == UPDATE) {
        // We have some updates for this key.  Search down the tree.
        // If it has something, then apply our updates to that.  If it
        // doesn't have anything, then apply our updates to the
        // default initial value.
        try {
          Value t = get_pivot(k)->second.child->query(bet, k);
          v = t;
        } catch (std::out_of_range & e) {}
      } else if (message_iter->second.opcode == DELETE) {
        // We have a delete message, so we don't need to look further
        // down the tree.  If we don't have any further update or
        // insert messages, then we should return does-not-exist (in
        // this subtree).
        message_iter++;
        if (message_iter == elements.end() || k < message_iter->first)
          throw std::out_of_range("Key does not exist");
      } else if (message_iter->second.opcode == INSERT) {
        // We have an insert message, so we don't need to look further
        // down the tree.  We'll apply any updates to this value.
        v = message_iter->second.val;
        message_iter++;
      }

      // Apply any updates to the value obtained above.
      while (message_iter != elements.end() && message_iter->first.key == k) {
        assert(message_iter->second.opcode == UPDATE);
        v = v + message_iter->second.val;
        message_iter++;
      }

      return v;
    }

    std::pair<MessageKey<Key>, Message<Value> >
    get_next_message_from_children(const MessageKey<Key> *mkey) const {
      if (mkey && *mkey < pivots.begin()->first)
	      mkey = NULL;
      auto it = mkey ? get_pivot(mkey->key) : pivots.begin();
      while (it != pivots.end()) {
        try {
          return it->second.child->get_next_message(mkey);
        } catch (std::out_of_range & e) {}
        ++it;
      }
      throw std::out_of_range("No more messages in any children");
    }
    
    std::pair<MessageKey<Key>, Message<Value> >
    get_next_message(const MessageKey<Key> *mkey) const {
      auto it = mkey ? elements.upper_bound(*mkey) : elements.begin();

      if (is_leaf()) {
        if (it == elements.end())
          throw std::out_of_range("No more messages in sub-tree");
        return std::make_pair(it->first, it->second);
      }

      if (it == elements.end())
	      return get_next_message_from_children(mkey);
      
      try {
        auto kids = get_next_message_from_children(mkey);
        if (kids.first < it->first)
          return kids;
        else 
          return std::make_pair(it->first, it->second);
        } catch (std::out_of_range & e) {
        return std::make_pair(it->first, it->second);	
      }
    }
    
    // kosumi: serialization of "pivots:"
    void _serialize(std::iostream &fs, serialization_context &context) {
      fs << "pivots:" << std::endl;
      serialize(fs, context, pivots);
      fs << "elements:" << std::endl;
      serialize(fs, context, elements);
    }
    
    void _deserialize(std::iostream &fs, serialization_context &context) {
      std::string dummy;
      fs >> dummy;
      deserialize(fs, context, pivots);
      fs >> dummy;
      deserialize(fs, context, elements);
    }

    
  };

  swap_space *ss;
  uint64_t min_flush_size;
  uint64_t max_node_size;
  uint64_t min_node_size;
  node_pointer root;
  uint64_t next_timestamp = 1; // Nothing has a timestamp of 0
  Value default_value;
  Logs<Op<Key, Value>>& logs;
  double epsilon; 
  uint64_t pivot_upper_bound;
  uint64_t message_upper_bound;
  // the state of betree 0: write heavy, 3: read heavy, 
  // 7: fixed mode(epsilon do not adjust to workload);
  // the initial state is write heavy 0
  int state = 0; 
  int split_counter = 0;
  
public:
  // actually the max_node_size, min_flush_size and min_node_size are 
  // defined at the betree constructor in test_logging_restore.cpp 
  betree(swap_space *sspace,
	 uint64_t maxnodesize = DEFAULT_MAX_NODE_SIZE,
	 uint64_t minnodesize = DEFAULT_MAX_NODE_SIZE / 4,
	 uint64_t minflushsize = DEFAULT_MIN_FLUSH_SIZE ) :
    ss(sspace),
    min_flush_size(minflushsize),
    max_node_size(maxnodesize),
    min_node_size(minnodesize)
  {
    root = ss->allocate(new node);
  }

  betree(swap_space *sspace,
     Logs<Op<Key, Value>>& logs,
     double epsilon,
     int betree_state,
    uint64_t maxnodesize = DEFAULT_MAX_NODE_SIZE,
    uint64_t minnodesize = DEFAULT_MAX_NODE_SIZE / 4,
    uint64_t minflushsize = DEFAULT_MIN_FLUSH_SIZE ) :
    logs(logs),
    ss(sspace),
    epsilon(epsilon),
    state(betree_state),
    min_flush_size(minflushsize),
    max_node_size(maxnodesize),
    min_node_size(minnodesize)
  {
    root = ss->allocate(new node);
    pivot_upper_bound = pow(static_cast<double>(max_node_size), epsilon);
    message_upper_bound = max_node_size - pivot_upper_bound;

    std::cout << "epsilon: " << epsilon << std::endl;
    std::cout << "state: " << state << std::endl;
    std::cout << "pivot_upper_bound: " << pivot_upper_bound << std::endl;
    std::cout << "max_node_size: " << max_node_size << std::endl;
    std::cout << "min_flush_size: " << min_flush_size << std::endl;
    std::cout << "min_node_size: " << min_node_size << std::endl;
  }

    // Ang: get the total number of splitting in a test
    int get_split_counter(void) {
      return split_counter;
    }

    // Ang: set epsilon and upper bounds
    void set_epsilon(double new_epsilon) {
      epsilon = new_epsilon;
      pivot_upper_bound = pow(static_cast<double>(max_node_size), epsilon);
      message_upper_bound = max_node_size - pivot_upper_bound;
    }

    double get_epsilon(void) {
      return epsilon;
    }

    int get_state(void) {
      return state;
    }

    void set_state(int new_state) {
      state = new_state;
    }

    int get_pivot_upper_bound(void) {
      return pivot_upper_bound;
    }

    int get_message_upper_bound(void) {
      return message_upper_bound;
    }

    uint64_t get_max_node_size(void) {
      return max_node_size;
    }

    uint64_t get_min_node_size(void) {
      return min_node_size;
    }

    uint64_t get_min_flush_size(void) {
      return min_flush_size;
    }

    //Ang : set the next_timestamp when do recovery
    void set_next_timestamp(uint64_t timestamp) {
        next_timestamp = timestamp;
    }

  // Ang : get betree root name
    std::string get_betree_root_id() {
        return std::to_string(root.get_target());
    }

    // Ang : compulsory flush message of root node
    // Recap: typedef typename std::map<Key, child_info> pivot_map;
    // Each node has two attributes, the one is pivots, which is a variable belong to pivot_map
    // the other one is elements, which is a variable belong to message_map.
    // The key of an entry in the pivot_map is the index of a database operation. 
    // The value of an entry in the pivot_map is a variable belong to child_info class,
    // which contains two parts: a node_pointer to its child, 
    // and an uint64_t integer child_size which denotes the size of the child.

    // void shorten_root_node(void) {
    //   std::cout << "the pivots size of root node (before the shortening process): "
    //     << root->pivots.size() << std::endl;

    //   // pivot_map new_nodes = root->compulsory_flush(*this);
    //   // if (new_nodes.size() > 0) {
    //   //   root = ss->allocate(new node);
    //   //   root->pivots = new_nodes;
    //   // }

    //   // compulsively flush all the messages of child nodes of root node
    //   for (auto it = root->pivots.begin(); it != root->pivots.end(); it++) {
    //     pivot_map new_children = it->second.child->compulsory_flush(*this);
    //     if (!new_children.empty()) {
    //       root->pivots.erase(it);
    //       root->pivots.insert(new_children.begin(), new_children.end());
    //     } else {
    //       it->second.child_size =
    //         it->second.child->pivots.size() +
    //         it->second.child->elements.size();
    //     }
    //   }

    //   // update the pivots of root node, make each of the pivot point to a grand child
    //   for (auto it = root->pivots.begin(); it != root->pivots.end(); it++) {
    //     // the iterator "it" is the pivot points to the child node 
    //     // if the child is a leaf node, continue to the next child
    //     if (it->second.child->is_leaf()) {
    //       continue;
    //     }

    //     pivot_map grand_child_pivots = it->second.child->pivots;
    //     // insert the grand_child_pivots to the root node.
    //     if (!grand_child_pivots.empty()) {
    //       root->pivots.erase(it);
    //       root->pivots.insert(grand_child_pivots.begin(), grand_child_pivots.end());
    //     }
    //   }

    //   std::cout << "the pivots size of root node (after the shortening process): "
    //     << root->pivots.size() << std::endl;
      
    // }

    void shorten_root_node(void) {
      root->shorten_node(*this);
    }

    void shorten_betree(void) {
      std::cout << "******** start shortening betree ********" << std::endl;

      std::deque<node_pointer> being_processed_nodes;
      being_processed_nodes.push_back(root);
      shorten_betree(being_processed_nodes);

      std::cout << "******** finish shortening betree ********" << std::endl;
    }

    void shorten_betree(std::deque<node_pointer>& being_processed_nodes) {
      if (being_processed_nodes.empty()) {
        return;
      }
      // std::cout << "******** in shorten_betree() ********" << std::endl;

      std::deque<node_pointer> next_to_be_processed_nodes;

      while (!being_processed_nodes.empty()) {
        node_pointer curr_node = being_processed_nodes.front();
        being_processed_nodes.pop_front();
        std::deque<node_pointer> curr_next_to_be_processed_nodes = curr_node->shorten_node(*this);
        while (!curr_next_to_be_processed_nodes.empty()) {
          next_to_be_processed_nodes.push_back(curr_next_to_be_processed_nodes.front());
          curr_next_to_be_processed_nodes.pop_front();
        }
      }

      if (!next_to_be_processed_nodes.empty()) {
        shorten_betree(next_to_be_processed_nodes);
      }
    }


    void traverse_betree(std::deque<node_pointer>& being_traversed_nodes, 
      int current_height, int& leaves_num, int& total_leaves_height) {

      std::cout << "in traverse_betree(), number of nodes to be traversed at this level: " << being_traversed_nodes.size() << std::endl;
      std::cout << "current_height: " << current_height << std::endl;

      if (being_traversed_nodes.empty()) {
        return;
      }

      std::deque<node_pointer> next_to_be_traversed_nodes;
      while (!being_traversed_nodes.empty()) {
        node_pointer curr_node = being_traversed_nodes.front();
        being_traversed_nodes.pop_front();
        if (curr_node->is_leaf()) {
          leaves_num++;
          total_leaves_height += current_height;
        } else {
          auto pivots = curr_node->pivots;
          for (auto it = pivots.begin(); it != pivots.end(); it++) {
            next_to_be_traversed_nodes.push_back(it->second.child);
          }
        }

      }

      if (!next_to_be_traversed_nodes.empty()) {
        current_height++;
        traverse_betree(next_to_be_traversed_nodes, current_height, leaves_num, total_leaves_height);
      }

    }

    double calculateAverageHeight(void) {
      std::cout << "root pivots size: " << root->pivots.size() << std::endl; 

      std::deque<node_pointer> being_traversed_nodes;
      being_traversed_nodes.push_back(root);
      int current_height = 0;
      int leaves_num = 0;
      int total_leaves_height = 0;
      traverse_betree(being_traversed_nodes, current_height, leaves_num, total_leaves_height);

      std::cout << "number of leaves: " << leaves_num << std::endl;
      std::cout << "total leaves height: " << total_leaves_height << std::endl;

      return total_leaves_height * 1.0 / leaves_num;
    }



    // Ang: Check if a file exists
    bool fileExists(const std::string& filePath) {
        struct stat buffer;
        return (stat(filePath.c_str(), &buffer) == 0);
    }

    // Ang :Function to copy a file
    bool copyFile(const std::string& sourcePath, const std::string& destinationPath) {
      std::ifstream sourceFile(sourcePath, std::ios::binary);
      std::ofstream destinationFile(destinationPath, std::ios::binary);

      if (!sourceFile || !destinationFile) {
          return false; // Error in opening source or destination file
      }

      destinationFile << sourceFile.rdbuf();
      return true;
    }

    bool directoryExist(const std::string& path) {
      struct stat info;
      return stat(path.c_str(), &info) == 0 && (info.st_mode & S_IFDIR);
    }

    bool createDirectory(const std::string& path) {
      #ifdef _WIN32
          // Windows-specific code
          return _mkdir(path.c_str()) == 0;
      #else
          // Unix-like system code
          return mkdir(path.c_str(), 0777) == 0;
      #endif
    }

    // Ang: Function to copy all files from a source directory to a destination directory
    bool copyFilesInDirectory(const std::string& sourceDir, const std::string& destDir) {
      DIR* dir = opendir(sourceDir.c_str());

      if (dir == nullptr) {
          return false; // Unable to open the source directory
      }

      struct dirent* entry;

      while ((entry = readdir(dir)) != nullptr) {
          if (entry->d_type == DT_REG) { // Check if it's a regular file
              std::string sourcePath = sourceDir + "/" + entry->d_name;
              std::string destPath = destDir + "/" + entry->d_name;

              if (!copyFile(sourcePath, destPath)) {
                  closedir(dir);
                  return false; // Error while copying the file
              }
          }
      }

      closedir(dir);
      return true;
    }


    // Ang : update loggingFileStatus.txt
    void updateLoggingFileStatus(std::string loggingFileStatusPath) {
      if (loggingFileStatusPath.empty()) {
          loggingFileStatusPath = LOGGING_FILE_STATUS;
      }
      std::ofstream loggingFileStatus(loggingFileStatusPath); // Open in overwrite mode

      if (loggingFileStatus) {
          // Add three lines of content to the file (appending)
          loggingFileStatus << "log_file_path " << logs.log_file_path <<std::endl;
          std::string betree_root_name = get_betree_root_id();
          loggingFileStatus << "betree_root_id " << betree_root_name << std::endl;
          loggingFileStatus << "persist_lsn " << logs.lastPersistLSN << std::endl;
          loggingFileStatus << "checkpoint_lsn " << logs.lastCheckpointLSN << std::endl;
          // std::cout << "In updateLoggingFileStatus(), root_id: " << betree_root_name <<std::endl;
          
      } else {
          std::cerr << "Error creating the file: loggingFileStatus." << std::endl;
      }

      loggingFileStatus.close();
    }

    void updateLoggingFileStatus_lastPersistLSN(std::string loggingFileStatusPath, uint64_t newPersistLSN) {
      // Open the file in read mode
      std::ifstream inputFile(loggingFileStatusPath);
      
      // Read the existing content line by line
      std::stringstream updatedContent;
      std::string line;
      while (std::getline(inputFile, line)) {
          if (line.find("persist_lsn") != std::string::npos) {
              // Replace the line containing "persist_lsn" with the new value
              updatedContent << "persist_lsn " << newPersistLSN << std::endl;
          } else {
              // Keep the other lines unchanged
              updatedContent << line << std::endl;
          }
      }

      // Close the input file
      inputFile.close();

      // Open the file in write mode (truncate)
      std::ofstream outputFile(loggingFileStatusPath);

      if (!outputFile) {
          std::cerr << "Error opening the file for writing: " << loggingFileStatusPath << std::endl;
          return;
      }

      // Write the updated content back to the file
      outputFile << updatedContent.str();
      outputFile.close();
    }


    // Ang: deserialize loggingFileStatus.txt
    void deserializeLoggingFileStatus(std::string loggingFileStatusPath) {
      if (loggingFileStatusPath.empty()) {
          loggingFileStatusPath = LOGGING_FILE_STATUS;
      }
      std::ifstream loggingFileStatus(loggingFileStatusPath);

      if (loggingFileStatus) {
          std::string line;
          while (std::getline(loggingFileStatus, line)) {
              std::istringstream iss(line);
              std::string key, value;
              if (iss >> key >> value) {
                  if (key == "log_file_path") {
                      logs.log_file_path = value;
                      // std::cout << "log_file_path: " << logs.log_file_path << std::endl;
                  } else if (key == "betree_root_id") {
                      uint64_t root_id = std::stoull(value);
                      root.set_target(root_id); 
                      // std::cout << "root.target: " << root.get_target() << std::endl;
                  }else if (key == "persist_lsn") {
                      logs.lastPersistLSN = std::stoull(value);
                      // std::cout << "logs.lastPersistLSN: " << logs.lastPersistLSN << std::endl;
                  }else if (key == "checkpoint_lsn") {
                      logs.lastCheckpointLSN = std::stoull(value);
                      // std::cout << "logs.lastCheckpointLSN: " << logs.lastCheckpointLSN << std::endl;
                  }
              }
          }
      } else {
          std::cerr << "Error opening the file for deserialization." << std::endl;
      }

      loggingFileStatus.close();
    }


    // Ang: Do checkpoint
    // Firstly, flush the whole tree to disk
    // Secondly, write a checkpoint record in the log file and flush it to disk
    // At last, update loggingFileStatus.txt
    void checkpoint(Key k, Value v){
      //flush current in memory logs to disk
      logs.persist();

      // flush in memory dirty nodes to disk
      if(!directoryExist(DESTINATION_BACKUP_DIRECTORY)) {
        createDirectory(DESTINATION_BACKUP_DIRECTORY);
      }
      ss->flush_whole_tree(DESTINATION_BACKUP_DIRECTORY);

      // then add checkpoint record into log file, 
      // this make sure the current checkpoint has already finished when the current checkpoint record is written to disk 
      MessageKey<Key> key = MessageKey<Key>(k, next_timestamp++); 
      Message<Value> val = Message<Value>(CHECKPOINT_OPCODE, v);
      Op<Key, Value> op = Op<Key, Value>(key, val);
      logs.log(op);
      logs.persist(); 
      logs.lastCheckpointLSN = op.get_LSN();

      updateLoggingFileStatus(LOGGING_FILE_STATUS);

      //flush swap_space objects to disk, store the information needed when do recovery
      ss->serialize_objects(SWAPSPACE_OBJECTS_FILE);
      
      // test functionality
      // ss->clear_objects();
      // ss->deserialize_objects(SWAPSPACE_OBJECTS_FILE);

      // root.set_swap_space(nullptr);
      // root.set_target(0);

      // deserializeLoggingFileStatus(LOGGING_FILE_STATUS);
      // root.set_swap_space(ss);
      
    }

    void check_if_need_persist_or_checkpoint(Key k, Value v) {
      if (logs.log_counter % logs.checkpoint_granularity == 0) {
        checkpoint(k, v); 
      //   updateLoggingFileStatus(LOGGING_FILE_STATUS);
        std::cout << "do checkpoint, logs.lastCheckpointLSN is " << logs.lastCheckpointLSN << std::endl;
        return; // when doing checkpoint, there is no need to do persist() again, as we will flush all the logs in memory to disk in checkpoint();
      }
      if (logs.log_counter % logs.persistence_granularity == 0) {
        logs.persist();
        updateLoggingFileStatus_lastPersistLSN(LOGGING_FILE_STATUS, logs.lastPersistLSN);
        std::cout << "do persist, logs.lastPersistLSN is " << logs.lastPersistLSN << std::endl;
      //   test recovery()
      //   recovery(LOGGING_FILE_STATUS, SWAPSPACE_OBJECTS_FILE);
      //   std::cout << "do recovery, log_counter is " << logs.log_counter << std::endl;
      }
    }

    void redo(std::string& logFilePath, uint64_t lastCheckpointLSN, uint64_t lastPersistLSN) {
      std::ifstream logFile(logFilePath);
      if (!logFile.is_open()) {
          std::cerr << "Error opening log file." << std::endl;
          return;
      }

      uint64_t timestamp;
      uint64_t key;
      int opcode;
      std::string timestamp_str, key_str, opcode_str, dummy;

      std::string line;
      std::getline(logFile, line); // get the first line, but do not parse it
      while (std::getline(logFile, line)) {
          std::istringstream iss(line);
          iss >> timestamp_str >> key_str >> dummy >> opcode_str;
          timestamp = std::stoull(timestamp_str);
          key = std::stoull(key_str);
          opcode = std::stoi(opcode_str);
          
          if (timestamp > lastCheckpointLSN && timestamp <= lastPersistLSN) {
              // std::cout << "in redo, getline: " << timestamp << ", " << key << ", " << opcode << std::endl;
              upsert(opcode, key, std::to_string(key) + ":");
          }
          if (timestamp > lastPersistLSN) {
              break;
          }
      }

      logFile.close();
    }

    void recovery(std::string loggingFileStatusPath, std::string swapspaceObjectsFilePath) {
      bool loggingFileStatus_exist = fileExists(loggingFileStatusPath);
      bool objects_exist = fileExists(swapspaceObjectsFilePath);

      // if any of these files needed for recovery does not exist, return directly
      if (!loggingFileStatus_exist || !objects_exist) { 
          return;
      }

      // copy all the files from sourceDir to destinationDir
      std::string sourceDir = "tmpdir_backup";  
      std::string destinationDir = "tmpdir"; 

      if (copyFilesInDirectory(sourceDir, destinationDir)) {
          std::cout << "Files copied successfully." << std::endl;
      } else {
          std::cerr << "Error copying files." << std::endl;
      }

      // !!! need to clear lru_pqueue, because the initialization of betree will add root node to lru_pqueue, but we do not need that when do recovery
      ss->clear_lru_pqueue();
      // if all the files needed exist, do recovery
      // 1. recovery objects in swap_space
      ss->deserialize_objects(swapspaceObjectsFilePath);
      uint64_t max_objects_id = ss->get_max_objects_id();
      std::cout << "In recovery, max_objects_id: " << max_objects_id << std::endl;
      // !!! set ss->next_id with max_objects_id + 1, it is very important to reset next_id to make sure the revcovery process are the following process of history record.
      ss->set_next_id(max_objects_id + 1);
      // 2. recovery betree root
      deserializeLoggingFileStatus(loggingFileStatusPath);
      set_next_timestamp(logs.lastPersistLSN + 1); // set next_timestamp;
      // ss->set_next_access_time(logs.lastPersistLSN + 1);
      // 3. redo from lastFlushLSN
      
      bool log_exist = fileExists(logs.log_file_path);
      if (log_exist) {
          redo(logs.log_file_path, logs.lastCheckpointLSN, logs.lastPersistLSN);
          std::cout << "In recovery, redo has finished." << std::endl;
      } else {
          std::cout << "In recovery, log file does not exist." << std::endl;
      }
    }


  // Insert the specified message and handle a split of the root if it
  // occurs.
  void upsert(int opcode, Key k, Value v)
  {
    // kosumi: logging here
    message_map tmp;
    MessageKey<Key> key = MessageKey<Key>(k, next_timestamp++); 
    Message<Value> val = Message<Value>(opcode, v);
    logs.log(Op<Key, Value>(key, val));
    tmp[key] = val;
    pivot_map new_nodes = root->flush(*this, tmp);
    if (new_nodes.size() > 0) {
      root = ss->allocate(new node);
      root->pivots = new_nodes;
    }

    // std::cout << "In upsert(), the number of elements in ss->objects is: " << ss->get_objects_size() << std::endl;
    // ss->print_objects_id();
    // Ang: check if we need persist or do checkpoint
    check_if_need_persist_or_checkpoint(k, v);
  }

  void insert(Key k, Value v)
  {
    upsert(INSERT, k, v);
  }

  void update(Key k, Value v)
  {
    upsert(UPDATE, k, v);
  }

  void erase(Key k)
  {
    upsert(DELETE, k, default_value);
  }
  
  Value query(Key k)
  {
    Value v = root->query(*this, k);
    return v;
  }

  void dump_messages(void) {
    std::pair<MessageKey<Key>, Message<Value> > current;

    std::cout << "############### BEGIN DUMP ##############" << std::endl;
    
    try {
      current = root->get_next_message(NULL);
      do { 
	std::cout << current.first.key       << " "
		  << current.first.timestamp << " "
		  << current.second.opcode   << " "
		  << current.second.val      << std::endl;
	current = root->get_next_message(&current.first);
      } while (1);
    } catch (std::out_of_range e) {}
  }

  class iterator {
  public:

    iterator(const betree &bet)
      : bet(bet),
	position(),
	is_valid(false),
	pos_is_valid(false),
	first(),
	second()
    {}

    iterator(const betree &bet, const MessageKey<Key> *mkey)
      : bet(bet),
	position(),	
	is_valid(false),
	pos_is_valid(false),
	first(),
	second()
    {
      try {
	position = bet.root->get_next_message(mkey);
	pos_is_valid = true;
	setup_next_element();
      } catch (std::out_of_range & e) {}
    }

    void apply(const MessageKey<Key> &msgkey, const Message<Value> &msg) {
      switch (msg.opcode) {
      case INSERT:
  	first = msgkey.key;
  	second = msg.val;
  	is_valid = true;
  	break;
      case UPDATE:
  	first = msgkey.key;
  	if (is_valid == false)
  	  second = bet.default_value;
  	second = second + msg.val;
  	is_valid = true;
  	break;
      case DELETE:
  	is_valid = false;
  	break;
      default:
  	abort();
  	break;
      }
    }

    void setup_next_element(void) {
      is_valid = false;
      while (pos_is_valid && (!is_valid || position.first.key == first)) {
	apply(position.first, position.second);
	try {
	  position = bet.root->get_next_message(&position.first);
	} catch (std::exception & e) {
	  pos_is_valid = false;
	}
      }
    }

    bool operator==(const iterator &other) {
      return &bet == &other.bet &&
	is_valid == other.is_valid &&
	pos_is_valid == other.pos_is_valid &&
	(!pos_is_valid || position == other.position) &&
	(!is_valid || (first == other.first && second == other.second));
    }

    bool operator!=(const iterator &other) {
      return !operator==(other);
    }

    iterator &operator++(void) {
      setup_next_element();
      return *this;
    }
    
    const betree &bet;
    std::pair<MessageKey<Key>, Message<Value> > position;
    bool is_valid;
    bool pos_is_valid;
    Key first;
    Value second;
  };

  iterator begin(void) const {
    return iterator(*this, NULL);
  }

  iterator lower_bound(Key key) const {
    MessageKey<Key> tmp = MessageKey<Key>::range_start(key);
    return iterator(*this, &tmp);
  }
  
  iterator upper_bound(Key key) const {
    MessageKey<Key> tmp = MessageKey<Key>::range_end(key);
    return iterator(*this, &tmp);
  }
  
  iterator end(void) const {
    return iterator(*this);
  }
};
