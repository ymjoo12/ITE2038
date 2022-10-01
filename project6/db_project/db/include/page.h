#ifndef DB_PAGE_H_
#define DB_PAGE_H_


/// Includes
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <deque>
#include <string>
#include <algorithm>
#include <iostream>


/// Constants
constexpr size_t INITIAL_DB_FILE_SIZE = 10 * 1024 * 1024;                 // 10 MiB
constexpr size_t PAGE_SIZE = 4 * 1024;                                    // 4096 B
constexpr size_t HEADER_SIZE = 128;                                       // 128 B
constexpr size_t BODY_SIZE = PAGE_SIZE - HEADER_SIZE;                     // 3968 B
constexpr size_t SLOT_SIZE = 16;                                          // 12 B
constexpr size_t EDGE_SIZE = 16;                                          // 16 B
constexpr size_t VALUE_MAX_SIZE = 108;                                    // 112 B
constexpr size_t VALUE_MIN_SIZE = 46;                                     // 50 B
constexpr size_t RECORD_THRESHOLD = 2500;                                 // 2500 B
constexpr int EDGE_MAX_COUNT = BODY_SIZE/EDGE_SIZE;                       // 248
constexpr int RECORD_MAX_COUNT = BODY_SIZE/(SLOT_SIZE+VALUE_MIN_SIZE);    // 64

/// Flags
namespace FLAG
{
  constexpr int SUCCESS = 0;      // Success
  constexpr int FAILURE = 1;      // Failure (invalid arguments, etc.)
  constexpr int FATAL = -1;       // Fatal error (out of memory, etc.)
  constexpr int ABORTED = 2;      // Aborted (Failure to acquire lock)
}


/// Type definition
using pagenum_t = uint64_t;


/// In-memory data types

// In-memory page structure for buffer
struct page_t
{
  // field
  char data[PAGE_SIZE];

  // constructor
  page_t();
  page_t(const page_t& copy);

  // operator, member functions
  bool operator==(const page_t& other);
  bool empty() const;
};

// In-memory page structure to manage page data (header page, free page)
// Only used in Disk Space Manager
struct HeaderPage
{
  // field
  pagenum_t next_free_page_number;      // common for header and free page
  pagenum_t num_of_pages;               // reserved in free page
  pagenum_t root_page_number;           // reserved in free page
  char reserved[PAGE_SIZE - 24];        // reserved

  // constructor
  HeaderPage();
  HeaderPage(const page_t& copy);

  // member functions and operators
  operator page_t();

};

// In-memory key pair
struct KeyPair
{
  // filed
  int64_t key;       // 8 bytes

  // constructor
  KeyPair();
  KeyPair(const KeyPair& copy);
  KeyPair(const int64_t key);

  // member functions and operators
  bool operator==(const KeyPair& other);
  bool operator<(const KeyPair& other);
};

// In-memory record struct to manage record data (leaf page)
// Only used in B Plus Tree
struct Record : public KeyPair
{
  // fields
  // int64_t key;      // 8 bytes (KeyPair)
  uint16_t size;       // 2 bytes
  uint16_t offset;     // 2 bytes
  int trx_id;          // 4 bytes
  std::string value;   // variable size (46 ~ 108 bytes)

  // constructor and destructor
  Record();
  Record(const int64_t key, const std::string value, const uint16_t offset = 0);
  Record(const Record& copy);
  Record(const page_t* src, int index);

  // member functions and operators
  bool operator==(const Record& other);
  bool operator<(const Record& other);
  void copyTo(page_t* dest, int index);
};

// each pair (key, page number) of body for internal page (16 bytes)
// Only used in B Plus Tree
struct Edge : public KeyPair
{
  // fields
  // int64_t key;          // 8 bytes (KeyPair)
  pagenum_t page_number;   // 8 bytes

  // constructor and destructor
  Edge();
  Edge(const int64_t key, const pagenum_t page_number);
  Edge(const Edge& copy);
  Edge(const page_t* src, int index);

  // member functions and operators
  bool operator==(const Edge& other);
  bool operator<(const Edge& other);
  void copyTo(page_t* dest, int index);
};

// In-memory page struct to manage page data (internal page, leaf page)
// Only used in B Plus Tree
struct NodePage
{
  // header structure of node page for leaf and internal page (128 bytes)
  struct page_header_t
  {
    pagenum_t parent_page_number;           // 8 bytes
    uint32_t is_leaf;                       // 4 bytes
    uint32_t number_of_keys;                // 4 bytes
    char reserved_8[8];                     // 8 bytes (reserved)
    int64_t page_lsn;                      // 8 bytes     
    char reserved_80[HEADER_SIZE - 48];     // 80 bytes (reserved)
    uint64_t amount_of_free_space;          // 8 bytes (reserved in internal page)
    union {
      pagenum_t right_sibling_page_number;  // for leaf page
      pagenum_t first_child_page_number;    // for internal page
    };                                      // 8 bytes
  };

  // fields
  page_header_t header;        // header (128 bytes)
  std::deque<Edge> edges;      // body for internal page (fixed max length: 248)
  std::deque<Record> slots;    // body for leaf page (variable max length: 32 ~ 64)

  // constructor and destructor
  NodePage(bool is_leaf = true);
  NodePage(const NodePage& copy);
  NodePage(const page_t& copy);

  // member functions and operator
  NodePage operator=(const page_t& other);
  operator page_t();
  bool operator==(const NodePage& other);
};


/// Operators
bool operator==(const KeyPair& keyPair, const int64_t key);
bool operator==(const int64_t key, const KeyPair& keyPair);
bool operator<(const KeyPair& keyPair, const int64_t key);
bool operator<(const int64_t key, const KeyPair& keyPair);


#endif  // DB_PAGE_H_
