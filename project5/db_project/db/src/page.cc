#include "page.h"

/// page_t
page_t::page_t()
{
    memset(this->data, 0, PAGE_SIZE);
}

page_t::page_t(const page_t& copy)
{
    memcpy(this->data, copy.data, PAGE_SIZE);
    // for (int byte = 0; byte < PAGE_SIZE; byte++) {
    //     this->data[byte] = copy.data[byte];
    // }
}

bool page_t::operator==(const page_t& other)
{
    for (int byte = 0; byte < PAGE_SIZE; byte++) {
        if (this->data[byte] != other.data[byte]) return false;
    }
    return true;
}

bool page_t::empty() const
{
    for (int byte = 0; byte < PAGE_SIZE; byte++) {
        if (this->data[byte] != 0) return false;
    }
    return true;
}


/// Header Page
HeaderPage::HeaderPage()
    : next_free_page_number(0), num_of_pages(0), root_page_number(0)
{
    // init data
    memset(this->reserved, 0, PAGE_SIZE - 24);
}

HeaderPage::HeaderPage(const page_t& copy)
{
    int16_t offset = 0;

    // copy data
    memcpy(&this->next_free_page_number, copy.data+offset, sizeof(pagenum_t));
    offset += sizeof(pagenum_t);
    memcpy(&this->num_of_pages, copy.data+offset, sizeof(pagenum_t));
    offset += sizeof(pagenum_t);
    memcpy(&this->root_page_number, copy.data+offset, sizeof(pagenum_t));
    offset += sizeof(pagenum_t);
    memcpy(&this->reserved, copy.data+offset, PAGE_SIZE - 24);
}

HeaderPage::operator page_t()
{
    page_t copy;
    int16_t offset = 0;

    // copy data
    memcpy(copy.data+offset, &this->next_free_page_number, sizeof(pagenum_t));
    offset += sizeof(pagenum_t);
    memcpy(copy.data+offset, &this->num_of_pages, sizeof(pagenum_t));
    offset += sizeof(pagenum_t);
    memcpy(copy.data+offset, &this->root_page_number, sizeof(pagenum_t));
    offset += sizeof(pagenum_t);
    memcpy(copy.data+offset, &this->reserved, PAGE_SIZE - 24);

    return copy;
}


/// Key Pair
KeyPair::KeyPair()
    : key(0)
{}

KeyPair::KeyPair(const KeyPair& copy)
    : key(copy.key)
{}

KeyPair::KeyPair(const int64_t key)
    : key(key)
{}

bool KeyPair::operator==(const KeyPair& other)
{
    return this->key == other.key;
}

bool KeyPair::operator<(const KeyPair& other)
{
    return this->key < other.key;
}


/// In-memory record struct to modify page data (leaf page)
// constructors
Record::Record()
    : KeyPair(0), size(0), offset(0), trx_id(0)
{}

Record::Record(const int64_t key, const std::string value, const uint16_t offset)
    : KeyPair(key), size(value.size()), offset(offset), trx_id(0),
    value(value)
{}

Record::Record(const Record& copy)
    : KeyPair(copy.key), size(copy.size), offset(copy.offset), trx_id(copy.trx_id),
    value(copy.value)
{}

Record::Record(const page_t* src, int index)
{   
    int slot_offset = HEADER_SIZE + index * SLOT_SIZE;

    memcpy(&(this->key),src->data+slot_offset,sizeof(int64_t));
    slot_offset += sizeof(int64_t);
    memcpy(&(this->size),src->data+slot_offset,sizeof(uint16_t));
    slot_offset += sizeof(uint16_t);
    memcpy(&(this->offset),src->data+slot_offset,sizeof(uint16_t));
    slot_offset += sizeof(uint16_t);
    memcpy(&(this->trx_id),src->data+slot_offset,sizeof(int));

    char* valueToCopy = new char[this->size+1];
    memset(valueToCopy, 0, this->size+1);
    memcpy(valueToCopy,src->data+this->offset,this->size);
    this->value = std::string(valueToCopy);
    delete[] valueToCopy;
}

// member functions and operator
bool Record::operator==(const Record& other)
{
    return (
        this->key == other.key && 
        this->value == other.value
    );
}

void Record::copyTo(page_t* dest, int index)
{
    int slot_offset = HEADER_SIZE + index * SLOT_SIZE;

    memcpy(dest->data+slot_offset, &this->key, sizeof(int64_t));
    slot_offset += sizeof(int64_t);
    memcpy(dest->data+slot_offset, &this->size, sizeof(uint16_t));
    slot_offset += sizeof(uint16_t);
    memcpy(dest->data+slot_offset, &this->offset, sizeof(uint16_t));
    slot_offset += sizeof(uint16_t);
    memcpy(dest->data+slot_offset, &this->trx_id, sizeof(int));

    std::copy(this->value.begin(),this->value.begin()+this->size,dest->data+this->offset);
}


/// In-memory edge (key - page number pair) struct to modify page data (internal page)
// constructors
Edge::Edge()
    : KeyPair(0), page_number(0) 
{}

Edge::Edge(const int64_t key, const pagenum_t page_number)
    : KeyPair(key), page_number(page_number)
{}

Edge::Edge(const Edge& copy)
    : KeyPair(copy.key), page_number(copy.page_number)
{}

Edge::Edge(const page_t* src, int index)
{
    int edge_offset = HEADER_SIZE + index * EDGE_SIZE;

    memcpy(&(this->key),src->data+edge_offset,sizeof(int64_t));
    edge_offset += sizeof(int64_t);
    memcpy(&(this->page_number),src->data+edge_offset,sizeof(pagenum_t));
    edge_offset += sizeof(pagenum_t);
}


// member functions and operator
bool Edge::operator==(const Edge& other)
{
    return (
        this->key == other.key &&
        this->page_number == other.page_number
    );
}

bool Edge::operator<(const Edge& other)
{
    return this->key < other.key;
}

void Edge::copyTo(page_t* dest, int index)
{
    int edge_offset = HEADER_SIZE + index * EDGE_SIZE;

    memcpy(dest->data+edge_offset,&this->key,sizeof(int64_t));
    edge_offset += sizeof(int64_t);
    memcpy(dest->data+edge_offset,&this->page_number,sizeof(pagenum_t));
}


/// In-memory page class to modify page data (internal page, leaf page)
// constructors
NodePage::NodePage(bool is_leaf)
{
    // initialize header data
    this->header.parent_page_number = 0;
    this->header.is_leaf = is_leaf ? 1 : 0;
    this->header.number_of_keys = 0;
    memset(this->header.reserved, 0, HEADER_SIZE - 32);
    this->header.amount_of_free_space = is_leaf ? (PAGE_SIZE - HEADER_SIZE) : 0;
    this->header.right_sibling_page_number = 0;

    // initialize body data
    this->edges.clear();
    this->slots.clear();
}

NodePage::NodePage(const NodePage& copy)
{
    // copy header data
    this->header.parent_page_number = copy.header.parent_page_number;
    this->header.is_leaf = copy.header.is_leaf;
    this->header.number_of_keys = copy.header.number_of_keys;
    this->header.amount_of_free_space = copy.header.amount_of_free_space;
    this->header.right_sibling_page_number = copy.header.right_sibling_page_number;

    // copy body data
    if (this->header.is_leaf) {
        this->slots = std::deque<Record>(copy.slots);
    } else {
        this->edges = std::deque<Edge>(copy.edges);
    }
}

NodePage::NodePage(const page_t& copy)
{
    // copy header data
    memcpy(&(this->header), copy.data, sizeof(NodePage::page_header_t));

    // copy body data
    if (this->header.is_leaf) {
        for (int idx = 0; idx < this->header.number_of_keys; idx++) {
            Record record(&copy, idx);
            this->slots.push_back(record);
        }
    } else {
        for (int idx = 0; idx < this->header.number_of_keys; idx++) {
            Edge edge(&copy, idx);
            this->edges.push_back(edge);
        }
    }
}


// member functions and operator
NodePage NodePage::operator=(const page_t& other)
{
    return NodePage(other);
}

NodePage::operator page_t()
{
    int16_t offset = 0;
    page_t copy;

    // copy header data
    memcpy(&(copy.data),&(this->header),sizeof(NodePage::page_header_t));
    offset += sizeof(NodePage::page_header_t);

    // copy body data
    if (this->header.is_leaf) {
        for (int idx = 0; idx < this->header.number_of_keys; idx++) {
            this->slots[idx].copyTo(&copy, idx);
        }
    } else {
        for (int idx = 0; idx < this->header.number_of_keys; idx++) {
            this->edges[idx].copyTo(&copy, idx);
        }
    }

    return copy;
}

bool NodePage::operator==(const NodePage& other)
{
    if (
        this->header.parent_page_number != other.header.parent_page_number ||
        this->header.is_leaf != other.header.is_leaf ||
        this->header.number_of_keys != other.header.number_of_keys ||
        this->header.right_sibling_page_number != other.header.right_sibling_page_number
    ) return false;

    if (this->header.is_leaf) {
        if (this->header.amount_of_free_space != other.header.amount_of_free_space) return false;
        for (int idx = 0; idx < this->header.number_of_keys; idx++) {
            if (!(this->slots[idx] == other.slots[idx])) return false;
        }
    } else {
        for (int idx = 0; idx < this->header.number_of_keys; idx++) {
            if (!(this->edges[idx] == other.edges[idx])) return false;
        }
    }

    return true;
}


/// operators
bool operator==(const KeyPair& keyPair, const int64_t key)
{
    return keyPair.key == key;
}

bool operator==(const int64_t key, const KeyPair& keyPair)
{
    return key == keyPair.key;
}


bool operator<(const KeyPair& keyPair, const int64_t key)
{
    return keyPair.key < key;
}

bool operator<(const int64_t key, const KeyPair& keyPair)
{
    return key < keyPair.key;
}

