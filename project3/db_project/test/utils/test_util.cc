#include "test_util.h"

namespace TestUtil 
{
    int GetFileSize(int64_t table_id)
    {
        int table_file = opened_tables.getFileDesc(table_id);
        int file_size = lseek(table_file, 0, SEEK_END);
        return file_size;
    }

    int GetNumberOfPages(int64_t table_id)
    {
        HeaderPage header;
        int table_file = opened_tables.getFileDesc(table_id);
        pread(table_file, &header, PAGE_SIZE, 0);
        return header.num_of_pages;
    }

    int GetFirstFreePageNumber(int64_t table_id)
    {
        HeaderPage header;
        int table_file = opened_tables.getFileDesc(table_id);
        pread(table_file, &header, PAGE_SIZE, 0);
        return header.next_free_page_number;
    }

    void FillRecord(Record& record)
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dis(1, 999);

        const std::string SAMPLE_VALUE = SAMPLE_DATA + SAMPLE_DATA;

        record.key = dis(gen) + 1;
        record.size = SAMPLE_VALUE.length();
        record.offset = PAGE_SIZE - record.size - dis(gen);
        record.value = SAMPLE_VALUE;
    }

    void FillEdge(Edge& edge)
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dis(1, 999);

        edge.key = dis(gen);
        edge.page_number = dis(gen);
    }

    void FillInternalPage(NodePage& page)
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dis(0, 247);

        const pagenum_t parent_page_number = dis(gen);
        const uint32_t is_leaf = 0;
        const uint32_t number_of_keys = dis(gen) + 1;
        const pagenum_t first_child_page_number = parent_page_number + dis(gen) + 1;

        page.header.parent_page_number = parent_page_number;
        page.header.is_leaf = is_leaf;
        page.header.number_of_keys = number_of_keys;
        page.header.first_child_page_number = first_child_page_number;

        int64_t key = dis(gen) + 1;
        pagenum_t page_number = first_child_page_number + dis(gen) + 1;
        for (int idx = 0; idx < number_of_keys; idx++) {
            page.edges.push_back(Edge(key, page_number));
            key += dis(gen) + 1;
            page_number += dis(gen) + 1;
        }
    }

    void FillLeafPage(NodePage& page)
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dis(0, 63);

        const pagenum_t parent_page_number = dis(gen);
        const uint32_t is_leaf = 1;
        const uint32_t number_of_keys = dis(gen) + 1;
        const pagenum_t right_sibling_page_number = parent_page_number + dis(gen) + 1;

        page.header.parent_page_number = parent_page_number;
        page.header.is_leaf = is_leaf;
        page.header.number_of_keys = number_of_keys;
        page.header.right_sibling_page_number = right_sibling_page_number;
        int64_t key = dis(gen) + 1;
        int16_t size, offset = PAGE_SIZE;
        std::string value;

        const int16_t MAX_SIZE = (PAGE_SIZE - HEADER_SIZE)/number_of_keys;

        for (int idx = 0; idx < number_of_keys; idx++) {
            key += dis(gen) + 1;
            size = dis(gen) % MAX_SIZE + 1;
            value = SAMPLE_DATA.substr(dis(gen) % 10, size);
            offset -= size;
            page.slots.push_back(Record(key,value,offset));
        }

        const uint64_t amount_of_free_space = PAGE_SIZE - (
            HEADER_SIZE + 
            page.header.number_of_keys * SLOT_SIZE + 
            (page.header.number_of_keys > 0 ? PAGE_SIZE - page.slots.back().offset : 0)
        );
        page.header.amount_of_free_space = amount_of_free_space;
    }

    bool IsFreedPage(int64_t table_id, int page_num)
    {
        if (page_num <= 0) return false;

        bool flag = false;
        HeaderPage page_buffer;
        int table_file = opened_tables.getFileDesc(table_id);

        pread(table_file, &page_buffer, PAGE_SIZE, 0);
        for (int current_page = page_buffer.next_free_page_number; current_page > 0; current_page = page_buffer.next_free_page_number) {
            if (current_page == page_num) {
                flag = true;
                break;
            }
            pread(table_file, &page_buffer, PAGE_SIZE, PAGE_SIZE * current_page);
        }

        return flag;
    }

    bool IsValidOpenedFile(int64_t table_id)
    {
        int table_file = opened_tables.getFileDesc(table_id);
        int flag = fcntl(table_file, F_GETFL, 0) & O_ACCMODE;
        return flag == O_RDWR && table_file > 0;
    }

    bool IsValidClosedFile(int64_t table_id)
    {
        int table_file = opened_tables.getFileDesc(table_id);
        return table_file < 0;
    }

    void LoadDB(const std::string filename)
    {
        filesystem::path origin = filename;
        filesystem::path dest = TEST_FILE_PATH;
        remove(TEST_FILE_PATH.c_str());
        copy(origin, dest);
    }
    
    void SaveDB(const std::string filename)
    {
        filesystem::path origin = TEST_FILE_PATH;
        filesystem::path dest = filename;
        remove(filename.c_str());
        copy(origin, dest);
    }

    int64_t GenRandomKey(int64_t min_value, int64_t max_value)
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int64_t> dis(min_value, max_value);

        int64_t key_to_return = dis(gen);

        return key_to_return;
    }

    std::string GenRandomValue(int noise)
    {
        static const char charset[] =
            "0123456789+/"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dis(VALUE_MIN_SIZE-noise, VALUE_MAX_SIZE+noise);

        std::string value_to_return;
        int len = dis(gen);
        value_to_return.reserve(len);

        for (int i = 0; i < len; i++) {
            value_to_return += charset[dis(gen) % (sizeof(charset) - 1)];
        }
        
        return value_to_return;
    }

    bool CompareFiles(const std::string& p1, const std::string& p2) 
    {
        std::ifstream f1(p1, std::ifstream::binary | std::ifstream::ate);
        std::ifstream f2(p2, std::ifstream::binary | std::ifstream::ate);

        // Check file problem
        if (f1.fail() || f2.fail()) {
            return false; 
        }

        // Check size mismatch
        if (f1.tellg() != f2.tellg()) {
            return false; 
        }

        // seek back to beginning and use std::equal to compare contents
        f1.seekg(0, std::ifstream::beg);
        f2.seekg(0, std::ifstream::beg);
        return std::equal(
            std::istreambuf_iterator<char>(f1.rdbuf()),
            std::istreambuf_iterator<char>(),
            std::istreambuf_iterator<char>(f2.rdbuf())
        );
    }
}