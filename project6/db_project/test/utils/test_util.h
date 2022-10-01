#include "page.h"
#include "file.h"
#include "index.h"

#include "file_util.h"
#include "bpt.h"

#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>

#include <iostream>
#include <algorithm>
#include <random>
#include <string>
#include <deque>
#include <filesystem>
#include <queue>
#include <fstream>
#include <iterator>
#include <tuple>


using namespace std;

namespace TestUtil
{
    // global variables
    const string SAMPLE_DATA = "A123456789B123456789C123456789D123456789E123456789F123456789G123456789H123456789I123456789J123456789";
    const string TEST_FILE_PATH = "TestFile.db";

    // file
    int GetFileSize(int64_t table_id);
    int GetNumberOfPages(int64_t table_id);
    int GetFirstFreePageNumber(int64_t table_id);

    // fill pages
    void FillRecord(Record& record);
    void FillEdge(Edge& edge);
    void FillInternalPage(NodePage& page);
    void FillLeafPage(NodePage& page);

    // print pages
    void PrintRecord(const Record& record);
    void PrintPageBytes(const page_t& page, bool verbose = false, int start = 0, int end = PAGE_SIZE);
    void PrintPage(const NodePage& page, bool verbose = false);
    void PrintEdge(const Edge& edge);

    // check validity
    bool IsFreedPage(int64_t table_id, int page_num);
    bool IsValidOpenedFile(int64_t table_id);
    bool IsValidClosedFile(int64_t table_id);

    // copy file
    void LoadDB(const std::string filename);
    void SaveDB(const std::string filename);
    int64_t GenRandomKey(int64_t min_value = numeric_limits<int64_t>::min(), int64_t max_value = numeric_limits<int64_t>::max());
    string GenRandomValue(int length = 0, int noise = 5);

    // compare files
    bool CompareFiles(const std::string& p1, const std::string& p2);
}