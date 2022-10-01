#include "page.h"
#include "file.h"
#include "debug_util.h"
#include "test_util.h"
#include <gtest/gtest.h>
using namespace std;

/// Tests
// Record Test
TEST(RecordTest, CheckRecordInMemory)
{
    Record recordA, recordB;
    TestUtil::FillRecord(recordA);
    DebugUtil::PrintRecord(recordA);

    page_t page;
    // int16_t slot_offset = HEADER_SIZE;
    recordA.copyTo(&page, 0);

    recordB = Record(&page, 0);
    DebugUtil::PrintRecord(recordB);
    EXPECT_TRUE(recordA == recordB);
}

TEST(RecordTest, CheckRecordOnDisk)
{
    remove(TestUtil::TEST_FILE_PATH.c_str());
    int64_t table_id = file_open_table_file(TestUtil::TEST_FILE_PATH.c_str());
    pagenum_t pagenum = file_alloc_page(table_id);

    Record recordA, recordB;
    TestUtil::FillRecord(recordA);
    DebugUtil::PrintRecord(recordA);

    page_t pageA, pageB;
    // int16_t slot_offset = HEADER_SIZE;
    recordA.copyTo(&pageA, 0);
    file_write_page(table_id, pagenum, &pageA);

    file_read_page(table_id, pagenum, &pageB);
    EXPECT_TRUE(pageA == pageB);

    recordB = Record(&pageB, 0);
    DebugUtil::PrintRecord(recordB);
    EXPECT_TRUE(recordA == recordB);

    file_close_table_files();
}

TEST(RecordTest, CheckRecordOtherTable)
{
    remove(TestUtil::TEST_FILE_PATH.c_str());
    remove((TestUtil::TEST_FILE_PATH+"2").c_str());
    int64_t table_id_1 = file_open_table_file(TestUtil::TEST_FILE_PATH.c_str());
    int64_t table_id_2 = file_open_table_file((TestUtil::TEST_FILE_PATH+"2").c_str());
    pagenum_t pagenum_1 = file_alloc_page(table_id_1);
    pagenum_t pagenum_2 = file_alloc_page(table_id_2);

    Record recordA, recordB, recordC;
    TestUtil::FillRecord(recordA);

    page_t pageA, pageB, pageC;
    // int16_t slot_offset = HEADER_SIZE;
    recordA.copyTo(&pageA, 0);
    file_write_page(table_id_1, pagenum_1, &pageA);

    file_read_page(table_id_1, pagenum_1, &pageB);
    file_write_page(table_id_2, pagenum_2, &pageB);

    file_read_page(table_id_2, pagenum_2, &pageC);
    EXPECT_TRUE(pageA == pageB);
    EXPECT_TRUE(pageA == pageC);

    recordB = Record(&pageB, 0);
    recordC = Record(&pageC, 0);

    EXPECT_TRUE(recordA == recordB);
    EXPECT_TRUE(recordA == recordC);

    file_close_table_files();

    DebugUtil::PrintRecord(recordA);
    DebugUtil::PrintRecord(recordB);
    DebugUtil::PrintRecord(recordC);
}

// Page Test
class PageTest : public ::testing::Test {
protected:
    int64_t table_id;
    const char TEST_VALUE = 'a';
    void SetUp() override {
        remove(TestUtil::TEST_FILE_PATH.c_str());
        table_id = file_open_table_file(TestUtil::TEST_FILE_PATH.c_str());
        int table_file = opened_tables.getFileDesc(table_id);
        ASSERT_GT(table_file, 0);
        EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));
    }
    void TearDown() override {
        file_close_table_files();
        EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));
    }
};

// Internal Page
TEST_F(PageTest, InternalPageInMemory)
{
    pagenum_t pageNum;
    NodePage pageSrc, pageDest;
    page_t pageBuf;

    pageNum = file_alloc_page(table_id);
    EXPECT_FALSE(TestUtil::IsFreedPage(table_id, pageNum));

    TestUtil::FillInternalPage(pageSrc);
    DebugUtil::PrintPage(pageSrc);

    pageBuf = pageSrc;
    DebugUtil::PrintPageBytes(pageBuf);

    pageDest = NodePage(pageBuf);
    DebugUtil::PrintPage(pageDest);

    EXPECT_TRUE(pageSrc == pageDest);
}

TEST_F(PageTest, InternalPageOnDisk)
{
    pagenum_t pageNum;
    NodePage pageSrc, pageDest;
    page_t pageBufSrc, pageBufDest;

    pageNum = file_alloc_page(table_id);
    EXPECT_FALSE(TestUtil::IsFreedPage(table_id, pageNum));

    TestUtil::FillInternalPage(pageSrc);
    DebugUtil::PrintPage(pageSrc);

    pageBufSrc = pageSrc;
    file_write_page(table_id, pageNum, &pageBufSrc);
    DebugUtil::PrintPageBytes(pageBufSrc);

    file_read_page(table_id, pageNum, &pageBufDest);
    DebugUtil::PrintPageBytes(pageBufDest);

    EXPECT_TRUE(pageBufSrc == pageBufDest);

    pageDest = NodePage(pageBufDest);
    DebugUtil::PrintPage(pageDest);

    EXPECT_TRUE(pageSrc == pageDest);
}

TEST_F(PageTest, InternalPageOtherTable)
{

    int64_t table_id_1 = file_open_table_file(TestUtil::TEST_FILE_PATH.c_str());
    int64_t table_id_2 = file_open_table_file((TestUtil::TEST_FILE_PATH+"2").c_str());
    pagenum_t pagenum_1 = file_alloc_page(table_id_1);
    pagenum_t pagenum_2 = file_alloc_page(table_id_2);

    NodePage pageSrc, pageMid, pageDest;
    page_t pageBufSrc, pageBufMid, pageBufDest;

    cout << "\n[[ PAGE SRC ]]\n";

    TestUtil::FillInternalPage(pageSrc);
    DebugUtil::PrintPage(pageSrc);

    pageBufSrc = pageSrc;

    file_write_page(table_id_1, pagenum_1, &pageBufSrc);
    DebugUtil::PrintPageBytes(pageBufSrc);

    cout << "\n[[ PAGE MID ]]\n";

    file_read_page(table_id_1, pagenum_1, &pageBufMid);
    pageMid = NodePage(pageBufMid);
    DebugUtil::PrintPage(pageMid);
    DebugUtil::PrintPageBytes(pageBufMid);

    pageBufMid = pageMid;
    file_write_page(table_id_2, pagenum_2, &pageBufMid);
    DebugUtil::PrintPageBytes(pageBufMid);

    cout << "\n[[ PAGE DEST ]]\n";

    file_read_page(table_id_2, pagenum_2, &pageBufDest);
    DebugUtil::PrintPageBytes(pageBufDest);

    pageDest = NodePage(pageBufDest);
    DebugUtil::PrintPage(pageDest);

    pageBufDest = pageDest;


    EXPECT_TRUE(pageBufSrc == pageBufDest);
    EXPECT_TRUE(pageBufSrc == pageBufMid);

    EXPECT_TRUE(pageSrc == pageDest);
    EXPECT_TRUE(pageSrc == pageMid);
}

// Leaf Page
TEST_F(PageTest, LeafPageInMemory)
{
    pagenum_t pageNum;
    NodePage pageSrc, pageDest;
    page_t pageBuf;

    pageNum = file_alloc_page(table_id);
    EXPECT_FALSE(TestUtil::IsFreedPage(table_id, pageNum));

    TestUtil::FillLeafPage(pageSrc);
    DebugUtil::PrintPage(pageSrc);

    pageBuf = pageSrc;
    DebugUtil::PrintPageBytes(pageBuf);

    pageDest = NodePage(pageBuf);
    DebugUtil::PrintPage(pageDest, true);

    EXPECT_TRUE(pageSrc == pageDest);
}

TEST_F(PageTest, LeafPageOnDisk)
{
    pagenum_t pageNum;
    NodePage pageSrc, pageDest;
    page_t pageBufSrc, pageBufDest;

    pageNum = file_alloc_page(table_id);
    EXPECT_FALSE(TestUtil::IsFreedPage(table_id, pageNum));

    TestUtil::FillLeafPage(pageSrc);
    DebugUtil::PrintPage(pageSrc);

    pageBufSrc = pageSrc;
    file_write_page(table_id, pageNum, &pageBufSrc);
    DebugUtil::PrintPageBytes(pageBufSrc);

    file_read_page(table_id, pageNum, &pageBufDest);
    DebugUtil::PrintPageBytes(pageBufDest);

    EXPECT_TRUE(pageBufSrc == pageBufDest);

    pageDest = NodePage(pageBufDest);
    DebugUtil::PrintPage(pageDest);

    EXPECT_TRUE(pageSrc == pageDest);
}

TEST_F(PageTest, LeafPageOtherTable)
{

    int64_t table_id_1 = file_open_table_file(TestUtil::TEST_FILE_PATH.c_str());
    int64_t table_id_2 = file_open_table_file((TestUtil::TEST_FILE_PATH+"2").c_str());
    pagenum_t pagenum_1 = file_alloc_page(table_id_1);
    pagenum_t pagenum_2 = file_alloc_page(table_id_2);

    NodePage pageSrc, pageMid, pageDest;
    page_t pageBufSrc, pageBufMid, pageBufDest;

    cout << "\n[[ PAGE SRC ]]\n";

    TestUtil::FillLeafPage(pageSrc);
    DebugUtil::PrintPage(pageSrc);

    pageBufSrc = pageSrc;

    file_write_page(table_id_1, pagenum_1, &pageBufSrc);
    DebugUtil::PrintPageBytes(pageBufSrc);

    cout << "\n[[ PAGE MID ]]\n";

    file_read_page(table_id_1, pagenum_1, &pageBufMid);
    pageMid = NodePage(pageBufMid);
    DebugUtil::PrintPage(pageMid);
    DebugUtil::PrintPageBytes(pageBufMid);

    pageBufMid = pageMid;
    file_write_page(table_id_2, pagenum_2, &pageBufMid);
    DebugUtil::PrintPageBytes(pageBufMid);

    cout << "\n[[ PAGE DEST ]]\n";

    file_read_page(table_id_2, pagenum_2, &pageBufDest);
    DebugUtil::PrintPageBytes(pageBufDest);

    pageDest = NodePage(pageBufDest);
    DebugUtil::PrintPage(pageDest);

    pageBufDest = pageDest;


    EXPECT_TRUE(pageBufSrc == pageBufDest);
    EXPECT_TRUE(pageBufSrc == pageBufMid);

    EXPECT_TRUE(pageSrc == pageDest);
    EXPECT_TRUE(pageSrc == pageMid);
}