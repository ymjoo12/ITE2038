# GoogleTest
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/e4717df71a4f45bf9f0ac88c6cd9846a0bc248dd.zip)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Tests
set(DB_TEST_UTIL_DIR utils)
set(DB_TEST_DIR src)
set(DB_TESTS
  ${DB_TEST_UTIL_DIR}/test_util.h
  ${DB_TEST_UTIL_DIR}/test_util.cc
  ${DB_TEST_DIR}/page_test.cc
  ${DB_TEST_DIR}/file_test.cc
  ${DB_TEST_DIR}/index_test.cc
  # ${DB_TEST_DIR}/trx_test.cc
  # ${DB_TEST_DIR}/trx_test_adv.cc
)

include_directories(${DB_TEST_UTIL_DIR})
add_executable(db_test ${DB_TESTS})

target_link_libraries(
  db_test
  db
  gtest_main
)


include(GoogleTest)
gtest_discover_tests(db_test PROPERTIES TIMEOUT 1500)

