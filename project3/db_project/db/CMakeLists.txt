# Sources
set(DB_SOURCE_DIR src)
set(DB_SOURCES
  ${DB_SOURCE_DIR}/page.cc
  ${DB_SOURCE_DIR}/file_util.cc
  ${DB_SOURCE_DIR}/file.cc
  ${DB_SOURCE_DIR}/buffer.cc
  ${DB_SOURCE_DIR}/log_util.cc
  ${DB_SOURCE_DIR}/bpt.cc
  ${DB_SOURCE_DIR}/api.cc
)

# Headers
set(DB_HEADER_DIR include)
set(DB_HEADERS
  ${DB_HEADER_DIR}/page.h
  ${DB_HEADER_DIR}/file_util.h
  ${DB_HEADER_DIR}/file.h
  ${DB_HEADER_DIR}/buffer.h
  ${DB_HEADER_DIR}/log_util.h
  ${DB_HEADER_DIR}/bpt.h
  ${DB_HEADER_DIR}/api.h
)

add_library(db STATIC ${DB_HEADERS} ${DB_SOURCES})

target_include_directories(db
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/${DB_HEADER_DIR}"
  )

