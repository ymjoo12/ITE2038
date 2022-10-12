cmake -S . -B build
cmake --build build
cd build
bin/disk_based_db
ctest
