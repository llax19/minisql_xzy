# test123的测试脚本

cd build
make disk_manager_test
./test/disk_manager_test

make buffer_pool_manager_test
./test/buffer_pool_manager_test

make lru_replacer_test
./test/lru_replacer_test

make b_plus_tree_test
./test/b_plus_tree_test

make table_heap_test
./test/table_heap_test

make tuple_test
./test/tuple_test
#若出现问题报错

make index_iterator_test
./test/index_iterator_test

make b_plus_tree_index_test
./test/b_plus_tree_index_test

make catalog_test
./test/catalog_test

make executor_test
./test/executor_test