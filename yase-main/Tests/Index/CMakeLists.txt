add_executable(skiplist_test skiplist_test.cc)
target_link_libraries(skiplist_test gtest glog gflags gtest_main table indexmanager buffermanager pthread)
add_test(NAME skiplist_test COMMAND skiplist_test)
add_custom_target(skiplist_test_custom COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS skiplist_test)

