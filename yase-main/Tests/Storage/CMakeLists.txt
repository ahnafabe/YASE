add_executable(basefile_test basefile_test.cc)
target_link_libraries(basefile_test gtest glog gflags basefile pthread) 
add_test(NAME basefile_test COMMAND basefile_test)
add_custom_target(basefile_test_custom COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS basefile_test)

add_executable(file_test file_test.cc)
target_link_libraries(file_test gtest glog gflags basefile file pthread)
add_test(NAME file_test COMMAND file_test)
add_custom_target(file_test_custom COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS file_test)

add_executable(buffer_manager_test buffer_manager_test.cc)
target_link_libraries(buffer_manager_test gtest glog gflags buffermanager file table)
add_test(NAME buffer_manager_test COMMAND buffer_manager_test)
add_custom_target(buffer_manager_test_custom COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS buffer_manager_test)

add_executable(table_test table_test.cc)
target_link_libraries(table_test gtest glog gflags buffermanager file table)
add_test(NAME table_test COMMAND table_test)
add_custom_target(table_test_custom COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS table_test)
