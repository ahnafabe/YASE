add_executable(dummy_test dummy_test.cc)
target_link_libraries(dummy_test gtest glog gflags libdummy pthread) 
add_test(NAME dummy_test COMMAND dummy_test)
add_custom_target(dummy_test_custom COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS dummy_test)
