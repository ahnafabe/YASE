cmake_minimum_required(VERSION 3.10)
project(yase)

add_compile_definitions(GLOG_USE_GLOG_EXPORT)

# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "" FORCE)
  message("-- No build mode chosen - using Debug by default")
endif(NOT CMAKE_BUILD_TYPE)

enable_testing()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -lpthread -DPAGE_SIZE=4096 -DSKIP_LIST_MAX_LEVEL=8")

include_directories(${CMAKE_SOURCE_DIR})

add_subdirectory(Dummy)
add_subdirectory(Tests)
add_subdirectory(Storage)
add_subdirectory(Index)
add_subdirectory(Lock)
add_subdirectory(Log)
