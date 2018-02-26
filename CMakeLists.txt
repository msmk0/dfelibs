cmake_minimum_required(VERSION 3.1)

# Check if being used directly or via add_subdirectory
set(dfelibs_MASTER_PROJECT FALSE)
if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  set(dfelibs_MASTER_PROJECT TRUE)
endif()

project(dfelibs LANGUAGES CXX)

option(dfelibs_BUILD_EXAMPLES "Build examples" ${dfelibs_MASTER_PROJECT})

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

# define header-only library
add_library(dfelibs INTERFACE)
target_include_directories(dfelibs INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

if(dfelibs_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

