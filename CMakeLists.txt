cmake_minimum_required(VERSION 3.1)

# Determine if built as a subproject (using add_subdirectory) or directly
set(IS_MASTER_PROJECT FALSE)
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  set(IS_MASTER_PROJECT TRUE)
endif()

project(dfelibs LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

# define header-only library
add_library(dfelibs INTERFACE)
target_include_directories(dfelibs INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

# examples
add_subdirectory(examples)

