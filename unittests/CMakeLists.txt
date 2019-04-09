# minimal version that provides test v3
find_package(Boost 1.59 REQUIRED COMPONENTS unit_test_framework)

set(unittests dispatcher histogram namedtuple poly)

foreach(_name ${unittests})
  set(_target "${PROJECT_NAME}_test_${_name}")
  add_executable(${_target} "test_${_name}.cpp")
  target_compile_definitions(${_target} PRIVATE DFE_UNITTESTS_DIRECTORY="${CMAKE_CURRENT_SOURCE_DIR}")
  target_compile_definitions(${_target} PRIVATE BOOST_TEST_MODULE="${_name}")
  target_compile_definitions(${_target} PRIVATE BOOST_TEST_DYN_LINK=1)
  target_include_directories(${_target} PRIVATE ${Boost_INCLUDE_DIR})
  target_link_libraries(${_target} PRIVATE dfelibs)
  target_link_libraries(${_target} PRIVATE ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY})
  add_test(NAME ${_name} COMMAND ${_target})
endforeach()
