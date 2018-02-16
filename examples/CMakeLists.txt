set(dfetools_examples namedtuple)

foreach(_name ${dfetools_examples})
  set(_target "dfetools_example_${_name}")
  add_executable(${_target} "${_name}.cpp")
  target_link_libraries(${_target} PRIVATE dfetools)
endforeach()
