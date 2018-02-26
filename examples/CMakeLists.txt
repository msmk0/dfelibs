set(dfelibs_examples dispatcher namedtuple)

foreach(_name ${dfelibs_examples})
  set(_target "dfelibs_example_${_name}")
  add_executable(${_target} "${_name}.cpp")
  target_link_libraries(${_target} PRIVATE dfelibs)
endforeach()
