set(examples dispatcher namedtuple)

foreach(_name ${examples})
  set(_target "${PROJECT_NAME}_example_${_name}")
  add_executable(${_target} "${_name}.cpp")
  target_link_libraries(${_target} PRIVATE dfelibs)
endforeach()
