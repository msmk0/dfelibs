function(add_example _name)
  set(_target "${PROJECT_NAME}_example_${_name}")
  add_executable(${_target} "example_${_name}.cpp")
  target_link_libraries(${_target} PRIVATE dfelibs)
endfunction()

add_example(dispatcher)
add_example(namedtuple)
