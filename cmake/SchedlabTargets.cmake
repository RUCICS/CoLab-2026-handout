function(schedlab_target_base_includes target_name)
  target_include_directories(${target_name}
    PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}
      ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
endfunction()

function(schedlab_target_student_includes target_name)
  target_include_directories(${target_name}
    PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}
      ${CMAKE_CURRENT_SOURCE_DIR}/include
      ${CMAKE_CURRENT_SOURCE_DIR}/student
  )
endfunction()

function(schedlab_add_binary target_name)
  set(options WITH_STUDENT)
  set(oneValueArgs RUNTIME_OUTPUT_DIRECTORY)
  set(multiValueArgs SOURCES LIBRARIES DEFINITIONS DEPENDS)
  cmake_parse_arguments(ARG "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  add_executable(${target_name} ${ARG_SOURCES})
  if (ARG_WITH_STUDENT)
    schedlab_target_student_includes(${target_name})
  else()
    schedlab_target_base_includes(${target_name})
  endif()
  if (ARG_LIBRARIES)
    target_link_libraries(${target_name} PRIVATE ${ARG_LIBRARIES})
  endif()
  if (ARG_DEFINITIONS)
    target_compile_definitions(${target_name} PRIVATE ${ARG_DEFINITIONS})
  endif()
  if (ARG_DEPENDS)
    add_dependencies(${target_name} ${ARG_DEPENDS})
  endif()
  if (ARG_RUNTIME_OUTPUT_DIRECTORY)
    set_target_properties(${target_name} PROPERTIES
      RUNTIME_OUTPUT_DIRECTORY "${ARG_RUNTIME_OUTPUT_DIRECTORY}"
    )
  endif()
endfunction()

function(schedlab_add_test_target target_name)
  set(options INTERNAL_ONLY WITH_STUDENT)
  set(oneValueArgs)
  set(multiValueArgs SOURCES LIBRARIES DEFINITIONS DEPENDS)
  cmake_parse_arguments(ARG "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  if (ARG_INTERNAL_ONLY AND NOT SCHEDLAB_BUILD_INTERNAL_TESTS)
    return()
  endif()

  if (NOT ARG_INTERNAL_ONLY AND NOT SCHEDLAB_BUILD_PUBLIC_TESTS)
    return()
  endif()

  if (ARG_WITH_STUDENT)
    schedlab_add_binary(${target_name}
      WITH_STUDENT
      SOURCES ${ARG_SOURCES}
      LIBRARIES ${ARG_LIBRARIES}
      DEFINITIONS ${ARG_DEFINITIONS}
      DEPENDS ${ARG_DEPENDS}
    )
  else()
    schedlab_add_binary(${target_name}
      SOURCES ${ARG_SOURCES}
      LIBRARIES ${ARG_LIBRARIES}
      DEFINITIONS ${ARG_DEFINITIONS}
      DEPENDS ${ARG_DEPENDS}
    )
  endif()

  add_test(NAME ${target_name} COMMAND ${target_name})
endfunction()
