add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)

if(PG_REGRESS)

add_test(
  NAME regress
  COMMAND ${PG_REGRESS}
    --temp-config=${PROJECT_SOURCE_DIR}/regress/regress.conf
    --temp-instance=${CMAKE_BINARY_DIR}/tmp
    --inputdir=${PROJECT_SOURCE_DIR}/regress
    --schedule=${PROJECT_SOURCE_DIR}/regress/schedule.conf
    --load-extension=quack
)

endif()