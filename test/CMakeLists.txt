SET(LIBREDIS_TEST_SRCS redis_test.cpp)
ADD_EXECUTABLE(redis_test ${LIBREDIS_TEST_SRCS})
TARGET_LINK_LIBRARIES(redis_test rediscxx boost_program_options-mt boost_thread-mt boost_system-mt)
