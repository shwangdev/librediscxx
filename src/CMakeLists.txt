SET(LIBREDISCXX_SRCS os.cpp redis_cmd.cpp redis_tss.cpp redis.cpp redis_partition.cpp tcp_client.cpp redis_base.cpp redis_protocol.cpp)

ADD_LIBRARY(rediscxx STATIC ${LIBREDISCXX_SRCS})
