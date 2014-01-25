/** @file
 * @brief os basic functions
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 * inner header
 */
#ifndef _LANGTAOJIN_LIBREDIS_OS_H_
#define _LANGTAOJIN_LIBREDIS_OS_H_

#include "redis_common.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

LIBREDIS_NAMESPACE_BEGIN

int get_thread_id();

std::string get_host_name();

std::string ec_2_string(int ec);

/************************************************************************/
/* socket functions: */
/* all of them will retry operations with possible EINTR */
/************************************************************************/
/**
 * return the readable bytes
 * return -1, failure, check errno
 */
int available_bytes(int fd);
/**
 * return 0, success
 * return -1, failure, check errno
 */
int set_nonblock(int fd);
/**
 * return 0, success
 * return -1, failure, check errno
 */
int set_block(int fd);
/**
 * the slow way performs a real read(peek)
 * return 1, open
 * return 0, closed
 */
int is_open_slow(int fd);
/**
 * the fast way only checks the fd
 * return 1, open
 * return 0, closed
 */
int is_open_fast(int fd);
/**
 * return 1, readable
 * return 0, timeout, check errno
 * return -1, error, check errno
 *
 * NOTICE: all 'timeout' are in milliseconds, a negative value means to wait forever
 */
int poll_read(int fd, int timeout);
/**
 * return 1, writable
 * return 0, timeout, check errno
 * return -1, error, check errno
 */
int poll_write(int fd, int timeout);
/**
 * return 0, success
 * return -1, failure, check errno
 * NOTE: the function will make fd non-blocking
 */
int timed_connect(int fd,
    const struct sockaddr * addr,
    socklen_t addrlen, int timeout);
/**
 * return the read bytes
 * return 0, 'errno==ETIMEDOUT' means timeout, others meas EOF
 * return -1, failure, check errno
 */
int timed_read(int fd, void * buf, size_t len, int flags, int timeout);
/**
 * return the read bytes
 * return 0, 'errno==ETIMEDOUT' means timeout, others meas EOF
 * return -1, failure, check errno
 */
int timed_readn(int fd, void * buf, size_t len, int flags, int timeout);
/**
 * return the written bytes
 * return 0, 'errno==ETIMEDOUT' means timeout, others meas EOF
 * return -1, failure, check errno
 */
int timed_writen(int fd, const void * buf, size_t len, int flags, int timeout);
/**
 * return 0, success
 * return -1, failure, check errno
 */
int safe_close(int fd);

struct net_endpoint
{
  int domain;
  int type;
  int protocol;
  union
  {
    struct sockaddr_in in4;
    struct sockaddr_in6 in6;
  } address;
};
/**
 * return 0, success
 * return -1, failure, check errno
 */
int resolve_host(const char * host, const char * service, struct net_endpoint * ep);

LIBREDIS_NAMESPACE_END

#endif// _LANGTAOJIN_LIBREDIS_OS_H_
