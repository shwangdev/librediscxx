/** @file
 * @brief a tcp client with timeout
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 * inner header
 */
#ifndef _LANGTAOJIN_LIBREDIS_TCP_CLIENT_H_
#define _LANGTAOJIN_LIBREDIS_TCP_CLIENT_H_

#include "redis_common.h"

LIBREDIS_NAMESPACE_BEGIN

// single thread safety
class TcpClient
{
  private:
    class Impl;
    Impl * impl_;

  public:
    TcpClient();
    ~TcpClient();

    // all 'timeout' are in milliseconds
    // all 'ec' are got from errno
    void connect(
        const std::string& ip_or_host,
        const std::string& port_or_service,
        int timeout,
        int * ec);

    void write(
        const std::string& line,
        int timeout,
        int * ec);

    // if 'timeout' is negative, block to read
    std::string read(
        size_t size,
        const std::string& delim,
        int timeout,
        int * ec);

    // if 'timeout' is negative, block to read until 'delim'
    std::string read_line(
        const std::string& delim,
        int timeout,
        int * ec);

    void close();

    bool is_open()const;

    bool available()const;
};

LIBREDIS_NAMESPACE_END

#endif// _LANGTAOJIN_LIBREDIS_TCP_CLIENT_H_
