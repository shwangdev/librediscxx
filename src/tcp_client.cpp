/** @file
 * @brief a tcp client with timeout
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#include "tcp_client.h"
#include "os.h"
#include <assert.h>
#include <time.h>
#include <string.h>
#include <boost/thread/shared_mutex.hpp>

LIBREDIS_NAMESPACE_BEGIN

namespace
{
  enum
  {
    kDefaultBufferSize = 512,
    kMaxBufferSize = 65536,

    kConnectTimeoutProportion = 5,
    kMinConnectTimeout = 10,

    kCheckOpenInterval = 180
  };

  /************************************************************************/
  /* HostResolver(multi thread safe) */
  /************************************************************************/
  class HostResolver
  {
    private:
      typedef std::pair<std::string, std::string> host_service_t;
      struct HostEntry
      {
        net_endpoint endpoint;
        int ec;
      };
      typedef std::map<host_service_t, HostEntry> dns_cache_t;

      dns_cache_t dns_cache_;
      mutable boost::shared_mutex dns_cache_mutex_;
      const bool enable_cache_;

    private:
      static void __resolve(
          const std::string& host,
          const std::string& service,
          net_endpoint * endpoint,
          int * ec)
      {
        int ret = resolve_host(host.c_str(), service.c_str(), endpoint);
        if (ret!=0)
          *ec = errno;
        else
          *ec = 0;
      }

      // return true, found in cache
      // return false, not found in cache
      bool __lookup_cache(
          const std::string& host,
          const std::string& service,
          net_endpoint * endpoint,
          int * ec)const
      {
        host_service_t key = std::make_pair(host, service);
        dns_cache_t::const_iterator iter;

        boost::shared_lock<boost::shared_mutex> guard(dns_cache_mutex_);

        iter = dns_cache_.find(key);
        if (iter==dns_cache_.end())
          return false;

        *endpoint = (*iter).second.endpoint;
        *ec = (*iter).second.ec;
        return true;
      }

      void __update_cache(
          const std::string& host,
          const std::string& service,
          const net_endpoint& endpoint,
          int ec)
      {
        host_service_t key = std::make_pair(host, service);
        HostEntry host_entry;
        host_entry.endpoint = endpoint;
        host_entry.ec = ec;

        boost::unique_lock<boost::shared_mutex> guard(dns_cache_mutex_);
        dns_cache_[key] = host_entry;
      }

    public:
      explicit HostResolver(bool enable_cache = true)
        : enable_cache_(enable_cache) {}

      ~HostResolver() {}

      void resolve(
          const std::string& host,
          const std::string& service,
          net_endpoint * endpoint,
          int * ec)
      {
        if (enable_cache_)
        {
          if (__lookup_cache(host, service, endpoint, ec))
            return;

          __resolve(host, service, endpoint, ec);
          __update_cache(host, service, *endpoint, *ec);
        }
        else
        {
          __resolve(host, service, endpoint, ec);
        }
      }

      void clear_cache()
      {
        boost::unique_lock<boost::shared_mutex> guard(dns_cache_mutex_);
        dns_cache_.clear();
      }
  } s_host_resolver;

  /************************************************************************/
  /* TcpClientBuffer */
  /************************************************************************/
  class TcpClientBuffer
  {
    private:
      std::vector<char> buffer_;
      char * read_;// ptr to first byte already read
      char * to_read_;// ptr to first byte to be read next time
      // the following inequality is always true:
      // buffer_.begin()<=read<=to_read<=buffer_.end()

      inline char * begin()
      {
        return &buffer_[0];
      }

      inline char * end()
      {
        return &buffer_[0] + buffer_.size();
      }

      inline const char * begin()const
      {
        return &buffer_[0];
      }

      inline const char * end()const
      {
        return &buffer_[0] + buffer_.size();
      }

    public:
      explicit TcpClientBuffer(size_t buf_size = kDefaultBufferSize)
        :buffer_(buf_size), read_(begin()), to_read_(begin())
      {
        assert(buf_size);
      }

      inline std::pair<const char *, size_t> get_read_buffer()const
      {
        return std::make_pair(read_, read_size());
      }

      inline std::pair<char *, size_t> get_to_read_buffer()const
      {
        return std::make_pair(to_read_, free_size());
      }

      inline size_t total_size()const
      {
        return buffer_.size();
      }

      inline size_t consumed_size()const
      {
        return static_cast<size_t>(read_ - begin());
      }

      inline size_t read_size()const
      {
        return static_cast<size_t>(to_read_ - read_);
      }

      inline size_t free_size()const
      {
        return static_cast<size_t>(end() - to_read_);
      }

      inline void clear()
      {
        buffer_.resize(kDefaultBufferSize);
        read_ = to_read_ = begin();
      }

      void prepare(size_t size = kDefaultBufferSize, bool drain = false)
      {
        if (size==0)
          return;

        if (!drain && free_size()>=size)
          return;

        size_t rsize = read_size();
        size_t min_new_size = rsize + size;
        size_t new_size = buffer_.size();

        for (; new_size<=min_new_size; new_size = new_size << 1)
        {
        }

        std::vector<char> buffer(new_size);
        ::memcpy(&buffer[0], read_, rsize);

        buffer.swap(buffer_);
        read_ = begin();
        to_read_ = read_ + rsize;
      }

      void produce(size_t size)
      {
        if (size==0)
          return;

        assert(free_size()>=size);

        to_read_ += size;
      }

      std::string consume(size_t size)
      {
        assert(size);
        assert(read_size()>=size);

        char * read = read_;
        read_ += size;
        std::string ret(read, size);

        if (read_size()==0 && total_size()>=kMaxBufferSize)
          drain();
        else if (read_size()==0)
          read_ = to_read_ = begin();

        return ret;
      }

      inline void drain()
      {
        prepare(kDefaultBufferSize, true);
      }
  };
}

/************************************************************************/
/*TcpClient::Impl*/
/************************************************************************/
class TcpClient::Impl
{
  private:
    int fd_;
    TcpClientBuffer buffer_;
    mutable time_t last_check_open_time_;

  public:
    Impl()
      : fd_(-1), last_check_open_time_(0) {}

    ~Impl()
    {
      close();
    }

    inline void connect(
        const std::string& ip_or_host,
        const std::string& port_or_service,
        int timeout,
        int * ec)
    {
      close();

      // resolve host
      net_endpoint endpoint;
      // It is only IPv4, because redis-server binds a v4 address.
      endpoint.domain = AF_INET;
      endpoint.type = SOCK_STREAM;
      endpoint.protocol = IPPROTO_TCP;
      s_host_resolver.resolve(ip_or_host, port_or_service, &endpoint, ec);
      if (*ec!=0)
        return;

      // create socket
      int fd;
      if ((fd = socket(endpoint.domain, endpoint.type, endpoint.protocol))==-1)
      {
        *ec = errno;
        return;
      }

      // It is only IPv4, because redis-server binds a v4 address.
      struct sockaddr * addr = /*lint -e(740) */(struct sockaddr * )&endpoint.address.in4;
      socklen_t addrlen = sizeof(endpoint.address.in4);

      // adjust timeout
      timeout /= kConnectTimeoutProportion;
      if (timeout<kMinConnectTimeout)
        timeout = kMinConnectTimeout;

      // connect
      if (timed_connect(fd, addr, addrlen, timeout)==-1)
      {
        safe_close(fd);
        *ec = errno;
        return;
      }

      fd_ = fd;
      ::time(&last_check_open_time_);
      *ec = 0;
    }

    inline void write(
        const std::string& line,
        int timeout,
        int * ec)
    {
      if (timed_writen(fd_, &line[0], line.size(), 0, timeout)
          !=static_cast<int>(line.size()))
      {
        close();
        *ec = errno;
      }
      else
      {
        ::time(&last_check_open_time_);
        *ec = 0;
      }
    }

    inline std::string read(
        size_t size,
        const std::string& delim,
        int timeout,
        int * ec)
    {
      std::string line;
      const size_t delim_size = delim.size();
      size_t expect = size + delim_size;
      size_t to_read = expect - buffer_.read_size();
      int count;

      for (;;)
      {
        // try to consume previous buffer
        if (buffer_.read_size()>=expect)
        {
          std::pair<const char *, size_t> read_buf = buffer_.get_read_buffer();
          (void)line.assign(read_buf.first, read_buf.first + expect - delim_size);
          (void)buffer_.consume(expect);
          *ec = 0;
          return line;
        }

        // try to read
        buffer_.prepare(to_read);
        std::pair<char *, size_t> to_read_buf = buffer_.get_to_read_buffer();
        count = timed_read(fd_, to_read_buf.first, to_read_buf.second, 0, timeout);
        if (count<=0)
        {
          close();
          *ec = errno;
          return std::string();
        }

        // push to buffer_
        buffer_.produce(static_cast<size_t>(count));
        to_read -= static_cast<size_t>(count);
      }
    }

    inline std::string read_line(
        const std::string& delim,
        int timeout,
        int * ec)
    {
      std::string line;
      const size_t delim_size = delim.size();
      const char * search_begin;
      const char * search_end;
      const char * search_curr;
      size_t search_offset = 0;
      size_t to_consume;
      int count;

      for (;;)
      {
        // try to consume previous buffer
        if (buffer_.read_size()>=delim_size)
        {
          std::pair<const char *, size_t> read_buf = buffer_.get_read_buffer();
          // search for delim
          search_begin = read_buf.first;
          search_end = search_begin + read_buf.second - delim_size;
          search_curr = search_begin + search_offset;

          for (; search_curr<=search_end; search_curr++, search_offset++)
          {
            if (::memcmp(delim.c_str(), search_curr, delim_size)==0)
            {
              // find delim in buffer_, grep it and return
              to_consume = static_cast<size_t>(search_curr - search_begin) + delim_size;
              (void)line.assign(read_buf.first, read_buf.first + to_consume - delim_size);
              (void)buffer_.consume(to_consume);
              *ec = 0;
              return line;
            }
          }
        }

        // try read
        buffer_.prepare();
        std::pair<char *, size_t> to_read_buf = buffer_.get_to_read_buffer();
        count = timed_read(fd_, to_read_buf.first, to_read_buf.second, 0, timeout);
        if (count<=0)
        {
          close();
          *ec = errno;
          return std::string();
        }

        // push to buffer_
        buffer_.produce(static_cast<size_t>(count));
      }
    }

    inline void close()
    {
      if (fd_!=-1)
      {
        safe_close(fd_);
        fd_ = -1;
      }
      buffer_.clear();
    }

    inline bool is_open()const
    {
      if (is_open_fast(fd_)==0)
        return false;

      // to detect the close of redis server
      time_t now;
      ::time(&now);
      if (now - last_check_open_time_>kCheckOpenInterval)
      {
        last_check_open_time_ = now;

        if (is_open_slow(fd_)==0)
          return false;
      }

      return true;
    }

    inline bool available()const
    {
      if (buffer_.read_size()!=0)
        return true;

      int bytes;
      if ((bytes = available_bytes(fd_))==-1)
        return false;

      return bytes!=0;
    }
};

/************************************************************************/
/*TcpClient*/
/************************************************************************/
TcpClient::TcpClient()
{
  impl_ = new Impl;
}

TcpClient::~TcpClient()
{
  delete impl_;
}

void TcpClient::connect(const std::string& ip_or_host,
    const std::string& port_or_service,
    int timeout,
    int * ec)
{
  impl_->connect(ip_or_host, port_or_service, timeout, ec);
}

void TcpClient::write(const std::string& line,
    int timeout,
    int * ec)
{
  impl_->write(line, timeout, ec);
}

std::string TcpClient::read(size_t size,
    const std::string& delim,
    int timeout,
    int * ec)
{
  return impl_->read(size, delim, timeout, ec);
}

std::string TcpClient::read_line(const std::string& delim,
    int timeout,
    int * ec)
{
  return impl_->read_line(delim, timeout, ec);
}

void TcpClient::close()
{
  impl_->close();
}

bool TcpClient::is_open()const
{
  return impl_->is_open();
}

bool TcpClient::available()const
{
  return impl_->available();
}

LIBREDIS_NAMESPACE_END
