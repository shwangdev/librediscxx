/** @file
 * @brief redis protocol operation
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#include "redis_protocol.h"
#include "tcp_client.h"
#include "os.h"
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <algorithm>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/foreach.hpp>

#define CHECK_PTR_PARAM(ptr) \
  if (ptr==NULL) {error_ = "EINVAL";return false;}

//lint -e429 I am sure this is a lint bug

LIBREDIS_NAMESPACE_BEGIN

static const std::string s_redis_line_end("\r\n");

  RedisProtocol::RedisProtocol(const std::string& host, const std::string& port, int timeout)
: host_(host), port_(port), timeout_(timeout), blocking_mode_(false), transaction_mode_(false)
{
  tcp_client_ = new TcpClient;
}

RedisProtocol::~RedisProtocol()
{
  close();
  delete tcp_client_;
}

bool RedisProtocol::assure_connect(int * status)
{
  if (tcp_client_->is_open())
  {
    if (status)
      *status = 0;
    return true;
  }

  if (connect())
  {
    if (status)
      *status = 1;
    return true;
  }

  if (status)
    *status = -1;
  return false;
}

bool RedisProtocol::connect()
{
  int ec;
  tcp_client_->connect(host_, port_, timeout_, &ec);

  if (ec)
  {
    close();
    error_ = str(boost::format("connect %s:%s failed, %s")
        % host_ % port_ % ec_2_string(ec));
    return false;
  }
  return true;
}

void RedisProtocol::close()
{
  tcp_client_->close();
  blocking_mode_ = false;
  transaction_mode_ = false;
}

bool RedisProtocol::available()const
{
  return tcp_client_->available();
}

bool RedisProtocol::is_open()const
{
  return tcp_client_->is_open();
}

bool RedisProtocol::check_connect()
{
  if (!is_open())
  {
    error_ = str(boost::format("not connected to %s:%s") % host_ % port_);
    return false;
  }
  return true;
}

bool RedisProtocol::exec_command(RedisCommand * command)
{
  CHECK_PTR_PARAM(command);

  if (!write_command(command))
    return false;

  if (!read_reply(command))
    return false;
  return true;
}

bool RedisProtocol::exec_commandv(RedisCommand * command, const char * format, va_list ap)
{
  CHECK_PTR_PARAM(command);

  if (!write_commandv(command, format, ap))
    return false;

  if (!read_reply(command))
    return false;
  return true;
}

bool RedisProtocol::exec_command(RedisCommand * command, const char * format, ...)
{
  CHECK_PTR_PARAM(command);

  va_list ap;
  va_start(ap, format);
  bool ret = exec_commandv(command, format, ap);
  va_end(ap);
  return ret;
}

bool RedisProtocol::exec_pipeline(redis_command_vector_t * commands)
{
  CHECK_PTR_PARAM(commands);

  size_t size = commands->size();
  for (size_t i=0; i<size; i++)
  {
    if (!write_command((*commands)[i]))
      return false;

    // To avoid being disconnected,
    // check the status of connection between each pair of operations.
    if (!tcp_client_->is_open())
    {
      close();
      error_ = "connection has been broken during this pipeline operation";
      return false;
    }
  }

  for (size_t i=0; i<size; i++)
  {
    if (!read_reply((*commands)[i]))
      return false;
    if (!tcp_client_->is_open())
    {
      close();
      error_ = "connection has been broken during this pipeline operation";
      return false;
    }
  }

  return true;
}

bool RedisProtocol::write_command(RedisCommand * command)
{
  CHECK_PTR_PARAM(command);

  std::stringstream ss;
  int given_argc = static_cast<int>(command->in.args().size());

  if (!check_argc(command, given_argc))
    return false;

  /**
   * Requests:
   * *<number of arguments> CR LF
   * $<number of bytes of argument 1> CR LF
   * <argument data> CR LF
   * ...
   * $<number of bytes of argument N> CR LF
   * <argument data> CR LF
   */
  ss << "*" << (given_argc+1) << s_redis_line_end;

  // write command
  ss << "$" << command->in.command_info().command_str.size() << s_redis_line_end;
  ss << command->in.command_info().command_str << s_redis_line_end;

  // write args
  BOOST_FOREACH(const std::string& arg, command->in.args())
  {
    ss << "$" << arg.size() << s_redis_line_end << arg << s_redis_line_end;
  }

  int ec;
  tcp_client_->write(ss.str(), timeout_, &ec);
  if (ec)
  {
    close();
    error_ = str(boost::format("write %s:%s failed, %s")
        % host_ % port_ % ec_2_string(ec));
    command->out.set_error(error_);
    return false;
  }
  return true;
}

struct is_empty_string
{
  bool operator()(const std::string& str)const
  {
    return str.empty();
  }
};

bool RedisProtocol::write_commandv(RedisCommand * command, const char * format, va_list ap)
{
  CHECK_PTR_PARAM(command);

  std::string buf;
  size_t buf_len = 64;
  int bytes;
  for (;;)
  {
    buf.resize(buf_len, '\0');
    bytes = vsnprintf(&buf[0], buf_len, format, ap);
    if (static_cast<size_t>(bytes)<buf_len)
    {
      buf.resize(static_cast<size_t>(bytes));
      break;
    }

    buf_len = buf_len << 1;
  }

  boost::trim(buf);
  (void)boost::split(command->in.args(), buf, boost::is_any_of(" "));
  (void)command->in.args().erase(std::remove_if(command->in.args().begin(),
        command->in.args().end(), is_empty_string()), command->in.args().end());

  std::stringstream ss;
  int given_argc = static_cast<int>(command->in.args().size());
  if (given_argc==0)
  {
    // no need to disconnect for soft error
    error_ = "given argc is zero";
    command->out.set_error(error_);
    return false;
  }

  command->in.set_command(command->in.args()[0]);
  if (command->in.command()==NOOP)
  {
    // no need to disconnect for client error
    error_ = str(boost::format("invalid command: %s") % command->in.args()[0]);
    command->out.set_error(error_);
    return false;
  }

  // given_argc excludes command string
  if (!check_argc(command, given_argc - 1))
    return false;

  /**
   * Requests:

   * *<number of arguments> CR LF
   * $<number of bytes of argument 1> CR LF
   * <argument data> CR LF
   * ...
   * $<number of bytes of argument N> CR LF
   * <argument data> CR LF
   */
  ss << "*" << given_argc << s_redis_line_end;

  // write command and args
  BOOST_FOREACH(const std::string& arg, command->in.args())
  {
    ss << "$" << arg.size() << s_redis_line_end << arg << s_redis_line_end;
  }

  int ec;
  tcp_client_->write(ss.str(), timeout_, &ec);
  if (ec)
  {
    close();
    error_ = str(boost::format("write %s:%s failed, %s")
        % host_ % port_ % ec_2_string(ec));
    command->out.set_error(error_);
    return false;
  }
  return true;
}

bool RedisProtocol::write_command(RedisCommand * command, const char * format, ...)
{
  va_list ap;
  va_start(ap, format);
  bool ret = write_commandv(command, format, ap);
  va_end(ap);
  return ret;
}

bool RedisProtocol::read_reply(RedisCommand * command)
{
  CHECK_PTR_PARAM(command);
  return __read_reply(command, &command->out, true);
}

bool RedisProtocol::__read_reply(RedisCommand * command, RedisOutput * output,
    bool check_reply_type)
{
  /**
   * Replies:

   * With a single line reply the first byte of the reply will be "+"
   * With an error message the first byte of the reply will be "-"
   * With an integer number the first byte of the reply will be ":"
   * With bulk reply the first byte of the reply will be "$"
   * With multi-bulk reply the first byte of the reply will be "*"
   */
  std::string buf;
  kCommand cmd = command->in.command();
  kReplyType exp_reply_type = command->in.command_info().reply_type;
  if (check_reply_type && exp_reply_type==kDepends)
    check_reply_type = false;

  if (check_reply_type && transaction_mode_)
  {
    switch (cmd)
    {
      case MULTI:
      case DISCARD:
      case EXEC:
        break;
      default:
        // in transaction mode, these common commands always reply kStatus
        exp_reply_type = kStatus;
        break;
    }
  }

  if (!read_line(&buf))
  {
    output->set_error(error_);
    return false;
  }
  assert(!buf.empty());

  switch (buf[0])
  {
    case '-':
      // no need to disconnect
      // store the redis error string
      error_ = buf.substr(1);
      output->set_error(error_);
      return false;

    case '+':
      if (check_reply_type && exp_reply_type!=kStatus)
      {
        close();
        error_ = str(boost::format("read %s:%s failed, reply type error +")
            % host_ % port_);
        output->set_error(error_);
        return false;
      }
      else
      {
        output->set_status(buf.substr(1));

        if (!transaction_mode_ && cmd==MULTI)
          transaction_mode_ = true;
        else if (transaction_mode_ && cmd==DISCARD)
          transaction_mode_ = false;

        return true;
      }

    case ':':
      if (check_reply_type && exp_reply_type!=kInteger)
      {
        close();
        error_ = str(boost::format("read %s:%s failed, reply type error :")
            % host_ % port_);
        output->set_error(error_);
        return false;
      }
      else
      {
        int64_t i;
        if (parse_integer(buf, &i))
        {
          output->set_i(i);
        }
        else
        {
          close();
          error_ = str(boost::format("read %s:%s failed, integer error : %s")
              % host_ % port_ % buf);
          output->set_error(error_);
          return false;
        }
        return true;
      }

    case '$':
      if (check_reply_type && exp_reply_type!=kBulk)
      {
        close();
        error_ = str(boost::format("read %s:%s failed, reply type error $")
            % host_ % port_);
        output->set_error(error_);
        return false;
      }
      else
      {
        std::string bulk;
        std::string * out_bulk;
        if (read_bulk_2(buf, &bulk, &out_bulk))
        {
          if (out_bulk)
            output->set_bulk(*out_bulk);
          else// nil
            output->set_nil_bulk();
          return true;
        }
        else
        {
          output->set_error(error_);
          return false;
        }
      }

    case '*':
      if (check_reply_type && exp_reply_type!=kMultiBulk
          && exp_reply_type!=kSpecialMultiBulk)
      {
        close();
        error_ = str(boost::format("read %s:%s failed, reply type error *")
            % host_ % port_);
        output->set_error(error_);
        return false;
      }
      else if (exp_reply_type==kMultiBulk)
      {
        mbulk_t mbulks;
        mbulk_t * out_mbulks;
        if (read_multi_bulk_2(buf, &mbulks, &out_mbulks))
        {
          if (out_mbulks)
            output->set_mbulks(out_mbulks);
          else// nil
            output->set_nil_mbulks();
          return true;
        }
        else
        {
          output->set_error(error_);
          return false;
        }
      }
      else
      {
        // treat as kSpecialMultiBulk
        int64_t bulk_size;
        if (!parse_integer(buf, &bulk_size))
        {
          close();
          error_ = str(boost::format("read %s:%s failed, integer error : %s")
              % host_ % port_ % buf);
          output->set_error(error_);
          return false;
        }

        if (bulk_size==-1)
        {
          output->set_nil_smbulks();
        }
        else
        {
          smbulk_t smbulks;

          for (int64_t i=0; i<bulk_size; i++)
          {
            RedisOutput * output_child = new RedisOutput;
            if (!__read_reply(command, output_child, false))
            {
              delete output_child;
              clear_smbulks(&smbulks);
              return false;
            }
            smbulks.push_back(output_child);
          }

          output->set_smbulks(&smbulks);
          clear_smbulks(&smbulks);
        }

        if (transaction_mode_ && cmd==EXEC && &command->out==output)
          transaction_mode_ = false;

        return true;
      }
    default:
      break;
  }

  close();
  error_ = str(boost::format("read %s:%s failed, unexpected reply %s")
      % host_ % port_ % buf);
  output->set_error(error_);
  return false;
}

bool RedisProtocol::read_line(std::string * line)
{
  int ec;
  *line = tcp_client_->read_line(s_redis_line_end,
      blocking_mode_?(-1):timeout_, &ec);

  if (ec)
  {
    close();
    error_ = str(boost::format("read %s:%s failed, %s")
        % host_ % port_ % ec_2_string(ec));
    return false;
  }

  if (line->empty())
  {
    close();
    error_ = str(boost::format("read %s:%s failed, empty line") % host_ % port_);
    return false;
  }

  return true;
}

bool RedisProtocol::read(size_t count, std::string * line)
{
  int ec;
  *line = tcp_client_->read(count, s_redis_line_end,
      blocking_mode_?(-1):timeout_, &ec);

  if (ec)
  {
    close();
    error_ = str(boost::format("read %s:%s failed, %s")
        % host_ % port_ % ec_2_string(ec));
    return false;
  }

  if (line->empty())
  {
    close();
    error_ = str(boost::format("read %s:%s failed, empty line") % host_ % port_);
    return false;
  }

  return true;
}

bool RedisProtocol::read_bulk_2(const std::string& header,
    std::string * bulk, std::string ** out_bulk)
{
  int64_t i;
  if (!parse_integer(header, &i))
  {
    close();
    error_ = str(boost::format("read %s:%s failed, integer error : %s")
        % host_ % port_ % header);
    return false;
  }

  if (i<-1 || i==0)
  {
    close();
    error_ = str(boost::format("read %s:%s failed, unexpected integer : %d")
        % host_ % port_ % i);
    return false;
  }
  else if (i==-1)
  {
    // nil
    *out_bulk = NULL;
    return true;
  }
  else
  {
    // read bulk
    if (!read(static_cast<size_t>(i), bulk))
    {
      return false;
    }

    if (static_cast<size_t>(i)!=bulk->size())
    {
      close();
      error_ = str(boost::format("read %s:%s failed, unexpected reply %s")
          % host_ % port_ % (*bulk));
      return false;
    }

    *out_bulk = bulk;
    return true;
  }
}

bool RedisProtocol::read_multi_bulk_2(const std::string& header,
    mbulk_t * mbulks, mbulk_t ** out_mbulks)
{
  int64_t i;
  if (!parse_integer(header, &i))
  {
    close();
    error_ = str(boost::format("read %s:%s failed, integer error : %s")
        % host_ % port_ % header);
    return false;
  }

  if (i<-1)
  {
    close();
    error_ = str(boost::format("read %s:%s failed, unexpected integer : %d")
        % host_ % port_ % i);
    return false;
  }
  else if (i==-1)
  {
    // nil
    *out_mbulks = NULL;
    return true;
  }
  else
  {
    mbulks->reserve(static_cast<size_t>(i));

    for (int64_t j=0; j<i; j++)
    {
      std::string * bulk;
      if (!read_bulk(&bulk))
      {
        clear_mbulks(mbulks);
        return false;
      }
      mbulks->push_back(bulk);
    }

    *out_mbulks = mbulks;
    return true;
  }
}

bool RedisProtocol::read_bulk(std::string ** bulk)
{
  std::string header;
  if (!read_line(&header))
    return false;

  if (header[0]=='$')
  {
    std::string b;
    std::string * ob;

    if (!read_bulk_2(header, &b, &ob))
      return false;

    if (ob)
    {
      *bulk = new std::string;
      (*bulk)->swap(b);
    }
    else
    {
      // nil
      *bulk = NULL;
    }
    return true;
  }
  else
  {
    close();
    error_ = str(boost::format("read %s:%s failed, reply type error $")
        % host_ % port_);
    return false;
  }
}

bool RedisProtocol::parse_integer(const std::string& line, int64_t * i)
{
  assert(!line.empty());

  *i = strtol(line.c_str()+1, NULL, 10);
  if (errno==EINVAL || errno==ERANGE)
    return false;
  return true;
}

bool RedisProtocol::check_argc(RedisCommand * command, int given_argc)
{
  bool err = false;
  int argc = command->in.command_info().argc;

  switch (argc)
  {
    case ARGC_NO_CHECKING:
      break;
    default:
      if (argc>=0)
      {
        if (argc!=given_argc)
          err = true;
      }
      else
      {
        if (given_argc<-argc)
          err = true;
      }
      break;
  }

  if (err)
  {
    error_ = str(boost::format("argc not matched, expect %d, given %d")
        % argc % given_argc);
    command->out.set_error(error_);
  }
  return !err;
}

LIBREDIS_NAMESPACE_END
