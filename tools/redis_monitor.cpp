/** @file
 * @brief redis monitor program
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#include <redis_protocol.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <map>
#include <vector>
#include <boost/program_options.hpp>
#include <boost/asio.hpp>
#include <boost/date_time.hpp>
#include <boost/date_time/c_local_time_adjustor.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>

bool redis_monitor;
std::string redis_host, redis_port;
int redis_timeout, update_cycle;

typedef boost::asio::ip::tcp::socket tcp_socket;
typedef boost::asio::deadline_timer deadline_timer;
typedef boost::asio::io_service io_service;

USING_LIBREDIS_NAMESPACE

namespace
{
  typedef std::pair<std::string, int> cmd_db_t;
  typedef std::map<cmd_db_t, int> cmd_db_map_t;
  typedef std::map<std::string, int> cmd_map_t;

  cmd_db_map_t s_cmd_db_map;
  cmd_map_t s_cmd_map;

  bool s_stop_flag = false;
  io_service s_ios;
  boost::scoped_ptr<RedisProtocol> s_monitor;
  boost::scoped_ptr<RedisProtocol> s_info;
  boost::scoped_ptr<deadline_timer> s_timer;

  bool init_monitor()
  {
    s_monitor.reset(new RedisProtocol(redis_host, redis_port, redis_timeout));
    if (!s_monitor->assure_connect(NULL))
    {
      std::cerr << "connection failed" << std::endl;
      s_monitor.reset();
      return false;
    }

    RedisCommand command;
    va_list ap;
    bool ret;
    memset(&ap, 0, sizeof(ap));
    ret = s_monitor->exec_command(&command, "MONITOR", ap);

    if (!ret)
    {
      s_monitor.reset();
      std::cerr << "MONITOR failed" << std::endl;
      return false;
    }

    if (!command.out.is_status_ok())
    {
      s_monitor.reset();
      std::cerr << "MONITOR response is not OK" << std::endl;
      return false;
    }

    std::cerr << command.out.ptr.status->c_str() << std::endl;
    return true;
  }

  void monitor()
  {
    if (!s_monitor)
      init_monitor();
    if (!s_monitor)
      return;

    RedisProtocol * pro = s_monitor.get();
    bool ret = true;
    std::string line;
    char * enddb;
    char * endcmd;
    int db;
    std::string cmd;

    while (pro->available() && !s_stop_flag)
    {
      ret = pro->read_line(&line);
      if (ret)
      {
        // example:
        // +1317279070.687700 (db 1) "EXPIRE" "tag_pv2_de8c68" "3600"
        // +1317280373.276440 "MONITOR"
        if (line.size() < 24 || line[20]!='d')
          continue;

        db = static_cast<int>(strtoul(&line[23], &enddb, 10));
        if (errno == ERANGE)
          continue;

        enddb += 3;
        endcmd = strchr(enddb, '"');
        if (endcmd == NULL)
          continue;

        cmd.assign(enddb, endcmd);

        s_cmd_db_map[std::make_pair(cmd, db)]++;
        s_cmd_map[cmd]++;
      }
      else
      {
        s_monitor.reset();
        break;
      }
    }
  }

  void uninit_monitor()
  {
    s_monitor.reset();
  }

  void print_monitor()
  {
    {
      cmd_db_map_t::const_iterator first = s_cmd_db_map.begin();
      cmd_db_map_t::const_iterator last = s_cmd_db_map.end();
      for (; first!=last; ++first)
      {
        std::cout << "[" << (*first).first.first << " " << (*first).first.second << "]:"
          << (*first).second << std::endl;
      }
      s_cmd_db_map.clear();
    }

    {
      cmd_map_t::const_iterator first = s_cmd_map.begin();
      cmd_map_t::const_iterator last = s_cmd_map.end();
      for (; first!=last; ++first)
      {
        std::cout << "[" << (*first).first << "]:" << (*first).second << std::endl;
      }
      s_cmd_map.clear();
    }
  }

  void init_info()
  {
    s_info.reset(new RedisProtocol(redis_host, redis_port, redis_timeout));
    if (!s_info->assure_connect(NULL))
    {
      std::cerr << "connection failed" << std::endl;
      s_info.reset();
      return;
    }
  }

  void info()
  {
    if (!s_info)
      init_info();
    if (!s_info)
      return;

    RedisCommand command;
    va_list ap;
    bool ret;
    memset(&ap, 0, sizeof(ap));
    ret = s_info->exec_command(&command, "INFO", ap);

    if (!ret)
    {
      s_info.reset();
      std::cerr << "INFO failed" << std::endl;
      return;
    }

    if (!command.out.is_bulk())
    {
      s_info.reset();
      std::cerr << "INFO response is not OK" << std::endl;
      return;
    }

    std::cout <<  command.out.ptr.bulk->c_str() << std::endl;
  }

  void ping()
  {
    if (!s_info)
      init_info();
    if (!s_info)
      return;

    boost::posix_time::ptime begin, end;
    RedisCommand command;
    va_list ap;
    bool ret;
    int iteration = 3;

    memset(&ap, 0, sizeof(ap));
    begin = boost::posix_time::microsec_clock::local_time();

    for (int i=0; i<iteration; i++)
    {
      ret = s_info->exec_command(&command, "PING", ap);
      if (!ret)
      {
        s_info.reset();
        std::cerr << "PING failed" << std::endl;
        return;
      }

      if (!command.out.is_status_pong())
      {
        s_info.reset();
        std::cerr << "PING response is not PONG" << std::endl;
        return;
      }
    }

    end = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration td = end - begin;
    int ms = static_cast<int>(td.total_milliseconds())/iteration;
    std::cout << "[PING]:" << ms << std::endl;
  }

  void uninit_info()
  {
    s_info.reset();
  }

  void update(const boost::system::error_code& ec)
  {
    if (ec)
      return;

    std::cout << "{begin}" << std::endl;
    std::cout << time(NULL) << std::endl;
    std::cout << redis_host << ":" << redis_port << std::endl;
    if (redis_monitor)
    {
      monitor();
      print_monitor();
    }
    info();
    ping();
    std::cout << "{end}" << std::endl;

    boost::posix_time::ptime next = s_timer->expires_at();
    boost::posix_time::seconds cycle = boost::posix_time::seconds(
        update_cycle);
    while (next <= boost::asio::deadline_timer::traits_type::now())
      next = next + cycle;

    s_timer->expires_at(next);
    s_timer->async_wait(update);
  }

  void signal_handler(int signo)
  {
    s_stop_flag = true;
    s_ios.stop();
  }
}

int main(int argc, char ** argv)
{
  try
  {
    namespace po = boost::program_options;
    po::options_description desc("Options");
    desc.add_options()
      ("help", "produce help message")
      ("redis_monitor,m", po::value<bool>()->default_value(false), "use command MONITOR")
      ("redis_host,h", po::value<std::string>()->default_value("localhost"), "redis host")
      ("redis_port,p", po::value<std::string>()->default_value("6379"), "redis port")
      ("redis_timeout,t", po::value<int>()->default_value(1000), "redis timeout in ms")
      ("update_cycle,u", po::value<int>()->default_value(5), "update cycle in seconds");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help"))
    {
      std::cout << desc << std::endl;
      return 0;
    }

    redis_monitor = vm["redis_monitor"].as<bool>();
    redis_host = vm["redis_host"].as<std::string>();
    redis_port = vm["redis_port"].as<std::string>();
    redis_timeout = vm["redis_timeout"].as<int>();
    update_cycle = vm["update_cycle"].as<int>();
  }
  catch (std::exception& e)
  {
    std::cout << "caught: " << e.what() << std::endl;
    return 1;
  }

  signal(SIGINT, signal_handler);

  s_timer.reset(new deadline_timer(s_ios));
  if (redis_monitor)
    init_monitor();
  init_info();

  std::cout << "starting..." << std::endl;

  time_t now = time(NULL);
  now = now - (now % update_cycle) + update_cycle;
  boost::posix_time::ptime next = boost::posix_time::from_time_t(now);
  boost::posix_time::seconds cycle = boost::posix_time::seconds(update_cycle);
  while (next <= boost::asio::deadline_timer::traits_type::now())
    next = next + cycle;

  s_timer->expires_at(next);
  s_timer->async_wait(update);

  s_ios.reset();
  s_ios.run();

  std::cout << "exiting..." << std::endl;

  uninit_info();
  if (redis_monitor)
    uninit_monitor();
  s_timer.reset();

  return 0;
}
