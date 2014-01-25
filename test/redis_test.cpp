/** @file
 * @brief unit test
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#include <redis_common.h>
#include <os.h>
#include <redis_protocol.h>
#include <redis_cmd.h>
#include <redis_base.h>
#include <redis.h>
#include <redis_partition.h>
#include <redis_tss.h>
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/assign/std/vector.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/microsec_time_clock.hpp>
#include <boost/thread.hpp>

USING_LIBREDIS_NAMESPACE
using namespace std;
using namespace boost::assign;

namespace
{
  // command line options
  std::string host, port, host_list, port_list;
  int db_index, timeout;

  // statistics
  size_t s_total = 0;
  size_t s_failed = 0;

#define VERIFY(exp) \
  do{ \
    bool x = (bool)(exp); \
    s_total++; \
    if (!x) \
    { \
      s_failed++; \
      cout << __FILE__ << "(" << __LINE__ << "):\"" << #exp << "\" fails" << endl; \
      cout << __FUNCTION__ << " failed" << endl; \
      return 1; \
    } \
  }while(0)

#define VERIFY_MSG(exp, client) \
  do{ \
    bool x = (bool)(exp); \
    s_total++; \
    if (!x) \
    { \
      s_failed++; \
      cout << __FILE__ << "(" << __LINE__ << "):\"" << #exp << "\" fails:" \
      << (client).last_c_error() << endl; \
      cout << __FUNCTION__ << " failed" << endl; \
      return 1; \
    } \
  }while(0)

#define DUMP_TEST_RESULT() \
  do \
  { \
    cout << "*************************" << endl; \
    cout << "Total Checking Points: " << s_total << endl; \
    cout << "Failure Checking Points: " << s_failed << endl; \
    if (s_failed) cout << "Some Failures may be due to the version of redis server." << endl; \
    cout << "*************************" << endl; \
  }while (0)

  std::string s_redis_version;

  void get_redis_version()
  {
    Redis2 r(host, port, db_index, timeout);
    std::string str;

    if (!r.info(&str))
      return;

    const char * begin, * end;

    begin = strstr(str.c_str(), "redis_version:");
    if (begin == NULL)
      return;

    begin += sizeof("redis_version:") - 1;

    end = strchr(begin, '\n');
    if (end == NULL)
      return;

    s_redis_version.assign(begin, end);
    cout << "redis version: " << s_redis_version << endl;
  }

  int os_test()
  {
    cout << "os_test..." << endl;
    cout << "thread id: " << get_thread_id() << endl;
    cout << "host name: " << get_host_name() << endl;
    cout << "os_test ok" << endl;
    return 0;
  }

  int protocol_test()
  {
    cout << "protocol_test..." << endl;

    std::string bulk;
    RedisCommand command;
    RedisProtocol rp(host, port, timeout);

    // object
    VERIFY_MSG(rp.assure_connect(NULL), rp);
    VERIFY_MSG(rp.exec_command(&command, "set string nothing"), rp);
    VERIFY_MSG(rp.exec_command(&command, "object refcount string"), rp);
    VERIFY_MSG(rp.exec_command(&command, "object encoding string"), rp);
    VERIFY_MSG(rp.exec_command(&command, "object idletime string"), rp);


    // pub/sub
    VERIFY_MSG(rp.assure_connect(NULL), rp);
    VERIFY_MSG(rp.exec_command(&command, "SUBSCRIBE %s", "a"), rp);
    VERIFY_MSG(rp.assure_connect(NULL), rp);
    VERIFY_MSG(rp.exec_command(&command, "UNSUBSCRIBE %s", "a"), rp);
    VERIFY_MSG(rp.assure_connect(NULL), rp);
    VERIFY_MSG(rp.exec_command(&command, "PSUBSCRIBE %s", "a"), rp);
    VERIFY_MSG(rp.assure_connect(NULL), rp);
    VERIFY_MSG(rp.exec_command(&command, "PUNSUBSCRIBE %s", "a"), rp);

    //// will block
    // VERIFY_MSG(rp.assure_connect(NULL), rp);
    // rp.set_blocking_mode(true);
    // VERIFY_MSG(rp.exec_command(&command, "UNSUBSCRIBE"), rp);
    // VERIFY_MSG(rp.assure_connect(NULL), rp);
    // rp.set_blocking_mode(true);
    // VERIFY_MSG(rp.exec_command(&command, "PUNSUBSCRIBE"), rp);


    // transaction
    VERIFY_MSG(rp.assure_connect(NULL), rp);
    VERIFY_MSG(rp.exec_command(&command, "SET a a"), rp);
    VERIFY_MSG(rp.exec_command(&command, "SET b b"), rp);
    VERIFY_MSG(rp.exec_command(&command, "SET c c"), rp);
    VERIFY_MSG(rp.exec_command(&command, "WATCH a"), rp);
    VERIFY_MSG(rp.exec_command(&command, "WATCH b"), rp);
    VERIFY_MSG(rp.exec_command(&command, "WATCH c"), rp);

    VERIFY_MSG(rp.exec_command(&command, "MULTI"), rp);
    // "ERR WATCH inside MULTI is not allowed"
    VERIFY(!rp.exec_command(&command, "WATCH a"));
    VERIFY_MSG(rp.exec_command(&command, "UNWATCH"), rp);
    VERIFY_MSG(rp.exec_command(&command, "DISCARD"), rp);

    VERIFY_MSG(rp.exec_command(&command, "WATCH a"), rp);
    VERIFY_MSG(rp.exec_command(&command, "WATCH b"), rp);
    VERIFY_MSG(rp.exec_command(&command, "WATCH c"), rp);
    VERIFY_MSG(rp.exec_command(&command, "UNWATCH"), rp);

    VERIFY_MSG(rp.exec_command(&command, "MULTI"), rp);
    VERIFY_MSG(rp.exec_command(&command, "GET a"), rp);
    VERIFY_MSG(rp.exec_command(&command, "GET b"), rp);
    VERIFY_MSG(rp.exec_command(&command, "GET c"), rp);
    VERIFY_MSG(rp.exec_command(&command, "KEYS *"), rp);
    VERIFY_MSG(rp.exec_command(&command, "INFO"), rp);
    // got a special multi bulk
    VERIFY_MSG(rp.exec_command(&command, "EXEC"), rp);


    // script
    VERIFY_MSG(rp.assure_connect(NULL), rp);
    command.in.set_command(EVAL);
    command.in.clear_arg();
    command.in.push_arg("return redis.pcall('x')");
    command.in.push_arg("0");
    // "Unknown Redis command called from Lua script"
    VERIFY_MSG(!rp.exec_command(&command), rp);

    command.in.clear_arg();
    command.in.push_arg("return redis.call('x')");
    command.in.push_arg("0");
    // "ERR Error running script (call to f_bc800b29f7eb19fc5ef9364cf4afd4be5646137c):
    // Unknown Redis command called from Lua script"
    VERIFY_MSG(!rp.exec_command(&command), rp);

    command.in.clear_arg();
    command.in.push_arg("return {1,2,{3,'Hello World!',{1,{2},{1}}}}");
    command.in.push_arg("0");
    VERIFY_MSG(rp.exec_command(&command), rp);

    command.in.set_command(SCRIPT);
    command.in.clear_arg();
    command.in.push_arg("LOAD");
    command.in.push_arg("return redis.pcall('info')");
    VERIFY_MSG(rp.exec_command(&command), rp);
    VERIFY(command.out.get_bulk(&bulk));

    command.in.set_command(EVALSHA);
    command.in.clear_arg();
    command.in.push_arg(bulk);
    command.in.push_arg("0");
    VERIFY_MSG(rp.exec_command(&command), rp);


    // other
    VERIFY_MSG(rp.assure_connect(NULL), rp);
    VERIFY_MSG(rp.exec_command(&command, "ECHO 12345"), rp);
    VERIFY_MSG(rp.exec_command(&command, "QUIT"), rp);
    rp.close();
    VERIFY_MSG(rp.assure_connect(NULL), rp);
    VERIFY_MSG(rp.exec_command(&command, "CONFIG get pidfile"), rp);
    VERIFY_MSG(rp.exec_command(&command, "CONFIG get loglevel"), rp);
    VERIFY_MSG(rp.exec_command(&command, "config set loglevel notice"), rp);

    VERIFY_MSG(rp.exec_command(&command, "set string nothing"), rp);
    VERIFY_MSG(rp.exec_command(&command, "debug object string"), rp);
    VERIFY_MSG(rp.exec_command(&command, "debug reload"), rp);

    VERIFY_MSG(rp.exec_command(&command, "save"), rp);

    VERIFY_MSG(rp.exec_command(&command, "slowlog get"), rp);
    VERIFY_MSG(rp.exec_command(&command, "slowlog reset"), rp);
    VERIFY_MSG(rp.exec_command(&command, "slowlog len"), rp);

    VERIFY_MSG(rp.exec_command(&command, "TIME"), rp);

    cout << "protocol_test ok" << endl;
    return 0;
  }

  int basic_test(RedisBase2& r)
  {
    cout << "basic_test..." << endl;

    Redis2 * rs = dynamic_cast<Redis2 *>(&r);

    std::string s, key, member;
    int64_t i;
    double d;
    bool is_nil;
    // bool expired;
    mbulk_t mb;
    smbulk_t smb;
    string_vector_t keys, keys2, values, values2;

    ClearGuard<mbulk_t> mb_guard(&mb);
    ClearGuard<smbulk_t> smb_guard(&smb);

    VERIFY_MSG(r.flushall(), r);


    cout << "keys commands..." << endl;
    VERIFY_MSG(r.flushdb(), r);
    VERIFY_MSG(r.set("a", "a"), r);
    VERIFY_MSG(r.set("b", "b"), r);
    VERIFY_MSG(r.set("c", "c"), r);

    VERIFY_MSG(r.exists("a", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.exists("d", &i), r);
    VERIFY(i == 0);

    VERIFY_MSG(r.randomkey(&s, &is_nil), r);
    VERIFY(!is_nil);

    VERIFY_MSG(r.type("a", &s), r);
    VERIFY_MSG(r.type("z", &s), r);

    VERIFY_MSG(r.keys("*", &mb), r);
    VERIFY(mb.size() == 3);

    VERIFY_MSG(r.dump("c", &s, &is_nil), r);
    VERIFY(!is_nil);

    VERIFY_MSG(r.restore("d", 0, s), r);

    VERIFY_MSG(r.get("d", &s, &is_nil), r);
    VERIFY(s == "c");
    VERIFY(!is_nil);

    VERIFY_MSG(r.del("a", &i), r);
    VERIFY(i == 1);

    keys.clear();
    keys += "b", "c";
    VERIFY_MSG(r.del(keys, &i), r);
    VERIFY(i == 2);

    VERIFY_MSG(r.flushdb(), r);
    VERIFY_MSG(r.set("a", "a"), r);
    VERIFY_MSG(r.set("b", "b"), r);
    VERIFY_MSG(r.set("c", "c"), r);

    VERIFY_MSG(r.expire("a", 10, &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.expireat("b", time(0)+10, &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.expireat("d", 10, &i), r);
    VERIFY(i == 0);

    VERIFY_MSG(r.ttl("a", &i), r);
    VERIFY_MSG(r.ttl("b", &i), r);

    VERIFY_MSG(r.persist("b", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.persist("c", &i), r);
    VERIFY(i == 0);
    VERIFY_MSG(r.persist("d", &i), r);
    VERIFY(i == 0);

    VERIFY_MSG(r.flushdb(), r);
    VERIFY_MSG(r.set("a", "a"), r);
    VERIFY_MSG(r.set("b", "b"), r);
    VERIFY_MSG(r.set("c", "c"), r);

    VERIFY_MSG(r.pexpire("a", 1000, &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.pexpireat("b", time(0)+1000, &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.pexpireat("d", 10, &i), r);
    VERIFY(i == 0);

    VERIFY_MSG(r.pttl("a", &i), r);
    VERIFY_MSG(r.pttl("b", &i), r);

    VERIFY_MSG(r.flushdb(), r);
    VERIFY_MSG(r.set("a", "a"), r);
    VERIFY_MSG(r.set("b", "b"), r);
    VERIFY_MSG(r.set("c", "c"), r);

    VERIFY_MSG(r.move("a", 1, &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.move("b", 2, &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.move("c", 3, &i), r);
    VERIFY(i == 1);

    VERIFY_MSG(r.select(1), r);
    VERIFY_MSG(r.get("a", &s, &is_nil), r);
    VERIFY(s == "a");
    VERIFY(!is_nil);
    VERIFY_MSG(r.select(2), r);
    VERIFY_MSG(r.get("b", &s, &is_nil), r);
    VERIFY(s == "b");
    VERIFY(!is_nil);
    VERIFY_MSG(r.select(3), r);
    VERIFY_MSG(r.get("c", &s, &is_nil), r);
    VERIFY(s == "c");
    VERIFY(!is_nil);

    if (rs)
    {
      VERIFY_MSG(r.flushdb(), r);
      VERIFY_MSG(r.set("a", "a"), r);
      VERIFY_MSG(r.set("b", "b"), r);
      VERIFY_MSG(r.set("c", "c"), r);

      VERIFY_MSG(rs->rename("a", "d"), *rs);
      VERIFY_MSG(rs->renamenx("b", "d", &i), *rs);
      VERIFY(i == 0);
      VERIFY_MSG(rs->renamenx("b", "e", &i), *rs);
      VERIFY(i == 1);
    }


    cout << "strings commands..." << endl;
    VERIFY_MSG(r.flushdb(), r);
    VERIFY_MSG(r.append("string", "Hello", &i), r);
    VERIFY(i == 5);
    VERIFY_MSG(r.append("string", " World", &i), r);
    VERIFY(i == 11);

    VERIFY_MSG(r.decr("number", &i), r);
    VERIFY(i == -1);
    VERIFY_MSG(r.decrby("number", 10, &i), r);
    VERIFY(i == -11);
    VERIFY_MSG(r.incr("number", &i), r);
    VERIFY(i == -10);
    VERIFY_MSG(r.incrby("number", 2, &i), r);
    VERIFY(i == -8);

    VERIFY_MSG(r.psetex("float number", 10000, "0"), r);
    VERIFY_MSG(r.incrbyfloat("float number", 3.1415926, &d), r);

    VERIFY_MSG(r.setbit("number", 0, 1, &i), r);
    VERIFY_MSG(r.getbit("number", 0, &i), r);
    VERIFY(i == 1);

    VERIFY_MSG(r.getrange("number", 0, -1, &s, &is_nil), r);
    VERIFY_MSG(r.getrange("string", 0, -1, &s, &is_nil), r);
    VERIFY(!is_nil && s == "Hello World");
    VERIFY_MSG(r.getrange("string", 1, 2, &s, &is_nil), r);
    VERIFY(!is_nil && s == "el");

    VERIFY_MSG(r.getset("string", "xx", &s, &is_nil), r);
    VERIFY(!is_nil && s == "Hello World");
    VERIFY_MSG(r.getset("string2", "xx", &s, &is_nil), r);
    VERIFY(is_nil);

    VERIFY_MSG(r.setex("string2", 60, "xx"), r);

    VERIFY_MSG(r.setnx("string2", "xx", &i), r);
    VERIFY(i == 0);
    VERIFY_MSG(r.setnx("string3", "xx", &i), r);
    VERIFY(i == 1);

    VERIFY_MSG(r.setrange("string3", 2, "yyzz", &i), r);
    VERIFY(i == 6);

    VERIFY_MSG(r.strlen("string2", &i), r);
    VERIFY(i == 2);
    VERIFY_MSG(r.strlen("string4", &i), r);
    VERIFY(i == 0);

    VERIFY_MSG(r.flushdb(), r);
    keys.clear();
    values.clear();
    keys += "a","b","c";
    values += "a","b","c";
    VERIFY_MSG(r.mset(keys, values), r);
    VERIFY_MSG(r.mget(keys, &mb), r);
    convert(&mb, &values2);
    VERIFY(values == values2);


    cout << "hashes commands..." << endl;
    VERIFY_MSG(r.flushall(), r);

    VERIFY_MSG(r.hset("h1", "f1", "1"), r);
    VERIFY_MSG(r.hset("h1", "f2", "2"), r);
    VERIFY_MSG(r.hsetnx("h1", "f3", "3", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.hsetnx("h1", "f3", "3", &i), r);
    VERIFY(i == 0);

    VERIFY_MSG(r.hset("h2", "1", "1"), r);
    VERIFY_MSG(r.hset("h2", "a", "a"), r);

    VERIFY_MSG(r.hget("h1", "f1", &s, &is_nil), r);
    VERIFY(!is_nil && s == "1");
    VERIFY_MSG(r.hget("h1", "f2", &s, &is_nil), r);
    VERIFY(!is_nil && s == "2");
    VERIFY_MSG(r.hget("h1", "f3", &s, &is_nil), r);
    VERIFY(!is_nil && s == "3");
    VERIFY_MSG(r.hget("h1", "z", &s, &is_nil), r);
    VERIFY(is_nil);
    VERIFY_MSG(r.hget("z", "z", &s, &is_nil), r);
    VERIFY(is_nil);

    VERIFY_MSG(r.hgetall("h1", &mb), r);
    VERIFY(mb.size() == 6);
    VERIFY_MSG(r.hgetall("z", &mb), r);
    VERIFY(mb.empty());

    VERIFY_MSG(r.hdel("h1", "f1", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.hdel("h1", "z", &i), r);
    VERIFY(i == 0);

    keys.clear();
    keys += "f2","f3","z";
    VERIFY_MSG(r.hdel("h1", keys, &i), r);
    VERIFY(i == 2);

    VERIFY_MSG(r.hincr("h2", "1", &i), r);
    VERIFY(i == 2);
    VERIFY_MSG(r.hincrby("h2", "1", 10, &i), r);
    VERIFY(i == 12);
    VERIFY_MSG(!r.hincr("h2", "a", &i), r);

    keys.clear();
    keys += "1","2","3";
    values.clear();
    values += "1","2","3";
    VERIFY_MSG(r.hmset("h3", keys, values), r);
    VERIFY_MSG(r.hmget("h3", keys, &mb), r);
    convert(&mb, &values2);
    VERIFY(values == values2);

    VERIFY_MSG(r.hexists("h3", "1", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.hexists("h3", "z", &i), r);
    VERIFY(i == 0);
    VERIFY_MSG(r.hexists("z", "z", &i), r);
    VERIFY(i == 0);

    VERIFY_MSG(r.hlen("h3", &i), r);
    VERIFY(i == 3);

    VERIFY_MSG(r.hkeys("h3", &mb), r);
    VERIFY_MSG(r.hvals("h3", &mb), r);

    VERIFY_MSG(r.hincrbyfloat("float number", "pai", 3.1415926, &d), r);


    cout << "lists commands..." << endl;
    VERIFY_MSG(r.flushall(), r);

    VERIFY_MSG(r.lpush("l", "4", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.lpush("l", "3", &i), r);
    VERIFY(i == 2);
    VERIFY_MSG(r.lpush("l", "2", &i), r);
    VERIFY(i == 3);
    VERIFY_MSG(r.lpush("l", "1", &i), r);
    VERIFY(i == 4);
    VERIFY_MSG(r.rpush("l", "5", &i), r);
    VERIFY(i == 5);
    VERIFY_MSG(r.rpush("l", "6", &i), r);
    VERIFY(i == 6);

    VERIFY_MSG(r.llen("l", &i), r);
    VERIFY(i == 6);

    VERIFY_MSG(r.lindex("l", 0, &s, &is_nil), r);
    VERIFY(!is_nil && s == "1");
    VERIFY_MSG(r.lindex("l", -1, &s, &is_nil), r);
    VERIFY(!is_nil && s == "6");

    values.clear();
    values += "1","2","3","4","5","6";
    VERIFY_MSG(r.lrange("l", 0, -1, &mb), r);
    convert(&mb, &values2);
    VERIFY(values == values2);

    VERIFY_MSG(r.lpop("l", &s, &is_nil), r);
    VERIFY(!is_nil && s == "1");
    VERIFY_MSG(r.lpop("z", &s, &is_nil), r);
    VERIFY(is_nil);
    VERIFY_MSG(r.rpop("l", &s, &is_nil), r);
    VERIFY(!is_nil && s == "6");
    VERIFY_MSG(r.rpop("z", &s, &is_nil), r);
    VERIFY(is_nil);

    VERIFY_MSG(r.lpushx("l", "1", &i), r);
    VERIFY(i == 5);
    VERIFY_MSG(r.rpushx("l", "6", &i), r);
    VERIFY(i == 6);
    VERIFY_MSG(r.lpushx("z", "1", &i), r);
    VERIFY(i == 0);
    VERIFY_MSG(r.rpushx("z", "6", &i), r);
    VERIFY(i == 0);

    VERIFY_MSG(r.ltrim("l", 1, -1), r);

    VERIFY_MSG(r.lpush("l", "2", &i), r);
    VERIFY(i == 6);
    VERIFY_MSG(r.lset("l", 0, "1"), r);

    VERIFY_MSG(r.linsert("l", true, "2", "1", &i), r);
    VERIFY(i == 7);
    VERIFY_MSG(r.linsert("l", false, "2", "3", &i), r);
    VERIFY(i == 8);

    VERIFY_MSG(r.lrem("l", 0, "1", &i), r);
    VERIFY(i == 2);
    VERIFY_MSG(r.lrem("l", 0, "2", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.lrem("l", 0, "3", &i), r);
    VERIFY(i == 2);

    if (rs)
    {
      VERIFY_MSG(rs->flushall(), *rs);

      VERIFY_MSG(rs->lpush("a", "1", &i), *rs);
      VERIFY(i == 1);
      VERIFY_MSG(rs->lpush("a", "2", &i), *rs);
      VERIFY(i == 2);
      VERIFY_MSG(rs->lpush("b", "3", &i), *rs);
      VERIFY(i == 1);
      VERIFY_MSG(rs->lpush("b", "4", &i), *rs);
      VERIFY(i == 2);
      VERIFY_MSG(rs->rpush("c", "5", &i), *rs);
      VERIFY(i == 1);
      VERIFY_MSG(rs->rpush("c", "6", &i), *rs);
      VERIFY(i == 2);

      /*
         keys.clear();
         keys += "a";
         VERIFY_MSG(rs->blpop(keys, 1, &key, &member, &expired), *rs);
         VERIFY(!expired);
         keys.clear();
         keys += "d", "e", "f", "g";
         VERIFY_MSG(rs->blpop(keys, 1, &key, &member, &expired), *rs);
         VERIFY(expired);

         keys.clear();
         keys += "a";
         VERIFY_MSG(rs->brpop(keys, 1, &key, &member, &expired), *rs);
         VERIFY(!expired);
         keys.clear();
         keys += "d", "e", "f", "g";
         VERIFY_MSG(rs->brpop(keys, 1, &key, &member, &expired), *rs);
         VERIFY(expired);

         VERIFY_MSG(rs->brpoplpush("a", "b", 1, &key, &expired), *rs);
         VERIFY(expired);
         VERIFY_MSG(rs->brpoplpush("b", "a", 1, &key, &expired), *rs);
         VERIFY(!expired);

         VERIFY_MSG(rs->rpoplpush("a", "b", &s, &is_nil), *rs);
         VERIFY(!is_nil);
         VERIFY_MSG(rs->rpoplpush("a", "b", &s, &is_nil), *rs);
         VERIFY(is_nil);
         */
    }


    cout << "sets commands..." << endl;
    VERIFY_MSG(r.flushall(), r);

    VERIFY_MSG(r.sadd("s", "1", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.sadd("s", "2", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.sadd("s", "3", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.sadd("s", "4", &i), r);
    VERIFY(i == 1);

    VERIFY_MSG(r.scard("s", &i), r);
    VERIFY(i == 4);

    VERIFY_MSG(r.sismember("s", "1", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.sismember("s", "z", &i), r);
    VERIFY(i == 0);
    VERIFY_MSG(r.sismember("z", "z", &i), r);
    VERIFY(i == 0);

    values.clear();
    values += "1","2","3","4";
    VERIFY_MSG(r.smembers("s", &mb), r);
    convert(&mb, &values2);
    std::sort(values2.begin(), values2.end());
    VERIFY(values == values2);

    VERIFY_MSG(r.spop("s", &s, &is_nil), r);
    VERIFY(!is_nil);
    VERIFY_MSG(r.spop("z", &s, &is_nil), r);
    VERIFY(is_nil);

    VERIFY_MSG(r.srandmember("s", &s, &is_nil), r);
    VERIFY(!is_nil);
    VERIFY_MSG(r.srandmember("z", &s, &is_nil), r);
    VERIFY(is_nil);

    VERIFY_MSG(r.srem("s", "1", &i), r);
    VERIFY_MSG(r.srem("s", "2", &i), r);
    VERIFY_MSG(r.srem("s", "3", &i), r);
    VERIFY_MSG(r.srem("s", "4", &i), r);

    if (rs)
    {
      VERIFY_MSG(rs->flushall(), *rs);

      VERIFY_MSG(rs->sadd("a", "1", &i), *rs);
      VERIFY_MSG(rs->sadd("a", "2", &i), *rs);
      VERIFY_MSG(rs->sadd("a", "3", &i), *rs);
      VERIFY_MSG(rs->sadd("a", "4", &i), *rs);
      VERIFY_MSG(rs->sadd("a", "5", &i), *rs);

      VERIFY_MSG(rs->sadd("b", "1", &i), *rs);
      VERIFY_MSG(rs->sadd("b", "3", &i), *rs);
      VERIFY_MSG(rs->sadd("b", "5", &i), *rs);
      VERIFY_MSG(rs->sadd("b", "7", &i), *rs);
      VERIFY_MSG(rs->sadd("b", "9", &i), *rs);

      VERIFY_MSG(rs->sadd("c", "2", &i), *rs);
      VERIFY_MSG(rs->sadd("c", "4", &i), *rs);
      VERIFY_MSG(rs->sadd("c", "6", &i), *rs);
      VERIFY_MSG(rs->sadd("c", "8", &i), *rs);
      VERIFY_MSG(rs->sadd("c", "10", &i), *rs);

      keys.clear();
      keys += "a","b";
      VERIFY_MSG(rs->sdiff(keys, &mb), *rs);
      convert(&mb, &values);
      values2.clear();
      values2 += "2","4";
      VERIFY(values == values2);
      VERIFY_MSG(rs->sdiffstore("d", keys, &i), *rs);
      VERIFY(i == 2);

      keys.clear();
      keys += "a","c";
      VERIFY_MSG(rs->sdiff(keys, &mb), *rs);
      convert(&mb, &values);
      values2.clear();
      values2 += "1","3","5";
      VERIFY(values == values2);
      VERIFY_MSG(rs->sdiffstore("e", keys, &i), *rs);
      VERIFY(i == 3);

      keys.clear();
      keys += "a","b";
      VERIFY_MSG(rs->sinter(keys, &mb), *rs);
      convert(&mb, &values);
      values2.clear();
      values2 += "1","3","5";
      VERIFY(values == values2);
      VERIFY_MSG(rs->sinterstore("f", keys, &i), *rs);
      VERIFY(i == 3);

      keys.clear();
      keys += "a","c";
      VERIFY_MSG(rs->sinter(keys, &mb), *rs);
      convert(&mb, &values);
      values2.clear();
      values2 += "2","4";
      VERIFY(values == values2);
      VERIFY_MSG(rs->sinterstore("g", keys, &i), *rs);
      VERIFY(i == 2);

      keys.clear();
      keys += "a","b";
      VERIFY_MSG(rs->sunion(keys, &mb), *rs);
      convert(&mb, &values);
      values2.clear();
      values2 += "1","2","3","4","5","7","9";
      VERIFY(values == values2);
      VERIFY_MSG(rs->sunionstore("h", keys, &i), *rs);
      VERIFY(i == 7);

      keys.clear();
      keys += "a","c";
      VERIFY_MSG(rs->sunion(keys, &mb), *rs);
      convert(&mb, &values);
      values2.clear();
      values2 += "1","2","3","4","5","6","8","10";
      VERIFY(values == values2);
      VERIFY_MSG(rs->sunionstore("i", keys, &i), *rs);
      VERIFY(i == 8);

      VERIFY_MSG(rs->smove("a", "b", "1", &i), *rs);
      VERIFY(i == 1);
      VERIFY_MSG(rs->smove("a", "b", "2", &i), *rs);
      VERIFY(i == 1);
      VERIFY_MSG(rs->smove("a", "b", "-1", &i), *rs);
      VERIFY(i == 0);
    }


    cout << "sorted sets commands..." << endl;
    VERIFY_MSG(r.flushall(), r);

    VERIFY_MSG(r.zadd("z", 1.0, "1", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.zadd("z", 2.0, "2", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.zadd("z", 3.0, "3", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.zadd("z", 4.0, "4", &i), r);
    VERIFY(i == 1);

    VERIFY_MSG(r.zcard("z", &i), r);
    VERIFY(i == 4);

    VERIFY_MSG(r.zcount("z", "-inf", "+inf", &i), r);
    VERIFY(i == 4);
    VERIFY_MSG(r.zcount("z", "1.0", "4.0", &i), r);
    VERIFY(i == 4);
    VERIFY_MSG(r.zcount("z", "(1.0", "(4.0", &i), r);
    VERIFY(i == 2);

    VERIFY_MSG(r.zscore("z", "1", &d, &is_nil), r);
    VERIFY(!is_nil && fabs(d - 1.0) < 0.0001);
    VERIFY_MSG(r.zscore("z", "zz", &d, &is_nil), r);
    VERIFY(is_nil);
    VERIFY_MSG(r.zscore("zz", "1", &d, &is_nil), r);
    VERIFY(is_nil);

    VERIFY_MSG(r.zadd("z", 4.0, "5", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.zincrby("z", 1.0, "5", &d), r);
    VERIFY(fabs(d - 5.0) < 0.0001);

    VERIFY_MSG(r.zrange("z", 0, -1, true, &mb), r);
    VERIFY_MSG(r.zrange("z", 0, -1, false, &mb), r);
    VERIFY_MSG(r.zrevrange("z", 0, -1, true, &mb), r);
    VERIFY_MSG(r.zrevrange("z", 0, -1, false, &mb), r);

    ZRangebyscoreLimit limit;
    limit.offset = 0;
    limit.count = 65536;
    VERIFY_MSG(r.zrangebyscore("z", "-inf", "+inf", true, &limit, &mb), r);
    VERIFY_MSG(r.zrangebyscore("z", "-inf", "+inf", true, NULL, &mb), r);
    VERIFY_MSG(r.zrangebyscore("z", "-inf", "+inf", false, &limit, &mb), r);
    VERIFY_MSG(r.zrangebyscore("z", "-inf", "+inf", false, NULL, &mb), r);
    VERIFY_MSG(r.zrevrangebyscore("z", "+inf", "-inf", true, &limit, &mb), r);
    VERIFY_MSG(r.zrevrangebyscore("z", "+inf", "-inf", true, NULL, &mb), r);
    VERIFY_MSG(r.zrevrangebyscore("z", "+inf", "-inf", false, &limit, &mb), r);
    VERIFY_MSG(r.zrevrangebyscore("z", "+inf", "-inf", false, NULL, &mb), r);

    VERIFY_MSG(r.zrank("z", "1", &i, &is_nil), r);
    VERIFY(!is_nil && i == 0);
    VERIFY_MSG(r.zrank("z", "2", &i, &is_nil), r);
    VERIFY(!is_nil && i == 1);
    VERIFY_MSG(r.zrank("z", "10", &i, &is_nil), r);
    VERIFY(is_nil);

    VERIFY_MSG(r.zrevrank("z", "1", &i, &is_nil), r);
    VERIFY(!is_nil && i == 4);
    VERIFY_MSG(r.zrevrank("z", "2", &i, &is_nil), r);
    VERIFY(!is_nil && i == 3);
    VERIFY_MSG(r.zrevrank("z", "10", &i, &is_nil), r);
    VERIFY(is_nil);

    VERIFY_MSG(r.zrem("z", "5", &i), r);
    VERIFY(i == 1);
    VERIFY_MSG(r.zrem("z", "10", &i), r);
    VERIFY(i == 0);
    VERIFY_MSG(r.zrem("zz", "zz", &i), r);
    VERIFY(i == 0);

    VERIFY_MSG(r.zremrangebyrank("z", 0, 1, &i), r);
    VERIFY(i == 2);

    VERIFY_MSG(r.zremrangebyscore("z", "3", "4", &i), r);
    VERIFY(i == 2);

    if (rs)
    {
      VERIFY_MSG(rs->flushall(), *rs);

      VERIFY_MSG(rs->zadd("a", 0, "1", &i), *rs);
      VERIFY_MSG(rs->zadd("a", 0, "2", &i), *rs);
      VERIFY_MSG(rs->zadd("a", 0, "3", &i), *rs);
      VERIFY_MSG(rs->zadd("a", 0, "4", &i), *rs);
      VERIFY_MSG(rs->zadd("a", 0, "5", &i), *rs);

      VERIFY_MSG(rs->zadd("b", 0, "1", &i), *rs);
      VERIFY_MSG(rs->zadd("b", 0, "3", &i), *rs);
      VERIFY_MSG(rs->zadd("b", 0, "5", &i), *rs);
      VERIFY_MSG(rs->zadd("b", 0, "7", &i), *rs);
      VERIFY_MSG(rs->zadd("b", 0, "9", &i), *rs);

      std::vector<double> weights;
      weights.clear();
      weights += 2.0,1.0;

      keys.clear();
      keys += "a","b";

      VERIFY_MSG(rs->zinterstore("c", keys, &weights, kSum, &i), *rs);
      VERIFY(i == 3);
      VERIFY_MSG(rs->zinterstore("c", keys, &weights, kMin, &i), *rs);
      VERIFY(i == 3);
      VERIFY_MSG(rs->zinterstore("c", keys, &weights, kMax, &i), *rs);
      VERIFY(i == 3);
      VERIFY_MSG(rs->zinterstore("c", keys, NULL, kSum, &i), *rs);
      VERIFY(i == 3);
      VERIFY_MSG(rs->zinterstore("c", keys, NULL, kMin, &i), *rs);
      VERIFY(i == 3);
      VERIFY_MSG(rs->zinterstore("c", keys, NULL, kMax, &i), *rs);
      VERIFY(i == 3);

      keys.clear();
      keys += "a","b";
      VERIFY_MSG(rs->zunionstore("c", keys, &weights, kSum, &i), *rs);
      VERIFY(i == 7);
      VERIFY_MSG(rs->zunionstore("c", keys, &weights, kMin, &i), *rs);
      VERIFY(i == 7);
      VERIFY_MSG(rs->zunionstore("c", keys, &weights, kMax, &i), *rs);
      VERIFY(i == 7);
      VERIFY_MSG(rs->zunionstore("c", keys, NULL, kSum, &i), *rs);
      VERIFY(i == 7);
      VERIFY_MSG(rs->zunionstore("c", keys, NULL, kMin, &i), *rs);
      VERIFY(i == 7);
      VERIFY_MSG(rs->zunionstore("c", keys, NULL, kMax, &i), *rs);
      VERIFY(i == 7);
    }


    if (rs)
    {
      cout << "transactions commands..." << endl;
      VERIFY_MSG(rs->flushall(), *rs);
      VERIFY_MSG(rs->multi(), *rs);

      RedisCommand c;
      redis_command_vector_t cmdv;

      VERIFY_MSG(rs->add_command(&c, "SET a a"), *rs);
      VERIFY_MSG(rs->add_command(&c, "SET b b"), *rs);
      VERIFY_MSG(rs->add_command(&c, "SET c c"), *rs);
      VERIFY_MSG(rs->add_command(&c, "GET a"), *rs);
      VERIFY_MSG(rs->add_command(&c, "GET b"), *rs);
      VERIFY_MSG(rs->add_command(&c, "GET c"), *rs);
      VERIFY_MSG(rs->exec(&cmdv), *rs);

      VERIFY(cmdv.size() == 6);
      VERIFY(cmdv[0]->out.is_status_ok());
      VERIFY(cmdv[1]->out.is_status_ok());
      VERIFY(cmdv[2]->out.is_status_ok());
      VERIFY(*cmdv[3]->out.ptr.bulk == "a");
      VERIFY(*cmdv[4]->out.ptr.bulk == "b");
      VERIFY(*cmdv[5]->out.ptr.bulk == "c");

      clear_commands(&cmdv);
    }


    if (rs)
    {
      cout << "connection commands..." << endl;
      VERIFY_MSG(rs->ping(), *rs);
    }


    if (rs)
    {
      cout << "server commands..." << endl;
      VERIFY_MSG(rs->bgrewriteaof(), *rs);
      // "ERR Can't BGSAVE while AOF log rewriting is in progress"
      // VERIFY_MSG(rs->bgsave(), *rs);
      VERIFY_MSG(rs->dbsize(&i), *rs);
      VERIFY_MSG(rs->info(&s), *rs);
      VERIFY_MSG(rs->info("server", &s), *rs);
      VERIFY_MSG(rs->info("clients", &s), *rs);
      VERIFY_MSG(rs->info("memory", &s), *rs);
      VERIFY_MSG(rs->info("persistence", &s), *rs);
      VERIFY_MSG(rs->info("stats", &s), *rs);
      VERIFY_MSG(rs->info("replication", &s), *rs);
      VERIFY_MSG(rs->info("cpu", &s), *rs);
      VERIFY_MSG(rs->info("keyspace", &s), *rs);
      VERIFY_MSG(rs->info("commandstats", &s), *rs);
      VERIFY_MSG(rs->lastsave(&i), *rs);
    }


    if (rs)
    {
      cout << "pipeline test..." << endl;
      VERIFY_MSG(rs->flushall(), *rs);
      VERIFY_MSG(rs->set("a", "a"), *rs);

      const int iteration = 1000;

      //non pipeline
      {
        std::string value;
        RedisCommand c(GET);
        c.push_arg("a");

        boost::posix_time::ptime begin, end;
        begin = boost::posix_time::microsec_clock::local_time();

        for (int i=0; i<iteration; i++)
        {
          VERIFY_MSG(rs->exec_command(&c), *rs);
          VERIFY(c.out.get_bulk(&value));
        }

        end = boost::posix_time::microsec_clock::local_time();
        boost::posix_time::time_duration td = end - begin;
        cout << iteration << " iterations(non-pipeline) cost "
          << td.total_milliseconds() << " ms" << endl;
      }

      //pipeline
      {
        std::string value;
        redis_command_vector_t cmds;
        RedisCommand c(GET);
        c.push_arg("a");

        for (int i=0; i<iteration; i++)
        {
          cmds.push_back(&c);
        }

        boost::posix_time::ptime begin, end;
        begin = boost::posix_time::microsec_clock::local_time();

        VERIFY_MSG(rs->exec_pipeline(&cmds), *rs);

        end = boost::posix_time::microsec_clock::local_time();
        boost::posix_time::time_duration td = end - begin;

        cout << iteration << " iterations(pipeline) cost "
          << td.total_milliseconds() << " ms" << endl;
      }
    }

    return 0;
  }

  int redis_tss_test_thread1(RedisTss * r)
  {
    RedisBase2 * redis_handle = r->get(kThreadSpecific);
    assert(redis_handle);
    redis_handle->set("a", "b");
    return 0;
  }

  int redis_tss_test_thread2(RedisTss * r)
  {
    RedisBase2 * redis_handle = r->get(kThreadSpecific);
    assert(redis_handle);
    redis_handle->set("a", "b");
    r->put(redis_handle, kThreadSpecific);
    return 0;
  }

  int redis_tss_test_thread3(RedisTss * r)
  {
    RedisBase2 * redis_handle = r->get(kNotThreadSpecific);
    assert(redis_handle);
    redis_handle->set("a", "b");
    r->put(redis_handle, kNotThreadSpecific);
    return 0;
  }

  int redis_tss_test(RedisTss& r, int (*thread_func)(RedisTss * r))
  {
    cout << "redis_tss_test..." << endl;

    const int iteration = 100;
    const int thread_number = 10;

    for (int i=0; i<iteration; i++)
    {
      boost::thread_group tg;
      for (int j=0; j<thread_number; j++)
      {
        tg.create_thread(boost::bind(thread_func, &r));
      }
      tg.join_all();
    }

    cout << "redis_tss_test ok" << endl;
    return 0;
  }
}

int main(int argc, char * argv[])
{
  try
  {
    namespace po = boost::program_options;
    po::options_description desc("Options");
    desc.add_options()
      ("help", "produce help message")
      ("host,h", po::value<std::string>()->default_value("localhost"), "redis host")
      ("port,p", po::value<std::string>()->default_value("6379"), "redis port")
      ("host_list", po::value<std::string>()->default_value("localhost"), "redis host list")
      ("port_list", po::value<std::string>()->default_value("6379"), "redis port list")
      ("db_index,i", po::value<int>()->default_value(5), "redis db index")
      ("timeout,t", po::value<int>()->default_value(2000), "timeout in ms");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help"))
    {
      cout << desc << endl;
      return 0;
    }

    host = vm["host"].as<std::string>();
    port = vm["port"].as<std::string>();
    host_list = vm["host_list"].as<std::string>();
    port_list = vm["port_list"].as<std::string>();
    db_index = vm["db_index"].as<int>();
    timeout = vm["timeout"].as<int>();
  }
  catch (std::exception& e)
  {
    cout << "caught: " << e.what() << endl;
    return 1;
  }

  os_test();
  protocol_test();
  get_redis_version();

  {
    Redis2 r(host, port, db_index, timeout);
    basic_test(r);
  }

  {
    Redis2P r(host_list, port_list, db_index, timeout);
    basic_test(r);
  }

  {
    RedisTss r(host, port, db_index, 1, timeout, kNormal);
    redis_tss_test(r, redis_tss_test_thread1);
    redis_tss_test(r, redis_tss_test_thread2);
    redis_tss_test(r, redis_tss_test_thread3);
  }

  DUMP_TEST_RESULT();

  return 0;
}
