/** @file
 * @brief libredis hash test
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#include <redis_cmd.h>
#include <iostream>
#include <boost/program_options.hpp>

USING_LIBREDIS_NAMESPACE

int main(int argc, char * argv[])
{
  std::string key;
  int group_number;

  try
  {
    namespace po = boost::program_options;
    po::options_description desc("Options");
    desc.add_options()
      ("help,h", "produce help message")
      ("key,k", po::value<std::string>()->default_value("test string"), "key")
      ("group_number,g", po::value<int>()->default_value(16), "group number");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help"))
    {
      std::cout << desc << std::endl;
      return 0;
    }

    key = vm["key"].as<std::string>();
    group_number = vm["group_number"].as<int>();
  }
  catch (std::exception& e)
  {
    std::cout << "caught: " << e.what() << std::endl;
    return 1;
  }

  uint32_t index = time33_hash_32(key) % group_number;
  std::cout << key << " %% " << group_number << " = " << index << std::endl;

  return 0;
}
