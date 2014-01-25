/** @file
 * @brief namespace definition
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#ifndef _LANGTAOJIN_LIBREDIS_REDIS_COMMON_H_
#define _LANGTAOJIN_LIBREDIS_REDIS_COMMON_H_

#if defined USING_LIBREDIS_NAMESPACE
# define LIBREDIS_NAMESPACE_BEGIN namespace libredis {
# define LIBREDIS_NAMESPACE_END }
# define USING_LIBREDIS_NAMESPACE using namespace libredis;
#else
# define LIBREDIS_NAMESPACE_BEGIN namespace com { namespace langtaojin { namespace adgaga {
# define LIBREDIS_NAMESPACE_END } } }
# define USING_LIBREDIS_NAMESPACE using namespace com::langtaojin::adgaga;
#endif

#include <stdarg.h>
#include <stdint.h>

#include <string>
#include <exception>
#include <vector>
#include <map>

#include <boost/shared_ptr.hpp>

#endif// _LANGTAOJIN_LIBREDIS_REDIS_COMMON_H_
