/// \file
/// \brief Unit tests for dfe::Dispatcher

#include <boost/test/unit_test.hpp>
#include <iostream>

#include <dfe_dispatcher.hpp>

// basic sanity checks

BOOST_AUTO_TEST_CASE(dispatcher_add)
{
  dfe::Dispatcher dp;
  BOOST_CHECK_THROW(dp.add("", nullptr, 1), std::invalid_argument); // no name
  BOOST_CHECK_NO_THROW(dp.add("test", nullptr, 2));
  BOOST_CHECK_THROW(
    dp.add("test", nullptr, 2), std::invalid_argument); // duplicate name
}

BOOST_AUTO_TEST_CASE(dispatcher_call)
{
  dfe::Dispatcher dp;
  BOOST_REQUIRE_NO_THROW(dp.add("invalid1", nullptr, 1));
  BOOST_REQUIRE_NO_THROW(dp.add("invalid2", nullptr, 2));
  BOOST_CHECK_THROW(dp.call("does-not-exist", {}), std::invalid_argument);
  BOOST_CHECK_THROW(dp.call("invalid1", {}), std::invalid_argument);    // nargs
  BOOST_CHECK_THROW(dp.call("invalid2", {"x"}), std::invalid_argument); // nargs
}

// native dispatcher interface functions

std::string
native(const std::vector<std::string>& args)
{
  std::string ret;
  for (const auto& arg : args) {
    ret += arg;
  }
  return ret;
}

BOOST_AUTO_TEST_CASE(dispatcher_native)
{
  dfe::Dispatcher dp;
  BOOST_REQUIRE_NO_THROW(dp.add("native1", native, 1));
  BOOST_REQUIRE_NO_THROW(dp.add("native3", native, 3));
  BOOST_REQUIRE_NO_THROW(dp.add("native5", native, 5));
  BOOST_TEST(dp.call("native1", {"x"}) == "x");
  BOOST_TEST(dp.call("native3", {"x", "y", "z"}) == "xyz");
  BOOST_TEST(dp.call("native5", {"x", "y", "z", "1", "2"}) == "xyz12");
}

// regular function

double
func(int i, float f)
{
  return i * f;
}

BOOST_AUTO_TEST_CASE(dispatcher_free)
{
  dfe::Dispatcher dp;
  BOOST_REQUIRE_NO_THROW(dp.add("func", func));
  BOOST_TEST(dp.call("func", {"2", "2.6"}) == "5.2");
  BOOST_TEST(dp.call("func", {"3", "1.2"}) == "3.6");
}

// regular function without return value

void
func_noreturn(const std::string& a, std::string b)
{
  std::cout << a << "+" << b << std::endl;
}

BOOST_AUTO_TEST_CASE(dispatcher_free_noreturn)
{
  dfe::Dispatcher dp;
  BOOST_REQUIRE_NO_THROW(dp.add("func", func_noreturn));
  BOOST_CHECK_NO_THROW(dp.call("func", {"2", "2.6"}));
  BOOST_CHECK_NO_THROW(dp.call("func", {"3", "1.2"}));
}

// member functions

struct FuncStruct {
  int i;

  double func(float f) { return i * f; }
  void noreturn(float f) { std::cout << (i * f) << std::endl; }
};

BOOST_AUTO_TEST_CASE(dispatcher_member)
{
  dfe::Dispatcher dp;
  FuncStruct f = {4};
  BOOST_REQUIRE_NO_THROW(dp.add("func", &FuncStruct::func, &f));
  BOOST_TEST(dp.call("func", {"2.6"}) == "10.4");
  BOOST_TEST(dp.call("func", {"1.2"}) == "4.8");
  BOOST_REQUIRE_NO_THROW(dp.add("noreturn", &FuncStruct::noreturn, &f));
  BOOST_REQUIRE_NO_THROW(dp.call("noreturn", {"2.6"}));
  BOOST_REQUIRE_NO_THROW(dp.call("noreturn", {"1.2"}));
}
