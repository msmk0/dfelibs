/// \file
/// \brief Unit tests for dfe::Dispatcher

#include <boost/test/unit_test.hpp>
#include <iostream>

#include <dfe_dispatcher.hpp>

// test for variable type

using dfe::Variable;
using dfe::VariableType;

BOOST_AUTO_TEST_CASE(dispatcher_variable_empty)
{
  Variable empty;

  BOOST_CHECK(empty.type() == VariableType::Empty);
  BOOST_CHECK_THROW(empty.as<bool>(), std::invalid_argument); // wrong type
  BOOST_CHECK_THROW(empty.as<int>(), std::invalid_argument);    // wrong type
  BOOST_CHECK_THROW(empty.as<double>(), std::invalid_argument); // wrong type
  BOOST_CHECK_THROW(
    empty.as<std::string>(), std::invalid_argument); // wrong type

  // asign non-empty variable
  empty = Variable(-23);
  BOOST_CHECK(empty.type() == VariableType::Integer);
  BOOST_TEST(empty.as<int>() == -23);
}

BOOST_AUTO_TEST_CASE(dispatcher_variable_bool)
{
  Variable vt(true);
  Variable vf(false);

  BOOST_CHECK(vt.type() == VariableType::Boolean);
  BOOST_CHECK(vf.type() == VariableType::Boolean);
  BOOST_TEST(vt.as<bool>());
  BOOST_TEST(!vf.as<bool>());
  BOOST_CHECK_THROW(vt.as<double>(), std::invalid_argument); // wrong type
  BOOST_CHECK_THROW(vf.as<int>(), std::invalid_argument);    // wrong type
}

BOOST_AUTO_TEST_CASE(dispatcher_variable_int)
{
  Variable i1(-123);

  BOOST_CHECK(i1.type() == VariableType::Integer);
  BOOST_TEST(i1.as<int>() == -123);
  BOOST_TEST(i1.as<int64_t>() == -123);
  BOOST_CHECK_THROW(i1.as<double>(), std::invalid_argument); // wrong type
}

BOOST_AUTO_TEST_CASE(dispatcher_variable_uint)
{
  Variable i1(123u);

  BOOST_CHECK(i1.type() == VariableType::Integer);
  BOOST_TEST(i1.as<int>() == 123);
  BOOST_TEST(i1.as<int64_t>() == 123);
  BOOST_TEST(i1.as<unsigned int>() == 123);
  BOOST_TEST(i1.as<uint64_t>() == 123);
  BOOST_CHECK_THROW(i1.as<float>(), std::invalid_argument); // wrong type
}

BOOST_AUTO_TEST_CASE(dispatcher_variable_float)
{
  Variable f1(0.123f);
  Variable f2(-1.234e14f);

  BOOST_CHECK(f1.type() == VariableType::Float);
  BOOST_CHECK(f2.type() == VariableType::Float);
  BOOST_TEST(f1.as<float>() == 0.123f);
  BOOST_TEST(f2.as<float>() == -1.234e14f);
  BOOST_CHECK_THROW(f1.as<int>(), std::invalid_argument); // wrong type
  BOOST_CHECK_THROW(f2.as<uint64_t>(), std::invalid_argument); // wrong type
}

BOOST_AUTO_TEST_CASE(dispatcher_variable_double)
{
  Variable d1(0.123);
  Variable d2(-1.234e14);

  BOOST_CHECK(d1.type() == VariableType::Float);
  BOOST_CHECK(d2.type() == VariableType::Float);
  BOOST_TEST(d1.as<double>() == 0.123);
  BOOST_TEST(d2.as<double>() == -1.234e14);
  BOOST_CHECK_THROW(d1.as<bool>(), std::invalid_argument); // wrong type
  BOOST_CHECK_THROW(d2.as<std::string>(), std::invalid_argument); // wrong type
}

BOOST_AUTO_TEST_CASE(dispatcher_variable_chars)
{
  const char* cs = "test";
  const char* cl = "testTESTtestTESTtestTESTtestTEST";
  Variable vs(cs);
  Variable vl(cl);

  BOOST_CHECK(vs.type() == VariableType::String);
  BOOST_CHECK(vl.type() == VariableType::String);
  BOOST_TEST(vs.as<std::string>() == cs);
  BOOST_TEST(vl.as<std::string>() == cl);
  BOOST_CHECK_THROW(vs.as<bool>(), std::invalid_argument); // wrong type
}

BOOST_AUTO_TEST_CASE(dispatcher_variable_string)
{
  std::string ss("axcvsd");
  std::string sl("asdf9asdfj;alcxv-1gasldkfjvb890basdf-0913");
  Variable vs(ss);
  Variable vl(sl);

  BOOST_CHECK(vs.type() == VariableType::String);
  BOOST_CHECK(vl.type() == VariableType::String);
  BOOST_TEST(vs.as<std::string>() == ss);
  BOOST_TEST(vl.as<std::string>() == sl);
  BOOST_CHECK_THROW(vs.as<int>(), std::invalid_argument); // wrong type
}

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
