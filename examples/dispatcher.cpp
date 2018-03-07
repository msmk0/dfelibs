/// \file
/// \brief Demonstrate dfe::Dispatcher functionality
///

#include <cstdlib>
#include <iostream>

#include <dfe_dispatcher.hpp>

// example functions

void
func_noreturn(int x, double f)
{
  std::cout << "free function w/o return: x=" << x << " f=" << f << '\n';
}

double
func_return(int x, double f)
{
  std::cout << "free function w/ return: x=" << x << " f=" << f << '\n';
  return x + f;
}

std::string
func_native(const std::vector<std::string>& args)
{
  std::cout << "native w/ " << args.size() << " arguments\n";
  std::string ret;
  for (const auto& arg : args) {
    ret += arg;
  }
  return ret;
}

struct WithFunctions {
  float x;

  float member_add(float y)
  {
    std::cout << "member add x=" << x << " y=" << y << '\n';
    return x + y;
  }
  static float static_add(float a, float b)
  {
    std::cout << "static add a=" << a << " b=" << b << '\n';
    return a + b;
  }
};

int
main(int argc, char* argv[])
{
  dfe::Dispatcher dispatchr;

  // add functions to dispatcher
  dispatchr.add("noreturn", func_noreturn);
  dispatchr.add("return", func_return);
  dispatchr.add("native1", func_native, 1); // with 1 argument
  dispatchr.add("native3", func_native, 3); // with 3 arguments
  WithFunctions adder = {5.5};
  dispatchr.add("member_add", &WithFunctions::member_add, &adder);
  dispatchr.add("static_add", WithFunctions::static_add);

  // list registered functions
  std::cout << "registered commands:\n";
  for (const auto cmd : dispatchr.commands()) {
    std::cout << "  " << cmd.first << "(" << cmd.second << ")\n";
  }

  // call functions by name
  std::cout << dispatchr.call("noreturn", {"1", "1.24"}) << '\n';
  std::cout << dispatchr.call("return", {"1", "1.24"}) << '\n';
  std::cout << dispatchr.call("native1", {"x"}) << '\n';
  std::cout << dispatchr.call("native3", {"x", "y", "z"}) << '\n';
  std::cout << dispatchr.call("native3", {"x", "y", "z"}) << '\n';
  std::cout << dispatchr.call("member_add", {"1.2"}) << '\n';
  std::cout << dispatchr.call("static_add", {"4.2", "2.3"}) << '\n';

  return EXIT_SUCCESS;
}
