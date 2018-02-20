/// \file
/// \brief Demonstrate dfe::Dispatcher functionality
///

#include <cstdlib>
#include <iostream>

#include <dfe_dispatcher.hpp>

void
func1(int x, double f)
{
  std::cout << "func1: x=" << x << " f=" << f << '\n';
}

void
func2(std::string name)
{
  std::cout << "func2: name=" << name << '\n';
}

void
func3(const std::vector<std::string>& args)
{
  std::cout << "func3:";
  for (const auto& arg : args) {
    std::cout << " " << arg;
  }
  std::cout << '\n';
}

struct WithFunction {
  float v;

  void func() { std::cout << "WithFunction::do: " << v << std::endl; }
  static void other(int y)
  {
    std::cout << "WithFunction::other: " << y << std::endl;
  }
};

int
main(int argc, char* argv[])
{
  dfe::Dispatcher dispatchr;

  // regular free functions

  dispatchr.add("cmd1", func1);
  dispatchr.add(
    "cmd1_1",
    std::function<void(double)>(std::bind(func1, -1, std::placeholders::_1)));
  dispatchr.add("cmd2", func2);

  dispatchr.call("cmd1", {"1", "1.24"});
  dispatchr.call("cmd1", {"16", "0.125"});
  dispatchr.call("cmd1_1", {"2.22"});
  dispatchr.call("cmd2", {"something"});
  dispatchr.call("cmd2", {"nothing"});
  // an argument is not convertible to the right type
  try {
    dispatchr.call("cmd1", {"x", "123"});
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }
  try {
    dispatchr.call("cmd1", {"12", "x"});
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }

  // register class member functions

  WithFunction w1 = {3.14f};
  WithFunction w2 = {2.31f};
  dispatchr.add("w1_func", &WithFunction::func, w1);
  dispatchr.add("w2_func", &WithFunction::func, w2);
  dispatchr.add("w_static_func", &WithFunction::other);
  dispatchr.call("w1_func", {});
  dispatchr.call("w2_func", {});
  dispatchr.call("w_static_func", {"27"});

  // functions that implement the native dispatcher interface

  // register the same function with two different number of arguments
  dispatchr.add("do3_1", func3, 1);
  dispatchr.add("do3_3", func3, 3);
  // fails since the name was already registered
  try {
    dispatchr.add("do3_3", func3, 2);
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }

  dispatchr.call("do3_1", {"single arg"});
  dispatchr.call("do3_1", {"another single arg"});
  dispatchr.call("do3_3", {"three args", "blub", "blaI"});
  // fails because the function does not exists
  try {
    dispatchr.call("missing", {});
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }
  // fails because we have the wrong number of argumetns
  try {
    dispatchr.call("do3_1", {"one", "too many"});
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }

  std::cout << "registered commands:\n";
  for (const auto cmd : dispatchr.commands()) {
    std::cout << "  " << cmd.first << "(" << cmd.second << ")\n";
  }

  return EXIT_SUCCESS;
}
