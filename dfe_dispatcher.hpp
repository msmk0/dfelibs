// "THE BEER-WARE LICENSE" (Revision 42):
// <msmk@cern.ch> wrote this file.  As long as you retain this notice you
// can do whatever you want with this stuff. If we meet some day, and you think
// this stuff is worth it, you can buy me a beer in return.   Moritz Kiehn

///
/// \file
/// \brief   command dispatcher that takes function names and string arguments
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2018-02-20
///

#pragma once

#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace dfe {

/// A simple command dispatcher.
///
/// You can register commands and call the by name using all-string arguments.
class Dispatcher {
public:
  using NativeInterface = std::function<void(const std::vector<std::string>&)>;

  /// Register a commands that implements the native dispatcher call interface
  void add(std::string name, NativeInterface func, std::size_t nargs);
  /// Call the command with some arguments
  void call(const std::string& name, const std::vector<std::string>& args);

private:
  struct Command {
    NativeInterface func;
    std::size_t nargs;
  };
  std::unordered_map<std::string, Command> m_commands;
};

// implementations

void
Dispatcher::add(
  std::string name, Dispatcher::NativeInterface func, std::size_t nargs)
{
  if (m_commands.count(name)) {
    throw std::invalid_argument(
      "try to register existing command '" + name + "'");
  }
  m_commands[std::move(name)] = Command{func, nargs};
}

void
Dispatcher::call(const std::string& name, const std::vector<std::string>& args)
{
  auto cmd = m_commands.find(name);
  if (cmd == m_commands.end()) {
    throw std::invalid_argument("unknown command '" + name + "'");
  }
  if (args.size() != cmd->second.nargs) {
    std::string msg;
    throw std::invalid_argument(
      "command '" + name + "' expects " + std::to_string(cmd->second.nargs) +
      " arguments but " + std::to_string(args.size()) + " given");
  }
  cmd->second.func(args);
}

} // namespace dfe
