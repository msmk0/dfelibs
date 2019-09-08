/// \file
/// \brief Unit tests for namedtuple readers and writers

#include <boost/test/unit_test.hpp>

#include <dfe/dfe_namedtuple.hpp>
#include "record.hpp"

BOOST_TEST_DONT_PRINT_LOG_VALUE(Record::Tuple)

BOOST_AUTO_TEST_CASE(namedtuple_names)
{
  auto example = make_record(123);
  BOOST_TEST(example.names().size() == 7);
  BOOST_TEST(example.names().at(0) == "x");
  BOOST_TEST(example.names().at(1) == "y");
  BOOST_TEST(example.names().at(2) == "z");
  BOOST_TEST(example.names().at(3) == "a");
  BOOST_TEST(example.names().at(4) == "b");
  BOOST_TEST(example.names().at(5) == "c");
  BOOST_TEST(example.names().at(6) == "d");
}

BOOST_AUTO_TEST_CASE(nametuple_assign)
{
  // check default values
  Record r;
  BOOST_TEST(r.x == 0);
  BOOST_TEST(r.y == 0);
  BOOST_TEST(r.z == 0);
  BOOST_TEST(r.a == 0);
  BOOST_TEST(r.b == 0.0f);
  BOOST_TEST(r.c == 0.0);
  BOOST_TEST(not r.d);
  // assign namedtuple from a regular tuple
  r = std::make_tuple(-1, 1, 2, -3, 1.23f, 6.54, true);
  BOOST_TEST(r.x == -1);
  BOOST_TEST(r.y == 1);
  BOOST_TEST(r.z == 2);
  BOOST_TEST(r.a == -3);
  BOOST_TEST(r.b == 1.23f);
  BOOST_TEST(r.c == 6.54);
  BOOST_TEST(r.d);
}
