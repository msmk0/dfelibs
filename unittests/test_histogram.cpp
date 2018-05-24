/// \file
/// \brief Unit tests for dfe::Histogram

#include <boost/test/unit_test.hpp>
#include <iostream>

#include <dfe_histogram.hpp>

// storage tests

using dfe::histogram_impl::NArray;

BOOST_AUTO_TEST_CASE(histogram_narray2f_init)
{
  NArray<float, 2> s({10, 9});

  BOOST_TEST(s.size().size() == 2);
  BOOST_TEST(s.size()[0] == 10);
  BOOST_TEST(s.size()[1] == 9);

  for (size_t i = 0; i < 10; ++i) {
    for (size_t j = 0; j < 9; ++j) {
      auto x = s[{i, j}];
      BOOST_TEST(x == 0.0f);
    }
  }
}

BOOST_AUTO_TEST_CASE(histogram_narray2f_at)
{
  NArray<float, 2> s({10, 9});

  BOOST_CHECK_NO_THROW(s.at({0, 0}));
  BOOST_CHECK_NO_THROW(s.at({0, 8}));
  BOOST_CHECK_NO_THROW(s.at({9, 0}));
  BOOST_CHECK_NO_THROW(s.at({9, 8}));
  BOOST_CHECK_THROW(s.at({0, 9}), std::out_of_range);
  BOOST_CHECK_THROW(s.at({10, 0}), std::out_of_range);
  BOOST_CHECK_THROW(s.at({10, 9}), std::out_of_range);
}

BOOST_AUTO_TEST_CASE(histogram_narray3f_init)
{
  NArray<float, 3> s({10, 9, 8});

  BOOST_TEST(s.size().size() == 3);
  BOOST_TEST(s.size()[0] == 10);
  BOOST_TEST(s.size()[1] == 9);
  BOOST_TEST(s.size()[2] == 8);

  for (size_t i = 0; i < 10; ++i) {
    for (size_t j = 0; j < 9; ++j) {
      for (size_t k = 0; k < 8; ++k) {
        auto x = s[{i, j}];
        BOOST_TEST(x == 0.0f);
      }
    }
  }
}
