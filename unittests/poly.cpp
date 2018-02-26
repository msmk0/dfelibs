/// \file
/// \brief Unit tests for dfe::poly

#include <array>
#include <vector>

#include <boost/test/unit_test.hpp>

#include <dfe_poly.hpp>

// test different input container types

#define COEFFS \
  { \
    1.0, 2.0, 0.25, 0.025 \
  }
constexpr double X0 = 0.5;
constexpr double Y0 = 2.065625;

BOOST_AUTO_TEST_CASE(poly_array)
{
  double coeffs[] = COEFFS;

  BOOST_TEST(dfe::polynomial_eval(X0, coeffs) == Y0);
}

BOOST_AUTO_TEST_CASE(poly_stdarray)
{
  BOOST_TEST(dfe::polynomial_eval(X0, std::array<double, 4>(COEFFS)) == Y0);
}

BOOST_AUTO_TEST_CASE(poly_stdvector)
{
  BOOST_TEST(dfe::polynomial_eval(X0, std::vector<double>(COEFFS)) == Y0);
}

// test different fixed polynomial orders

BOOST_AUTO_TEST_CASE(poly_const)
{
  BOOST_TEST(dfe::polynomial_eval_fixed(-1.0, 42) == 42.0);
  BOOST_TEST(dfe::polynomial_eval_fixed(+0.0, 42) == 42.0);
  BOOST_TEST(dfe::polynomial_eval_fixed(+1.0, 42) == 42.0);
}

BOOST_AUTO_TEST_CASE(poly_linear)
{
  BOOST_TEST(dfe::polynomial_eval_fixed(-0.5, 42, 1.0) == 41.5);
  BOOST_TEST(dfe::polynomial_eval_fixed(+0.0, 42, 1.0) == 42.0);
  BOOST_TEST(dfe::polynomial_eval_fixed(+0.5, 42, 1.0) == 42.5);
}

BOOST_AUTO_TEST_CASE(poly_quadratic)
{
  BOOST_TEST(dfe::polynomial_eval_fixed(-0.5, 42, 1.0, 0.5) == 41.625);
  BOOST_TEST(dfe::polynomial_eval_fixed(+0.0, 42, 1.0, 0.5) == 42.0);
  BOOST_TEST(dfe::polynomial_eval_fixed(+0.5, 42, 1.0, 0.5) == 42.625);
}

BOOST_AUTO_TEST_CASE(poly_cubic)
{
  BOOST_TEST(dfe::polynomial_eval_fixed(-0.5, 42, 1.0, 0.5, -1) == 41.75);
  BOOST_TEST(dfe::polynomial_eval_fixed(+0.0, 42, 1.0, 0.5, -1) == 42.0);
  BOOST_TEST(dfe::polynomial_eval_fixed(+0.5, 42, 1.0, 0.5, -1) == 42.5);
}
