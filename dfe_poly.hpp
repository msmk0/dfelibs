// "THE BEER-WARE LICENSE" (Revision 42):
// <msmk@cern.ch> wrote this file.  As long as you retain this notice you
// can do whatever you want with this stuff. If we meet some day, and you think
// this stuff is worth it, you can buy me a beer in return.   Moritz Kiehn

///
/// \file
/// \brief   Efficient evaluation of polynomial functions
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2018-02-26
///

#include <array>
#include <iterator>
#include <type_traits>

#pragma once

namespace dfe {

/// Evaluate a n-th order polynomial.
///
/// \param x      Where to evaluate the polynomial.
/// \param coeffs ReversibleContainer with n+1 coefficients.
///
/// The coefficients must be given in increasing order. A second order
/// polynomial with coefficients c0, c1, c2 defines the function
///
///     f(x) = c0 + c1*x + c2*x^2
///
template<typename T, typename Container>
inline T
polynomial_eval(T x, const Container& coeffs)
{
  // Use Horner's method to evaluate polynomial
  // the rbegin/rend variants for reverse iteration are only available with
  // c++14. on c++11 we have to use the regular variants and reverse manually.
  // using begin/end allows us to support regular arrays and std containers.
  T value = T(0);
  for (auto c = std::end(coeffs); c != std::begin(coeffs);) {
    c = std::prev(c);
    value *= x;
    value += *c;
  }
  return value;
}

/// Evaluate a fixed-order polynomial.
///
/// \param x      Where to evaluate the polynomial.
/// \param coeffs Coefficients in increasing order, i.e. c0, c1, c2, ... .
template<typename T, typename... Coefficients>
inline T
polynomial_eval_fixed(T x, Coefficients&&... coeffs)
{
  static_assert(
    0 < sizeof...(Coefficients), "Need at at least one polynomial coefficient");
  using Common = typename std::common_type<Coefficients...>::type;
  using Array = std::array<Common, sizeof...(Coefficients)>;
  return polynomial_eval(x, Array{coeffs...});
}

} // namespace dfe
