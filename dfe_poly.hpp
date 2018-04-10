// Copyright 2018 Moritz Kiehn
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

/// \file
/// \brief   Efficient evaluation of polynomial functions
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2018-02-26

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
polynomial_eval(const T& x, const Container& coeffs)
{
  // Use Horner's method to evaluate polynomial
  // the rbegin/rend variants for reverse iteration are only available with
  // c++14. on c++11 we have to use the regular variants and reverse manually.
  // using begin/end allows us to support regular arrays and std containers.
  T value = x; // make sure variable-sized types, e.g. std::valarray, work
  value = 0;
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
polynomial_eval_fixed(const T& x, Coefficients&&... coeffs)
{
  static_assert(
    0 < sizeof...(Coefficients), "Need at at least one polynomial coefficient");
  using Common = typename std::common_type<Coefficients...>::type;
  using Array = std::array<Common, sizeof...(Coefficients)>;
  return polynomial_eval(x, Array{static_cast<Common>(coeffs)...});
}

} // namespace dfe
