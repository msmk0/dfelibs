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
/// \brief   N-dimensional template-based histograms
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2018-05-22

#pragma once

#include <array>
#include <functional>
#include <numeric>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace dfe {
namespace histogram_impl {

/// A simple n-dimensional array.
///
/// Data can only be accessed through n-dimensional indices. The internal
/// storage format is considered an implementation detail. The size along
/// each dimension is set at run-time.
template<typename T, std::size_t NDIMENSIONS>
class NArray {
public:
  using Index = std::array<std::size_t, NDIMENSIONS>;

  /// Construct zero-initialized NArray with given size along each dimension.
  constexpr NArray(Index size);
  NArray(const NArray&) = default;
  NArray(NArray&&) = default;
  NArray& operator=(const NArray&) = default;
  NArray& operator=(NArray&&) = default;
  ~NArray() = default;

  constexpr const Index& size() const { return m_size; }
  /// Read-only access element without boundary check.
  constexpr const T& operator[](Index idx) const { return m_data[linear(idx)]; }
  /// Access element without boundary check.
  constexpr T& operator[](Index idx) { return m_data[linear(idx)]; }
  /// Read-only access element with boundary check.
  const T& at(Index idx) const;
  /// Access element with boundary check.
  T& at(Index idx);

private:
  constexpr std::size_t linear(Index idx) const;
  constexpr bool within_bounds(Index idx) const;

  std::vector<T> m_data;
  Index m_size;
};

} // namespace histogram_impl

// implementations

template<typename T, std::size_t NDIMENSIONS>
constexpr histogram_impl::NArray<T, NDIMENSIONS>::NArray(Index size)
  : m_data(
      std::accumulate(
        size.begin(), size.end(), static_cast<std::size_t>(1),
        std::multiplies<std::size_t>()),
      static_cast<T>(0))
  , m_size{size}
{
}

// construct linear column-major index from n-dimensional index
template<typename T, std::size_t NDIMENSIONS>
constexpr std::size_t
histogram_impl::NArray<T, NDIMENSIONS>::linear(Index idx) const
{
  std::size_t result = 0;
  std::size_t step = 1;
  for (std::size_t i = 0; i < NDIMENSIONS; ++i) {
    result += step * idx[i];
    step *= m_size[i];
  }
  return result;
}

template<typename T, std::size_t NDIMENSIONS>
constexpr bool
histogram_impl::NArray<T, NDIMENSIONS>::within_bounds(Index idx) const
{
  for (std::size_t i = 0; i < NDIMENSIONS; ++i) {
    if (m_size[i] <= idx[i]) return false;
  }
  return true;
}

template<typename T, std::size_t NDIMENSIONS>
inline const T&
histogram_impl::NArray<T, NDIMENSIONS>::at(Index idx) const
{
  if (!within_bounds(idx))
    throw std::out_of_range("NArray index is out of valid range");
  return m_data[linear(idx)];
}

template<typename T, std::size_t NDIMENSIONS>
inline T&
histogram_impl::NArray<T, NDIMENSIONS>::at(Index idx)
{
  if (!within_bounds(idx))
    throw std::out_of_range("NArray index is out of valid range");
  return m_data[linear(idx)];
}

} // namespace dfe
