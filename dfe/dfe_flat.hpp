// Copyright 2019 Moritz Kiehn
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
/// \brief   Minimal associative containers based on continous storage
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2019-02-27, Initial version

#pragma once

#include <algorithm>
#include <functional>
#include <stdexcept>
#include <vector>

namespace dfe {

/// An set of unique elements stored in a continous container.
///
/// Supports membership check, ensures uniqueness of elements, and allows
/// iteration over elements. By using a continous container, memory allocation
/// is greatly simplified and lookups benefit from higher memory locality
/// at the expense of slower insertion of elements. Should work best for
/// smaller sets with frequent lookups.
///
/// The set elements can not be modified on purpose. With a non-standard
/// `Compare` function modifying a contained object might change its identity
/// and thus its position in the set. This would break the internal sorting.
template<typename T, typename Compare = std::less<T>>
class FlatSet {
public:
  using const_iterator = typename std::vector<T>::const_iterator;

  /// Access the equivalent element or throw if it does not exists.
  template<typename U>
  const T& at(U&& u) const;

  const_iterator begin() const { return m_items.begin(); }
  const_iterator end() const { return m_items.end(); }

  /// Return true if there are no elements in the set.
  bool empty() const { return m_items.empty(); }
  /// Return the number of elements in the set.
  std::size_t size() const { return m_items.size(); }

  /// Remove all elements from the container.
  void clear() { m_items.clear(); }
  /// Add the element to the set or replace the existing equivalent element.
  ///
  /// Depending on the `Compare` function we might have elements with different
  /// values but the same identity with respect to the chosen `Compare`
  /// function. Only one can be kept and this function replaces the existing
  /// element with the new one in such a case.
  void insert_or_assign(const T& t);

  /// Return an interator to the equivalent element or `.end()` if not found.
  template<typename U>
  const_iterator find(U&& u) const;
  /// Return true if the equivalent element is in the set.
  template<typename U>
  bool contains(U&& u) const;

private:
  std::vector<T> m_items;
};

/// A key-value map that stores keys and values in continous containers.
///
/// Supports membership check, access to values by key, addition and replacement
/// of values for a given key. Keys and values are stored in separate
/// continous containers to simplify allocation and benefit from greater
/// memory locality.
template<typename Key, typename T, typename Compare = std::less<Key>>
class FlatMap {
public:
  /// Writable access to an element or throw if it does not exists.
  T& at(const Key& key) { return m_items[m_keys.at(key).index]; }
  /// Read-only access to an element or throw if it does not exists.
  const T& at(const Key& key) const { return m_items[m_keys.at(key).index]; }

  /// Return true if there are no elements in the map.
  bool empty() const { return m_keys.empty(); }
  /// Return the number of elements in the container.
  std::size_t size() const { return m_keys.size(); }

  /// Remove all elements from the container.
  void clear() { m_keys.clear(), m_items.clear(); }
  /// Add the element under the given key or replace an existing element.
  ///
  /// New elements are constructed or assigned in-place with the parameters
  /// forwarded to a `T(...)` constructor call.
  template<typename... Params>
  void emplace(const Key& key, Params&&... params);

  /// Return true if an element exists for the given key
  bool contains(const Key& key) const { return m_keys.contains(key); }

private:
  struct KeyIndex {
    Key key;
    std::size_t index;
  };
  struct KeyCompare {
    constexpr bool operator()(const KeyIndex& lhs, const KeyIndex& rhs) const
    {
      return Compare()(lhs.key, rhs.key);
    }
    constexpr bool operator()(const KeyIndex& lhs, const Key& rhs_key) const
    {
      return Compare()(lhs.key, rhs_key);
    }
    constexpr bool operator()(const Key& lhs_key, const KeyIndex& rhs) const
    {
      return Compare()(lhs_key, rhs.key);
    }
  };

  FlatSet<KeyIndex, KeyCompare> m_keys;
  std::vector<T> m_items;
};

// implementation FlatSet

template<typename T, typename Compare>
template<typename U>
inline const T&
FlatSet<T, Compare>::at(U&& u) const
{
  auto pos = find(std::forward<U>(u));
  if (pos == end()) {
    throw std::out_of_range("The requested element does not exists");
  }
  return *pos;
}

template<typename T, typename Compare>
inline void
FlatSet<T, Compare>::insert_or_assign(const T& t)
{
  auto pos = std::lower_bound(m_items.begin(), m_items.end(), t, Compare());
  if (((pos != m_items.end()) and !Compare()(t, *pos))) {
    *pos = t;
  } else {
    m_items.insert(pos, t);
  }
}

template<typename T, typename Compare>
template<typename U>
inline typename FlatSet<T, Compare>::const_iterator
FlatSet<T, Compare>::find(U&& u) const
{
  auto end = m_items.end();
  auto pos =
    std::lower_bound(m_items.begin(), end, std::forward<U>(u), Compare());
  return ((pos != end) and !Compare()(std::forward<U>(u), *pos)) ? pos : end;
}

template<typename T, typename Compare>
template<typename U>
inline bool
FlatSet<T, Compare>::contains(U&& u) const
{
  return std::binary_search(
    m_items.begin(), m_items.end(), std::forward<U>(u), Compare());
}

// implementation FlatMap

template<typename Key, typename T, typename Compare>
template<typename... Params>
inline void
FlatMap<Key, T, Compare>::emplace(const Key& key, Params&&... params)
{
  auto idx = m_keys.find(key);
  if (idx != m_keys.end()) {
    m_items[idx->index] = T(std::forward<Params>(params)...);
  } else {
    m_items.emplace_back(std::forward<Params>(params)...);
    m_keys.insert_or_assign(KeyIndex{key, m_items.size() - 1});
  }
}

} // namespace dfe
