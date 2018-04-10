Dr. Fred Edison's incredible useful C++11 libraries
===================================================
May contain traces of industrial strength snake oil

This is a set of small single-header libraries. They require no installation
and only need a C++11 compatible compiler. To use any of them just copy the
header file into your own project and include it where needed.
If you are using the [CMake][cmake] build system you can also add the full
project as a subdirectory and use any of the libraries by linking with
the `dfelibs` target, i.e.

    add_subdirectory(<path/to/dfelibs/folder>)
    ...
    target_link_library(<your/project/target> dfelibs)

All libraries are licensed under the terms of the [MIT license][mit_license].

Dispatcher
----------

Register arbitrary commands with the dispatcher

```cpp
#include "dfe_dispatcher.hpp"

void func1(int x, double y, std::string z) { ... }
void func2(float a, unsigned it b) { ... }

dfe::Dispatcher dispatchr;
dispatchr.add("a_function", func1);
dispatchr.add("another_function", func2);
```

and call them by name with a list of string arguments:

```cpp
dispatchr.call("a_function", {"12", "0.23", "a message"});
dispatchr.call("another_function", {"3.14", "23"});
```

The string arguments are automatically converted to the correct type.

Namedtuple
----------

Add some self-awareness to a POD type

```cpp
#include "dfe_namedtuple.hpp"

struct Record {
  uint32_t x;
  float b;
  int16_t z;
  DFE_NAMEDTUPLE(Record, x, b, z);
}
```

and write it to disk in multiple formats:

```cpp
dfe::CsvNamedtupleWriter csv("records.csv"); // or
dfe::TsvNamedtupleWriter tsv("records.tsv"); // or
dfe::NpyNamedtupleWriter npy("records.npy");

csv.append(Record{1, 1.4, -2}); // same call for other writers
```

The results are either comma-separated text values

    x,b,z
    1,1.4,-2
    ...

or tab-separated text values

    x       b       z
    1       1.4     -2
    ...

or binary [NPY][npy] data.

Poly
----

Evaluate polynomial functions using either a (variable-sized) container
of coefficients

```cpp
#include "dfe_poly.h"

std:vector<double> coeffs = {0.0, 2.0, 3.0, 4.0};
// evaluate f(x) = 2*x + 3*x^2 + 4*x^3 at x=-1.0
double y = dfe::polynomial_eval(-1.0, coeffs);
```

or using the coefficients directly for fixed order polynomials:

```cpp
// evaluate f(x) = 0.25 + x + 0.75*x^2 at x=0.5
float y = dfe::polynomial_eval_fixed(0.5f, 0.25f, 1.0f, 0.75f);
```


[cmake]: https://www.cmake.org
[mit_license]: https://opensource.org/licenses/MIT
[npy]: https://docs.scipy.org/doc/numpy/neps/npy-format.html
