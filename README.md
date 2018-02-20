Dr. Fred Edison's incredible useful C++11 libraries
===================================================
May contain traces of industrial strength snake oil

This is a set of small single-header libraries. If not mentioned
otherwise they only need a C++11 compatible compiler. They require no
installation and can be used by copying them into your own projects and
including them directly.

Dispatcher
----------

Register arbitrary commands with the dispatcher

    #include "dfe_dispatcher.hpp"

    void func1(int x, double y, std::string z) { ... }
    void func2(float a, unsigned it b) { ... }

    dfe::Dispatcher dispatchr;
    dispatchr.add("a_function", func1);
    dispatchr.add("another_function", func2);

and call them by name with list of string arguments:

    dispatchr.call("a_function", {"12", "0.23", "a message"});
    dispatchr.call("another_function", {"3.14", "23"});

The string arguments are automatically converted to the correct type.

Namedtuple
----------

Add some self-awareness to a POD type

    #include "dfe_namedtuple.hpp"

    struct Record {
      uint32_t x;
      float b;
      int16_t z;
      DFE_NAMEDTUPLE(Record, x, b, z);
    }

and write it to disk in multiple formats:

    dfe::CsvNamedtupleWriter csv("records.csv"); // or
    dfe::NpyNamedtupleWriter npy("records.npy"); // or
    dfe::TabularNamedtupleWriter tab("records.txt");

    csv.append(Record{1, 1.4, -2}); // same call for other writers

The results are either comma-separated values

    x,b,z
    1,1.4,-2
    ...

or tabular text data

    x     b     z
    1     1.4   -2
    ...

or binary [npy](https://docs.scipy.org/doc/numpy/neps/npy-format.html) data.
