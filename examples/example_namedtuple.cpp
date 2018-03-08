#include <cstdlib>
#include <iostream>

#include <dfe_namedtuple.hpp>

struct Data {
  uint32_t dac0;
  uint32_t temperature;
  uint32_t timestamp;
  float humidity;

  DFE_NAMEDTUPLE(Data, dac0, temperature, timestamp, humidity);
};

int
main(int argc, char* argv[])
{
  dfe::CsvNamedtupleWriter<Data> csv("test.csv");
  dfe::NpyNamedtupleWriter<Data> npy("test.npy");
  dfe::TabularNamedtupleWriter<Data> tab("test.txt");

  Data x;
  x.dac0 = 0;
  x.temperature = 18289;
  x.timestamp = 1012;
  x.humidity = 0.012;

  for (const auto& name : x.names()) {
    std::cout << name << std::endl;
  }

  for (uint32_t i = 0; i < 1024; ++i) {
    x.dac0 = i;

    std::cout << x << std::endl;
    csv.append(x);
    npy.append(x);
    tab.append(x);
  }

  return EXIT_SUCCESS;
}
