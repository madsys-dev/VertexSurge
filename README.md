# Vertex Surge: Variable Length Graph Pattern Match on Billion-edge Graphs

VertexSurge is a highly optimized graph process system specially designed for Variable Length Graph Pattern Match(VLGPM).


## Build
This project depends on the following libraries:
- fmtlib 9.1.0
- spdlog 1.11.0
- xxHash 0.8.0
- googletest
- intel tbb
Other dependencies are included in the project. Check CMakeList.txt to see the details.

Clone this project and run these commands to build the project:
```shell
mkdir build
cd build
cmake ..
make -j && make install
```

## Generate Graph
To generate a graph from LDBC Social Network (Generated by its handoop generator), you can use the following command:
```shell
./bin/importer -s $SNB_PATH  -d $OUTPUT_PATH -b 1000 -u  -t sn -i ldbc-ic -o vlgpm -h
```

To generate a graph from LDBC Social Network (Generated by its spard generator), you can use the following command:
```shell
./bin/importer -s $SNB_PATH  -d $OUTPUT_PATH -b 1000 -u  -t sn -i ldbc-bi -o vlgpm -h
```

To inspect the importer options, you can use this.
```shell
./bin/importer
```

Also you can check *scripts/gen_vlgpm_graph.py* to see how to generate a graph from other sources.

## Run
To run the test on the generated graph, you can use the following command:
```shell
./bin/e1-self-ring
```
All the tests are in *src/objects/testify*, checkout to see the details. 

