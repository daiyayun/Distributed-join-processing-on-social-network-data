
# Makefile
# Created on May 7, 2017
# Author: yayundai & zejianli

LINK_TARGET = test_join test_mpi_triangle test_mpi_join
OBJS = test_join.o Graph.o

REBUILDABLES = $(OBJS) $(LINK_TARGET)

all: $(LINK_TARGET)

test_join: test_join.o Graph.o
	g++ -o $@ $^

test_mpi_triangle: test_mpi_triangle.o mpi_join.o Graph.o
	mpic++ -o $@ $^

test_mpi_join: test_mpi_join.o mpi_join.o Graph.o
	mpic++ -o $@ $^

%.o: %.cpp
	mpic++ -o $@ -c $<

test_join.cpp: Graph.hpp Graph.cpp
test_mpi_join.cpp: Graph.hpp Graph.cpp mpi_join.cpp
test_mpi_triangle.cpp: Graph.hpp Graph.cpp mpi_join.cpp
clean:
	rm -f $(REBUILDABLES)
