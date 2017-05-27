
# Makefile
# Created on May 7, 2017
# Author: yayundai & zejianli

LINK_TARGET = test_join test_mpi_triangle test_MPI_multi_Join test_Hyper
OBJS = test_join.o Graph.o test_mpi_triangle.o test_mpi_join.o test_MPI_multi_Join.o test_Hyper.o

REBUILDABLES = $(OBJS) $(LINK_TARGET)

all: $(LINK_TARGET)

test_join: test_join.o Graph.o
	g++ -o $@ $^

test_mpi_triangle: test_mpi_triangle.o mpi_join.o Graph.o
	mpic++ -o $@ $^

test_MPI_multi_Join: test_MPI_multi_Join.o mpi_join.o Graph.o
	mpic++ -o $@ $^

test_Hyper: test_Hyper.o mpi_join.o Graph.o
	mpic++ -o $@ $^

%.o: %.cpp
	mpic++ -o $@ -c $<

test_join.cpp: Graph.hpp Graph.cpp
test_mpi_join.cpp: Graph.hpp Graph.cpp mpi_join.cpp
test_mpi_triangle.cpp: Graph.hpp Graph.cpp mpi_join.cpp
test_MPI_multi_Join.cpp: Graph.hpp Graph.cpp mpi_join.cpp
test_Hyper.cpp: Graph.hpp Graph.cpp mpi_join.cpp
clean:
	rm -f $(REBUILDABLES)
