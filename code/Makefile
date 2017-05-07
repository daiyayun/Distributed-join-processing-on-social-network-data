
# Makefile
# Created on May 7, 2017
# Author: yayundai & zejianli

LINK_TARGET = test_order test_join test_multi_join

OBJS = test_order.o test_join.o test_multi_join.o Graph.o

REBUILDABLES = $(OBJS) $(LINK_TARGET)

all: $(LINK_TARGET)

test_order: test_order.o Graph.o
	g++ -std=c++11 -o $@ $^

test_join: test_join.o Graph.o
	g++ -std=c++11 -o $@ $^

test_multi_join: test_multi_join.o Graph.o
	g++ -std=c++11 -o $@ $^	

%.o: %.cpp
	g++ -std=c++11 -o $@ -c $<

test_order.cpp: Graph.hpp Graph.cpp
test_join.cpp: Graph.hpp Graph.cpp
test_multi_join.cpp: Graph.hpp Graph.cpp	

clean:
	rm -f $(REBUILDABLES)
