#include <string>
#include <vector>
#include <iostream>
#include "Graph.hpp"

using namespace std;

int main(void){

	/*
	 *creat a new graph from a data file
	*/ 	
	const string fileName="facebook";//the name of the file to be read
	const string path = "../"+fileName+".dat";
	const string pathOut="../"+fileName+"_sorted.dat";
	Graph g(path);

	cerr<<"Size: "<<g.getSize()<<endl;
	cerr<<"Arity: "<<g.getArity()<<endl;
	
	/*
	 *order the relation with a given permutation
	*/ 
	vector<unsigned int> p={1,2};
	g.order(p);
	g.saveTo(pathOut);

	cerr<<"Written to file "+fileName+"_sorted.dat"<<endl;
	
}
