#include <string>
#include <vector>
#include <iostream>
#include "Graph.hpp"

using namespace std;

int main(void){

	/*
	 *creat a new graph from a data file
	*/ 	
	const string fileName="test";//the name of the file to be read
	const string path = "../"+fileName+".dat";
	const string pathSorted="../"+fileName+"_sorted.dat";
	const string pathJoined="../"+fileName+"_joined.dat";
	Graph g(path);

	cerr<<"Size: "<<g.getSize()<<endl;
	cerr<<"Arity: "<<g.getArity()<<endl;
	
	/*
	 *order the relation with a given permutation
	*/ 

	cerr<<"testing sort..."<<endl;
	vector<unsigned int> p={1,2};
	g.order(p);
	g.saveTo(pathSorted);

	cerr<<"Written to file "+fileName+"_sorted.dat"<<endl;

	cerr<<"testing join..."<<endl;
	vector<string> var1={"x1","x2"};
	vector<string> var2={"x2","x3"};
	Graph gJoined=Graph::join(g,var1,g,var2);
	gJoined.saveTo(pathJoined);
	cerr<<"Written tofile "+fileName+"_joined.dat"<<endl;
	return 0;

	
}
