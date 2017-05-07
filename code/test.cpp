/*
 * test.cpp
 *
 *  Created on: May 1, 2017
 *		Author: yayundai & zejianli
*/

#include <string>
#include <vector>
#include <iostream>
#include "Graph.hpp"

using namespace std;

int main(int argc, char **argv){

	//creat a new graph from a data file
	string fileName;
	if(argc==2){
		fileName=argv[1]; 
	} else{
		fileName="test";//the name of the default file to be read
	}	
	const string path = "../"+fileName+".dat";
	const string pathSorted="../"+fileName+"_sorted.dat";
	const string pathJoined="../"+fileName+"_joined.dat";
	cerr<<"Reading "<<fileName+".dat"<<"..."<<endl;
	Graph g(path);

	cerr<<"Size: "<<g.getSize()<<endl;
	cerr<<"Arity: "<<g.getArity()<<endl;
	
	//test the order function
	cerr<<"Testing sort..."<<endl;
	vector<unsigned int> p={1,2};
	g.order(p);
	cerr<<"Sorted."<<endl;
	g.saveTo(pathSorted);

	cerr<<"Written to "+fileName+"_sorted.dat"<<endl;

	//test the join function
	cerr<<"Testing join..."<<endl;
	vector<string> var1={"x1","x2"};
	vector<string> var2={"x2","x3"};
	Graph gJoined=Graph::join(g,var1,g,var2);
	cerr<<"Join done."<<endl;
	if(!gJoined.isEmpty()){
		gJoined.saveTo(pathJoined);
		cerr<<"Written tofile "+fileName+"_joined.dat"<<endl;		
	} else {
		cerr<<"Empty graph, nothing to save !"<<endl;
	}

	return 0;

	
}
