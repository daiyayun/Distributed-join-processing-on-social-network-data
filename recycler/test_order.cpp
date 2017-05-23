/*
 * test_order.cpp
 *
 *  Created on: May 7, 2017
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
	//const string pathJoined="../"+fileName+"_joined.dat";
	cerr<<"Reading "<<fileName+".dat"<<"..."<<endl;
	Graph g(path);

	cerr<<"Size: "<<g.getSize()<<endl;
	cerr<<"Arity: "<<g.getArity()<<endl;
	
	//test the order function
	cerr<<"Testing sort..."<<endl;
	vector<int> p={1,2};
	g.order(p);
	cerr<<"Sorted."<<endl;
	g.saveTo(pathSorted);

	cerr<<"Written to "+fileName+"_sorted.dat"<<endl;

	
}
