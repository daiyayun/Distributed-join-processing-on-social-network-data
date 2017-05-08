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

	//creat another graph from another data file
	const string fileName1="test1";
	const string path1 = "../"+fileName1+".dat";
	const string pathJoined1 = "../"+fileName+"_"+fileName1+"_joined.dat";
	cerr<<"Reading "<<fileName1+".dat"<<"..."<<endl;
	Graph g1(path1);

	//creat another graph from another data file
	const string fileName2="test2";
	const string path2 = "../"+fileName2+".dat";
	const string pathJoined2 = "../"+fileName+"_"+fileName1+"_"+fileName2+"_joined.dat";
	cerr<<"Reading "<<fileName2+".dat"<<"..."<<endl;
	Graph g2(path2);

	//test the join function
	cerr<<"Testing join..."<<endl;
	vector<string> var1={"x1","x2"};
	vector<string> var2={"x2","x3"};
	vector<string> var3={"x1","x2","x3"};
	vector<string> var4={"x1","x3"};
	Graph gJoined=Graph::join(g,var1,g1,var2);
	Graph gJoined1=Graph::join(gJoined,var3,g2,var4);
	cerr<<"Join done."<<endl;
	if(!gJoined.isEmpty()){
		gJoined.saveTo(pathJoined1);
		cerr<<"Written to "+pathJoined1<<endl;		
	} else {
		cerr<<"Empty graph, nothing to save !"<<endl;
	}

	if(!gJoined1.isEmpty()){
		gJoined1.saveTo(pathJoined2);
		cerr<<"Written to "+pathJoined2<<endl;		
	} else {
		cerr<<"Empty graph, nothing to save !"<<endl;
	}

	return 0;

	
}
