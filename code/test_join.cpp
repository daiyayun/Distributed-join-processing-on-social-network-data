/*
 * test_join.cpp
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
	string fileName1, fileName2;
	if(argc==3){
		fileName1=argv[1]; 
		fileName2=argv[2];
	} else{
		fileName1="test";//the name of the default file to be read
		fileName2="test1";
	}	
	const string path1 = "../"+fileName1+".dat";
	const string path2 = "../"+fileName2+".dat";
	const string pathJoined="../"+fileName1+"_" +fileName2+"_joined.dat";
	
	Graph g1(path1);
	Graph g2(path2);

	//test the join function
	cerr<<"Testing join..."<<endl;
	cerr<<"joining "<< fileName1 << " and "<< fileName2 << endl;
	vector<string> var1;
	var1.push_back("x1");var1.push_back("x2");
	vector<string> var2;
	var2.push_back("x2");var2.push_back("x3");

	Graph::joinTo(&g1,var1,&g2,var2,pathJoined);
	cerr<<"Written to "+pathJoined<<endl;
	// Graph* gJoined=Graph::join(&g1,var1,&g2,var2);
	// cerr<<"Join done."<<endl;
	// cerr<<"joined size: "<<gJoined->size;
	// cerr<<" joined arity: "<<gJoined->arity<<endl;;
	// if(!gJoined->isEmpty()){
	// 	gJoined->saveTo(pathJoined);
	// 	cerr<<"Written to "+pathJoined<<endl;		
	// } else {
	// 	cerr<<"Empty graph, nothing to save !"<<endl;
	// }
	// delete gJoined;

	return 0;

	
}
