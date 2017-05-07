/*
 * test_multi_join.cpp
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
	string fileName1,fileName2,fileName3;
	if(argc==4){
		fileName1=argv[1]; 
		fileName2=argv[2];
		fileName3=argv[3];
	} else{
		fileName1="test";//the name of the default file to be read
		fileName2="test1";
		fileName3="test2";
	}	
	const string path1 = "../"+fileName1+".dat";
	const string path2 = "../"+fileName2+".dat";
	const string path3 = "../"+fileName3+".dat";

	const string pathJoined1="../"+fileName1+"_"+fileName2+"_joined.dat";
	const string pathJoined2="../"+fileName1+"_"+fileName2+"_"+fileName3+"_joined.dat";
	
	Graph g1(path1);
	Graph g2(path2);
	Graph g3(path3);


	//test the join function
	cerr<<"Testing join..."<<endl;
	cerr<<"joining "<<fileName1<<", "<<fileName2 << " and "<<fileName3<<endl;
	vector<string> var1={"x1","x2"};
	vector<string> var2={"x2","x3"};
	vector<string> var3={"x1","x2","x3"};
	vector<string> var4={"x1","x3"};
	Graph gJoined=Graph::join(g1,var1,g2,var2);
	Graph gJoined1=Graph::join(gJoined,var3,g3,var4);
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
