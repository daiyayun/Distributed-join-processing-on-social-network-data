/*
 * test_sort.cpp
 *
 *  Created on: May 26, 2017
 *		Author: yayundai & zejianli
*/

#include<iostream>
#include<string>
#include "Graph.hpp"

using namespace std;

int main(int argc, char **argv){
	string fileName;
	if(argc==2){
		fileName=argv[1]; 
	} else{
		//the names of the default files to be read
		fileName="test";
	}	
    const string path = "../"+fileName+".dat";
    const string pathOut="../"+fileName+"_sorted.dat";
    cerr<<"reading "<<path<<endl;
    Graph g(path);
	cerr<<"Testing sort..."<<endl;
	vector<int> p;
    p.push_back(2);
    p.push_back(1);
	g.order(p);
	cerr<<"Sorted."<<endl;
    g.saveTo(pathOut);
    cerr<<"written to "<<pathOut<<endl;
   // g.clean();
    return 0;
}