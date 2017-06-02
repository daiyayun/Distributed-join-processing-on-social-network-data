#include <fstream>
#include <iostream>
#include <sstream>
#include<string>
#include<vector>
#include "Graph.hpp"

using namespace std;

int main(int argc, char **argv){
	//creat a new graph from a data file
	string fileName;
	if(argc==2){
		fileName=argv[1]; 
	} else{
		//the names of the default files to be read
		fileName="test";
	}	
	const string path = "../"+fileName+".dat";
	const string pathOut="../"+fileName+"_mod.dat";
	Graph g(path);
	ofstream myfile(pathOut.c_str());
	if(myfile.is_open()){
		for(int i=0;i<g.size;i++){
			for(int j=0;j<g.arity;j++){
				myfile<<5*g.at(i,j);
				if(j<g.arity-1)
					myfile<<" ";
			}
			myfile << endl;
		}
		myfile.close();
	}

	else cerr<<"Unable to open file";


	return 0;
}
