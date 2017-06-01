/*
 * MPI_join.cpp
 *
 *  Created on: May 7, 2017
 *		Author: yayundai & zejianli
*/

#include <string>
#include <vector>
#include <iostream>
#include "Graph.hpp"
#include "mpi.h"

using namespace std;

int main(int argc, char **argv){
	bool useHash=false;
	//creat a new graph from a data file
	string fileName;
	if(argc==2){
		fileName=argv[1]; 
	} else{
		//the names of the default files to be read
		fileName="test";
	}
	if(argc==3){
		fileName=argv[1];
		useHash=(bool)atoi(argv[2]);
	}
	MPI_Init(&argc, &argv);
	int numtasks, taskid;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

	const string path = "../"+fileName+".dat";
	const string pathInterm="../"+fileName+"_join_interm.dat";
	const string pathJoined="../"+fileName+"_mpi_triangles.dat";
	if(taskid==0){
		cerr<<"Finding triangles in "<<fileName<<" with MPI..."<<endl;
		if(useHash){
			cerr<<"using hash."<<endl;
		}
	}

	Graph* g=new Graph(path);
	
	vector<string> var1;
	var1.push_back("x1");var1.push_back("x2");
	vector<string> var2;
	var2.push_back("x2");var2.push_back("x3");
	vector<string> var3;
	var3.push_back("x1");var3.push_back("x3");
	vector<string> var;//=joinedVar(var1,var2);
	var.push_back("x1");var.push_back("x2");var.push_back("x3");
	Graph* gJoined=Graph::MPIJoin(g,var1,g,var2,useHash);
	int firstJoinEmpty=1;

	if(taskid==0){
		cerr<<"First join done."<<endl;
		if(!gJoined->isEmpty()){
			gJoined->saveTo(pathInterm);
			delete gJoined;	
			firstJoinEmpty=0;
		}else{
			cerr<<"Empty graph, nothing to save !"<<endl;
		}
	}
	MPI_Bcast(&firstJoinEmpty,1,MPI_INT,0,MPI_COMM_WORLD);
	if(firstJoinEmpty){
		MPI_Finalize();
		return 0;
	}
	Graph* g1=new Graph(pathInterm);
	Graph* gResult=Graph::MPIJoin(g1,var,g,var3,useHash);
	delete g1;
	delete g;
	//if(taskid==1)gResult->print();
	if(taskid==0){
		cerr<<"join done."<<endl;
		if(!gResult->isEmpty()){
			gResult->saveTo(pathJoined);
			cerr<<"written to "<<pathJoined<<endl;
			delete gResult;
		}else{
			cerr<<"Empty graph, nothing to save !"<<endl;
		}
	}

	MPI_Finalize();
	return 0;
}