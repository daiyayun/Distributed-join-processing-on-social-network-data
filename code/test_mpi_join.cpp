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
	//creat a new graph from a data file
	string fileName1, fileName2;
	if(argc==3){
		fileName1=argv[1]; 
		fileName2=argv[2];
	} else{
		//the names of the default files to be read
		fileName1="test";
		fileName2="test1";
	}	
	MPI_Init(&argc, &argv);
	int numtasks, taskid;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

	const string path1 = "../"+fileName1+".dat";
	const string path2 = "../"+fileName2+".dat";
	const string pathJoined="../"+fileName1+"_" +fileName2+"_dist_joined.dat";
	if(taskid==0){
		cerr<<"Testing join with MPI..."<<endl;
		cerr<<"joining "<< fileName1 << " and "<< fileName2 << endl;
	}

	Graph g1(path1);
	Graph g2(path2);
	
	vector<string> var1;
	var1.push_back("x1");var1.push_back("x2");var1.push_back("x3");
	vector<string> var2;
	var2.push_back("x1");var2.push_back("x3");
	Graph* gJoined=Graph::MPIJoin(&g1,var1,&g2,var2,USE_HASH);

	if(taskid==0){
		cerr<<"Join done."<<endl;
		if(!gJoined->isEmpty()){
			//Graph::saveRelation(gathered,pathJoined);
			gJoined->saveTo(pathJoined);
			cerr<<"Written to "+pathJoined<<endl;	
			delete gJoined;	
		}else{
			cerr<<"Empty graph, nothing to save !"<<endl;
		}
	}
	MPI_Finalize();
	return 0;
}