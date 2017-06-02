/*
 * test_mpi_triangle.cpp
 *
 *  Created on: May 7, 2017
 *		Author: yayundai & zejianli
*/
#include <ctime>
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

    clock_t t = clock();
	Graph* gJoined=Graph::MPIJoin(g,var1,g,var2,useHash);
	int firstJoinEmpty=1;
	int length;
	int* rJoined;
	if(taskid==0){
		cerr<<"First join done."<<endl;
		if(!gJoined->isEmpty()){
			length=gJoined->arity*gJoined->size;
			firstJoinEmpty=0;
			rJoined=gJoined->relation;
		}else{
			cerr<<"Empty graph, nothing to save !"<<endl;
		}
	}
	MPI_Bcast(&firstJoinEmpty,1,MPI_INT,0,MPI_COMM_WORLD);
	if(firstJoinEmpty){
		MPI_Finalize();
		return 0;
	}
	MPI_Bcast(&length,1,MPI_INT,0,MPI_COMM_WORLD);
	if(taskid!=0) rJoined=new int[length];
	MPI_Bcast(rJoined,length,MPI_INT,0,MPI_COMM_WORLD);
	if(taskid!=0) gJoined=new Graph(rJoined,var.size(),length/var.size());
	gJoined=Graph::MPIJoin(gJoined,var,g,var3,useHash);
    t = clock() - t;
	delete rJoined;
	delete g;
	if(taskid==0){
		cerr<<"join done."<<endl;
		cout <<endl
     	    <<"execution time: "
        	<<(t*1000)/CLOCKS_PER_SEC
         	<<"ms\n\n";
		if(!gJoined->isEmpty()){
			gJoined->saveTo(pathJoined);
			cerr<<"written to "<<pathJoined<<endl;
			delete gJoined;
		}else{
			cerr<<"Empty graph, nothing to save !"<<endl;
		}
	}

	MPI_Finalize();
	return 0;
}