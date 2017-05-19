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
	string fileName;
	if(argc==2){
		fileName=argv[1]; 
	} else{
		//the names of the default files to be read
		fileName="test";
	}	


	const string path = "../"+fileName+".dat";
	const string pathInterm = "../"+fileName+"_interm.dat";
	const string pathJoined="../"+fileName+"_dist_triangles.dat";
	Graph g(path);
	vector<string> var1={"x1","x2"};
	vector<string> var2={"x2","x3"};
	vector<string> var3={"x1","x2","x3"};
	vector<string> var4={"x1","x3"};
	int arityJoined=unionSize(var1,var2);//arity of joined relation	


	MPI_Init(&argc, &argv);
	int numtasks, taskid;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

	if(taskid==0){
		cerr<<"Testing join..."<<endl;
		cerr<<"Finding triangles in "<< fileName << endl;		
	}

	Graph gJoined,gJoined1;
	gJoined=Graph::mpiJoin(g,var1,g,var2);
//	cerr<<"first result is empty: "<<gJoined.isEmpty()<<endl;
	vector<int> gJoinedUnfolded;
	int unfoldedLength;
	vector<vector<int> > gJoinedRefolded;
	int firstJoinEmpty=1;


	if(taskid==0){

		cerr<<"First join done."<<endl;
		if(!gJoined.isEmpty()){
			firstJoinEmpty=0;
			gJoinedUnfolded=unfold(gJoined.relation);
			unfoldedLength=gJoinedUnfolded.size();
			gJoined.saveTo(pathInterm);
			cerr<<"Written to "+pathInterm<<endl;		
		}else{
			cerr<<"Empty graph, nothing to save !"<<endl;
		}
	}
	MPI_Bcast(&firstJoinEmpty,1,MPI_INT,0,MPI_COMM_WORLD);
	if(firstJoinEmpty){
		MPI_Finalize();
		return 0;
	}
	MPI_Bcast(&unfoldedLength,1,MPI_INT,0,MPI_COMM_WORLD);

	if(taskid!=0){
		gJoinedUnfolded=vector<int>(unfoldedLength);
	}

	MPI_Bcast(&gJoinedUnfolded[0],unfoldedLength,MPI_INT,0,MPI_COMM_WORLD);

	gJoinedRefolded=fold(gJoinedUnfolded,arityJoined);
	gJoined.relation=gJoinedRefolded;
	gJoined1=Graph::mpiJoin(gJoined,var3,g,var4);


	if(taskid==0){

		cerr<<"Join done."<<endl;
		if(!gJoined1.isEmpty()){
			gJoined1.saveTo(pathJoined);
			cerr<<"Written to "+pathJoined<<endl;		
		}else{
			cerr<<"Empty graph, nothing to save !"<<endl;
		}
	}
	MPI_Finalize();
	return 0;
}