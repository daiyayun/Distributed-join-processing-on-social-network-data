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
		fileName1="test";//the name of the default file to be read
		fileName2="test1";
	}	
	const string path1 = "../"+fileName1+".dat";
	const string path2 = "../"+fileName2+".dat";
	const string pathJoined="../"+fileName1+"_" +fileName2+"_joined.dat";
	Graph r1(path1);
	Graph r2(path2);
	vector<string> var1={"x1","x2"};
	vector<string> var2={"x2","x3"};
	vector<vector<int> > x = findcommon(v1, v2);
	int numtasks, taskid;
	int ilocal;
	Graph glocal,gJoined;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM__WORLD, &taskid);
	MPI_Comm_size(MPI_COMM__WORLD, &numtasks);

	vector<vector<vector int> > > buf[numtasks];
	vector<vector<int> >::iterator it1;
	vector<vector<int> >::iterator it2;
	for(it1=r1.begin(); it1!=r1.end(); it1++){
		buf[(*it1)[x[0][0]]%numtasks][0].push_back(*it1);
	}
	for(it2=r2.begin(); it2!=r2.end(); it2++){
		buf[(*it2)[x[1][0]]%numtasks][1].push_back(*it2);
	}
	int msg[numtasks];
	for(int i=0; i<numtasks; i++) msg[i] = i;
	MPI_Scatter(msg,1,MPI_INT,&ilocal,1,MPI_INT,0,MPI_COMM__WORLD);
	glocal = Graph::join(Graph(buf[ilocal][0]),var1,Graph(buf[ilocal][1]),var2);


	MPI_Finalize();
}