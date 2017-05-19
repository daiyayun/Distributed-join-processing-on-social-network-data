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


	const string path1 = "../"+fileName1+".dat";
	const string path2 = "../"+fileName2+".dat";
	const string pathJoined="../"+fileName1+"_" +fileName2+"_dist_joined.dat";
	Graph g1(path1);
	Graph g2(path2);
	vector<vector<int> > r1=g1.relation;
	vector<vector<int> > r2=g2.relation;
	vector<string> var1={"x1","x2"};
	vector<string> var2={"x2","x3"};
	vector<vector<int> > x = findcommon(var1, var2);
	int arityJoined=unionSize(var1,var2);//arity of joined relation
	int numtasks, taskid;
	//int ilocal;
	Graph gJoinedLocal,gJoined;
	vector<int> unfoldedLocal;	//local join result unfolded in 1D
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	if(taskid==0){
		cerr<<"Testing join on "<<numtasks<<" processes."<<endl;
		cerr<<"joining "<< fileName1 << " and "<< fileName2 << endl;		
	}
	vector<vector<vector<int> > > buf[numtasks];
	int lengths[numtasks];//length of each local join result
	int displs[numtasks];// displacement of each local join result w.r.t. address of receive buffer
	vector<int> gatheredRaw; //gathered join result in 1D
	vector<vector<int> > gathered; //gathered join result reconstructed in 2D

	//assigning the task for each process
	for(int i=0;i<numtasks;i++){
		vector<vector<int> > tmp1,tmp2;
		buf[i].push_back(tmp1);
		buf[i].push_back(tmp2);

	}
	vector<vector<int> >::iterator it1;
	vector<vector<int> >::iterator it2;

	for(it1=r1.begin(); it1!=r1.end(); it1++){
		buf[((*it1)[x[0][0]])%numtasks][0].push_back(*it1);
	}

	for(it2=r2.begin(); it2!=r2.end(); it2++){
		buf[(*it2)[x[1][0]]%numtasks][1].push_back(*it2);
	}
	//int msg[numtasks];
	//for(int i=0; i<numtasks; i++) msg[i] = i;

	//MPI_Scatter(msg,1,MPI_INT,&ilocal,1,MPI_INT,0,MPI_COMM_WORLD);
	gJoinedLocal = Graph::join(Graph(buf[taskid][0]),var1,Graph(buf[taskid][1]),var2);
	int lengthLocal=gJoinedLocal.getSize()*gJoinedLocal.getArity();
	//gather length information from each process, prepare space for receiving 
	MPI_Gather(&lengthLocal,1,MPI_INT,lengths,1,MPI_INT,0,MPI_COMM_WORLD);
	if(taskid==0){
		int	length=0;
		for(int i=0;i<numtasks;i++){
			length+=lengths[i];
		}
		cerr<<"total length of data to receive : "<<length<<endl;
		cerr<<"Receiving data..."<<endl;
		gatheredRaw=vector<int>(length);
		for(int i=0;i<numtasks;i++){
			displs[i]=0;
			for(int j=0;j<i;j++){
				displs[i]+=lengths[j];
			}
		}

	}
	unfoldedLocal=unfold(gJoinedLocal.relation);
	//when there is nothing to send, send a null pointer.
	if(unfoldedLocal.size()==0){
		 MPI_Gatherv(NULL,0,MPI_INT,&gatheredRaw[0],lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	}else{
		MPI_Gatherv(&unfoldedLocal[0],lengthLocal,MPI_INT,&gatheredRaw[0],lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	}
	if(taskid==0){
		cerr<<"Join done."<<endl;
		gathered=fold(gatheredRaw,arityJoined);
		cerr<<"data folded."<<endl;
		gJoined=Graph(gathered);
		cerr<<"new Graph object created."<<endl;
		if(gathered.size()!=0){
			//Graph::saveRelation(gathered,pathJoined);
			gJoined.saveTo(pathJoined);
			cerr<<"Written to "+pathJoined<<endl;		
		}else{
			cerr<<"Empty graph, nothing to save !"<<endl;
		}
	}
	MPI_Finalize();
	return 0;
}