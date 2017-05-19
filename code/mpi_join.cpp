#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>


#include "Graph.hpp"

#include "mpi.h"

Graph Graph::mpiJoin(Graph g1, vector<string> var1, Graph g2, vector<string> var2){
	int taskid,numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

	vector<vector<int> > r1=g1.relation;
	vector<vector<int> > r2=g2.relation;
	vector<vector<int> > x = findcommon(var1, var2);
	int arityJoined=unionSize(var1,var2);//arity of joined relation	

	//int ilocal;	
	Graph gJoinedLocal,gJoined;
	vector<int> unfoldedLocal;	//local join result unfolded in 1D
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
		gathered=fold(gatheredRaw,arityJoined);
		gJoined=Graph(gathered);
		return gJoined;
	}

}