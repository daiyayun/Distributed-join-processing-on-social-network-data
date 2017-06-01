/*
 * mpi_join.cpp
 *
 *  Created on: May 17, 2017
 *		Author: yayundai & zejianli
*/

#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <math.h>

#include "Graph.hpp"

#include "mpi.h"

using namespace std;

Graph Graph::MPIJoin(Graph* g1, vector<string> var1, Graph* g2, vector<string> var2){
	int taskid,numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

	vector<vector<int> > r1 = g1->relation;
	vector<vector<int> > r2 = g2->relation;
	vector<vector<int> > x = findCommon(var1, var2);
	int arityJoined = unionSize(var1,var2);//arity of joined relation	

	//int ilocal;	
	Graph gJoinedLocal,gJoined;
	vector<int> unfoldedLocal;	//local join result unfolded in 1D
	vector<vector<vector<int> > >* buf=new vector<vector<vector<int> > >[numtasks];
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
	cerr<<"writing buffer"<<endl;
	for(it1=r1.begin(); it1!=r1.end(); it1++){
		buf[((*it1)[x[0][0]])%numtasks][0].push_back(*it1);
	}

	for(it2=r2.begin(); it2!=r2.end(); it2++){
		buf[(*it2)[x[1][0]]%numtasks][1].push_back(*it2);
	}
	cerr<<"buffer written. Creating local graphs"<<endl;
	//int msg[numtasks];
	//for(int i=0; i<numtasks; i++) msg[i] = i;

	//MPI_Scatter(msg,1,MPI_INT,&ilocal,1,MPI_INT,0,MPI_COMM_WORLD);
	Graph gLocal1=Graph(buf[taskid][0]);
	Graph gLocal2=Graph(buf[taskid][1]);
	delete[] buf;
	cerr<<"local graphs created, buf deleted.Joining local graphs"<<endl;
	gJoinedLocal = Graph::join(&gLocal1,var1,&gLocal2,var2);
	cerr<<"local graphs joined."<<endl;
	gLocal1.clean();
	gLocal2.clean();
	cerr<<"local graphs cleaned. Preparing to gather data."<<endl;
	int lengthLocal=gJoinedLocal.getSize()*gJoinedLocal.getArity();
	//gather length information from each process, prepare space for receiving 
	MPI_Gather(&lengthLocal,1,MPI_INT,lengths,1,MPI_INT,0,MPI_COMM_WORLD);
	if(taskid==0){
		int	length=0;
		for(int i=0;i<numtasks;i++){
			length+=lengths[i];
		}
		// cerr<<"total length of data to receive : "<<length<<endl;
		// cerr<<"Receiving data..."<<endl;
		gatheredRaw=vector<int>(length);
		for(int i=0;i<numtasks;i++){
			displs[i]=0;
			for(int j=0;j<i;j++){
				displs[i]+=lengths[j];
			}
		}

	}
	cerr<<"unfolding local join result."<<endl;
	unfoldedLocal=unfold(gJoinedLocal.relation);
	cerr<<"local join result unfolded."<<endl;
	gJoinedLocal.clean();
	cerr<<"local joined graph cleaned. Gathering data."<<endl;
	//when there is nothing to send, send a null pointer.
	if(unfoldedLocal.size()==0){
		 MPI_Gatherv(NULL,0,MPI_INT,&gatheredRaw[0],lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	}else{
		MPI_Gatherv(&unfoldedLocal[0],lengthLocal,MPI_INT,&gatheredRaw[0],lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	}
	cerr<<"data gathered to root"<<endl;
	vector<int>().swap(unfoldedLocal);
	cerr<<"local unfolded result cleaned."<<endl;
	if(taskid==0){
		// cerr<<"folding gathered data."<<endl;
		// gathered=fold(gatheredRaw,arityJoined);
		//cerr<<"gathered data folded."<<endl;
		//vector<int>().swap(gatheredRaw);
		//cerr<<"gathered raw data cleaned."<<endl;
		gJoined=Graph(gatheredRaw);
		cerr<<"gJoined created"<<endl;
	}
	return gJoined;

}

Graph Graph::mpiJoinHash(Graph* g1, vector<string> var1, Graph* g2, vector<string> var2){
	int taskid,numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

	vector<vector<int> > r1=g1->relation;
	vector<vector<int> > r2=g2->relation;
	vector<vector<int> > x = findCommon(var1, var2);
	int arityJoined=unionSize(var1,var2);//arity of joined relation	

	//int ilocal;	
	Graph gJoinedLocal,gJoined;
	vector<int> unfoldedLocal;	//local join result unfolded in 1D
	vector<vector<vector<int> > >* buf=new vector<vector<vector<int> > >[numtasks];
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
		buf[myhash((*it1)[x[0][0]])%numtasks][0].push_back(*it1);
	}

	for(it2=r2.begin(); it2!=r2.end(); it2++){
		buf[myhash((*it2)[x[1][0]])%numtasks][1].push_back(*it2);
	}
	//int msg[numtasks];
	//for(int i=0; i<numtasks; i++) msg[i] = i;

	//MPI_Scatter(msg,1,MPI_INT,&ilocal,1,MPI_INT,0,MPI_COMM_WORLD);
	Graph gLocal1=Graph(buf[taskid][0]);
	Graph gLocal2=Graph(buf[taskid][1]);
	delete[] buf;
	gJoinedLocal = Graph::join(&gLocal1,var1,&gLocal2,var2);
	gLocal1.clean();
	gLocal2.clean();
	int lengthLocal=gJoinedLocal.getSize()*gJoinedLocal.getArity();
	//gather length information from each process, prepare space for receiving 
	MPI_Gather(&lengthLocal,1,MPI_INT,lengths,1,MPI_INT,0,MPI_COMM_WORLD);
	if(taskid==0){
		int	length=0;
		for(int i=0;i<numtasks;i++){
			length+=lengths[i];
		}
		// cerr<<"total length of data to receive : "<<length<<endl;
		// cerr<<"Receiving data..."<<endl;
		gatheredRaw=vector<int>(length);
		for(int i=0;i<numtasks;i++){
			displs[i]=0;
			for(int j=0;j<i;j++){
				displs[i]+=lengths[j];
			}
		}

	}
	unfoldedLocal=unfold(gJoinedLocal.relation);
	gJoinedLocal.clean();
	//when there is nothing to send, send a null pointer.
	if(unfoldedLocal.size()==0){
		 MPI_Gatherv(NULL,0,MPI_INT,&gatheredRaw[0],lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	}else{
		MPI_Gatherv(&unfoldedLocal[0],lengthLocal,MPI_INT,&gatheredRaw[0],lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	}
	vector<int>().swap(unfoldedLocal);
	if(taskid==0){
		gathered=fold(gatheredRaw,arityJoined);
		vector<int>().swap(gatheredRaw);
		gJoined=Graph(gathered);
		return gJoined;
	}

}


Graph Graph::multiMPIJoin(Graph* g, vector<string>* v, int n){
	int taskid,numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

	vector<vector<int> > r1, r2;
	vector<string> var1 = v[0];
	vector<string> var2 = v[1];
	vector<vector<int> > x = findCommon(var1, var2);
	vector<vector<int> >::iterator it1;

	//distribute the first relation to each machine
	for(it1 = g[0].relation.begin(); it1 != g[0].relation.end(); it1++){
		if(myhash((*it1)[x[0][0]])%numtasks==taskid)
			r1.push_back(*it1);
	}

	for(int i=1; i<n; i++){

		vector<vector<int> >::iterator it2;
		//distribute the second relation to each machine
		r2.clear();
		for(it2 = g[i].relation.begin(); it2 != g[i].relation.end(); it2++){
			if(myhash((*it2)[x[1][0]])%numtasks == taskid)
				r2.push_back(*it2);

		}
		//join the two relations
		Graph gLocal1=Graph(r1);
		Graph gLocal2=Graph(r2);
		r1 = Graph::join(&gLocal1,var1,&gLocal2,var2).relation;

		//distribute the intermediate results
		if(i!=n-1){
			var2 = v[i+1];
			vector<vector<int> > buf[numtasks];//buf[i] stores the intermediate results to be sent to machine i
			int size[numtasks];//size[i] records the number of intermediate results to be sent
			for(int j=0; j<numtasks; j++) size[j] = 0;//initialize the numbers
			var1 = joinedVar(var1, var2);
			x = findCommon(var1, var2);
			//get buf[] and size[] ready to be sent
			for(it1 = r1.begin(); it1 != r1.end(); it1++){
				buf[myhash((*it1)[x[0][0]])%numtasks].push_back(*it1);
				size[myhash((*it1)[x[0][0]])%numtasks]+=var1.size();
			}
			//let each machine how many results they are expected to receive from other machines
			int rcvCount[numtasks];
			for(int j = 0 ; j<numtasks; ++j){
				MPI_Gather(&size[j],1,MPI_INT,rcvCount,1,MPI_INT,j,MPI_COMM_WORLD);
			}

			int displs[numtasks];
			displs[0] = 0;
			for (int j = 1; j < numtasks; ++j){
				displs[j] = rcvCount[j-1] + displs[j-1];
			}
			vector<int> unfoldSend[numtasks];
			for(int j=0;j<numtasks;++j){
				unfoldSend[j]=unfold(buf[j]);
			}
			int sizeRecv=0;
			for(int j=0;j<numtasks;++j){
				sizeRecv+=rcvCount[j];
			}
			vector<int> unfoldRecv(sizeRecv);
			//each machine gathers the intermediate results 
			for(int j=0; j<numtasks; ++j){
				if(unfoldSend[j].size()==0){
					MPI_Gatherv(NULL,0,MPI_INT,&unfoldRecv[0],rcvCount,displs,MPI_INT,j,MPI_COMM_WORLD);
				}else{
					MPI_Gatherv(&unfoldSend[j][0],size[j],MPI_INT,&unfoldRecv[0],rcvCount,displs,MPI_INT,j,MPI_COMM_WORLD);
				}
			}
			r1=fold(unfoldRecv,var1.size());		
		}
	}

	//gather all the results
	vector<int> unfolded=unfold(r1);
	int unfoldSize=unfolded.size();
	int unfoldSizes[numtasks];
	MPI_Gather(&unfoldSize,1,MPI_INT,unfoldSizes,1,MPI_INT,0,MPI_COMM_WORLD);
	int displs[numtasks];
	int recvSize;
	vector<int> gatheredRaw;
	if(taskid==0){
		displs[0] = 0;
		for (int i = 1; i < numtasks; ++i){
			displs[i] = unfoldSizes[i-1] + displs[i-1];
		}
		recvSize=displs[numtasks-1]+unfoldSizes[numtasks-1];

		gatheredRaw=vector<int>(recvSize);
	}
	if(unfoldSize==0){

		MPI_Gatherv(NULL,0,MPI_INT,&gatheredRaw[0],unfoldSizes,displs,MPI_INT,0,MPI_COMM_WORLD);
	} else {
		MPI_Gatherv(&unfolded[0],unfoldSize,MPI_INT,&gatheredRaw[0],unfoldSizes,displs,MPI_INT,0,MPI_COMM_WORLD);

	}
	if(taskid==0){
		r1=fold(gatheredRaw,r1[0].size());

	}

	return Graph(r1);

}

Graph Graph::HyperCubeJoin(Graph& g){
	int taskid,numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

	int m = (int)cbrt(numtasks);
	if(pow(m, 3)!=numtasks) {cerr<<"the number of machines must be a cuber of an integer!"; exit(EXIT_FAILURE);}
	vector<vector<int> > r, r1, r2, r3;
	vector<vector<int> >::iterator it;
	int m1,m2,m3;//encode the machine
	m1 = taskid/(m*m);
	m2 = taskid%(m*m)/m;
	m3 = taskid%(m*m)%m;
   
    //distribute the entries
	for(it = g.relation.begin(); it != g.relation.end(); it++){
		if((myhash((*it)[0])%m==m1)&&(myhash((*it)[1])%m==m2))
			r1.push_back(*it);
		if((myhash((*it)[0])%m==m2)&&(myhash((*it)[1])%m==m3))
			r2.push_back(*it);
		if((myhash((*it)[0])%m==m1)&&(myhash((*it)[1])%m==m3))
			r3.push_back(*it);
	}
	//join the sub-relations in each machine
	vector<string> var1,var2,var3,var;
	var1.push_back("x1");var1.push_back("x2");
	var2.push_back("x2");var2.push_back("x3");
	var3.push_back("x1");var3.push_back("x3");
	var = joinedVar(var1, var2);
	Graph g1(r1); Graph g2(r2);
	r = Graph::join(&g1,var1,&g2,var2).relation;
	Graph g12(r); Graph g3(r3);
	r = Graph::join(&g12,var,&g3,var3).relation;

	//gather the results
	vector<int> unfoldSend=unfold(r);
	vector<int> unfoldRecv;

	int lengthLocal=unfoldSend.size();
	int lengths[numtasks];
	MPI_Gather(&lengthLocal,1,MPI_INT,lengths,1,MPI_INT,0,MPI_COMM_WORLD);
	int displs[numtasks];

	if(taskid==0){
		int length=0;
		displs[0]=0;
		for(int i=0;i<numtasks;i++){
			length+=lengths[i];
		}
		for(int i=1;i<numtasks;i++){
			displs[i]=displs[i-1]+lengths[i-1];
		}
		unfoldRecv=vector<int>(length);

	}
	if(lengthLocal==0){
		MPI_Gatherv(NULL,0,MPI_INT,&unfoldRecv[0],lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	} else {
		MPI_Gatherv(&unfoldSend[0],lengthLocal,MPI_INT,&unfoldRecv[0],lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	}
	if(taskid==0){
		r=fold(unfoldRecv,3);

	}
	return Graph(r);

}

