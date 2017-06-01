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

Graph* Graph::MPIJoin(Graph* g1, vector<string> var1, Graph* g2, vector<string> var2,bool useHash=false){
	int taskid,numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	int a1=g1->arity;
	int a2=g2->arity;
	//int* r1 = g1->relation;
	//int* r2 = g2->relation;
	//int* r1Last=g1->relation+(g1->size-1)*a1;
	//int* r2Last=g2->relation+(g2->size-1)*a2;
	vector<vector<int> > x = findCommon(var1, var2);
	int arityJoined = unionSize(var1,var2);//arity of joined relation	

	//int ilocal;	
	Graph* gJoinedLocal;
	Graph* gJoined;
	vector<vector<int> >* buf=new vector<vector<int> >[numtasks];
	int lengths[numtasks];//length of each local join result
	int displs[numtasks];// displacement of each local join result w.r.t. address of receive buffer
	//vector<int> gatheredRaw; //gathered join result in 1D
	int* gathered; 
	
	//assigning the task for each process
	
	for(int i=0;i<numtasks;i++){
		vector<int> tmp1,tmp2;
		buf[i].push_back(tmp1);
		buf[i].push_back(tmp2);

	}
	if(useHash){
		// cerr<<"using hash."<<endl;
		for(int i=0;i<g1->size;i++){
			for(int j=0;j<a1;j++){
				buf[myhash(g1->at(i,x[0][0]))%numtasks][0].push_back(g1->at(i,j));
			}
		}

		for(int i=0;i<g2->size;i++){
			for(int j=0;j<a2;j++){
				buf[myhash(g2->at(i,x[1][0]))%numtasks][1].push_back(g2->at(i,j));
			}
		}
	}else{
		for(int i=0;i<g1->size;i++){
			for(int j=0;j<a1;j++){
				buf[g1->at(i,x[0][0])%numtasks][0].push_back(g1->at(i,j));
			}
		}

		for(int i=0;i<g2->size;i++){
			for(int j=0;j<a2;j++){
				buf[g2->at(i,x[1][0])%numtasks][1].push_back(g2->at(i,j));
			}
		}
	}



	int* gLocal1Rel=new int[buf[taskid][0].size()];
	int* gLocal2Rel=new int[buf[taskid][1].size()];
	
	for(int i=0;i<buf[taskid][0].size();i++){
		gLocal1Rel[i]=buf[taskid][0][i];
	}
	for(int i=0;i<buf[taskid][1].size();i++){
		gLocal2Rel[i]=buf[taskid][1][i];
	}
	Graph* gLocal1=new Graph(gLocal1Rel,a1,buf[taskid][0].size()/a1);
	Graph* gLocal2=new Graph(gLocal2Rel,a2,buf[taskid][1].size()/a2);


	delete[] buf;
	gJoinedLocal = Graph::join(gLocal1,var1,gLocal2,var2);
	delete gLocal1;
	delete gLocal2;
	int lengthLocal=gJoinedLocal->size*gJoinedLocal->arity;
	//gather length information from each process, prepare space for receiving 
	MPI_Gather(&lengthLocal,1,MPI_INT,lengths,1,MPI_INT,0,MPI_COMM_WORLD);
	int length;
	if(taskid==0){
		length=0;
		for(int i=0;i<numtasks;i++){
			length+=lengths[i];
		}

		gathered=new int[length];
		displs[0]=0;
		for(int i=1;i<numtasks;i++){
			displs[i]=displs[i-1]+lengths[i-1];
		}

	}

	//when there is nothing to send, send a null pointer.
	if(gJoinedLocal->isEmpty()){
		 MPI_Gatherv(NULL,0,MPI_INT,gathered,lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	}else{
		MPI_Gatherv(gJoinedLocal->relation,lengthLocal,MPI_INT,gathered,lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	}
	if(taskid==0){
		for(int i=0;i<length;i++){
		}
		gJoined=new Graph(gathered,arityJoined,length/arityJoined);
	}
	return gJoined;

}

Graph* Graph::multiMPIJoin(Graph** g, vector<string>* v, int n){
	int taskid,numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	vector<string> var1 = v[0];
	vector<string> var2 = v[1];
	vector<vector<int> > x = findCommon(var1, var2);
	//distribute the first relation to each machine
	vector<int> r1buf,r2buf;
	int* r1;
	int* r2;
	for(int i=0; i<g[0]->size; i++){
		if(myhash(g[0]->at(i,x[0][0]))%numtasks==taskid){
			for(int j=0;j<g[0]->arity;j++){
				r1buf.push_back(g[0]->at(i,j));
			}
		}
	}
	r1=new int[r1buf.size()];
	for(int i=0;i<r1buf.size();i++){
		r1[i]=r1buf[i];
	}

	int a1,s1,a2,s2;
	a1=g[0]->arity;
	s1=r1buf.size()/a1;
	//vector<int>().swap(r1buf);
	Graph* gJoined;

	for(int i=1; i<n; i++){

		//distribute the second relation to each machine
		r2buf.clear();
		for(int j=0;j<g[i]->size;j++){
			if(myhash(g[i]->at(j,x[1][0]))%numtasks == taskid){
				for(int k=0;k<g[i]->arity;k++){
					r2buf.push_back(g[i]->at(j,k));
				}
			}
		}
		r2=new int[r2buf.size()];
		for(int j=0;j<r2buf.size();j++){
			r2[j]=r2buf[j];
		}

		a2=g[i]->arity;
		s2=r2buf.size()/a2;

		//join the two relations
		Graph gLocal1=Graph(r1,a1,s1);
		Graph gLocal2=Graph(r2,a2,s2);
		gJoined = Graph::join(&gLocal1,var1,&gLocal2,var2);
		//distribute the intermediate results
		if(i!=n-1){
			var2 = v[i+1];
			vector<int> buf[numtasks];//buf[i] stores the intermediate results to be sent to machine i
			int size[numtasks];//size[i] records the number of intermediate results to be sent
			for(int j=0; j<numtasks; j++) size[j] = 0;//initialize the numbers
			var1 = joinedVar(var1, var2);
			x = findCommon(var1, var2);
			//get buf[] and size[] ready to be sent
		
			for(int j=0;j<gJoined->size;j++){
				for(int k=0;k<gJoined->arity;k++){
					buf[myhash(gJoined->at(j,x[0][0]))%numtasks].push_back(gJoined->at(j,k));

				}
				size[myhash(gJoined->at(j,x[0][0]))%numtasks]+=var1.size();
			}
			delete gJoined;

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
			int sizeRecv=0;
			for(int j=0;j<numtasks;++j){
				sizeRecv+=rcvCount[j];
			}



			r1=new int[sizeRecv];


			//each machine gathers the intermediate results 
			for(int j=0; j<numtasks; ++j){

				if(buf[j].size()==0){
					MPI_Gatherv(NULL,0,MPI_INT,r1,rcvCount,displs,MPI_INT,j,MPI_COMM_WORLD);
				}else{
					MPI_Gatherv(&buf[j][0],size[j],MPI_INT,r1,rcvCount,displs,MPI_INT,j,MPI_COMM_WORLD);
				}
			}
			a1=var1.size();
			s1=sizeRecv/a1;


		}
	}
	vector<int>().swap(r1buf);
	vector<int>().swap(r2buf);
	//gather all the results
	int size[numtasks];
	int sizeLocal=gJoined->size*gJoined->arity;
	MPI_Gather(&sizeLocal,1,MPI_INT,size,1,MPI_INT,0,MPI_COMM_WORLD);
	int displs[numtasks];
	int recvSize;
	int* gathered;
	if(taskid==0){
		displs[0] = 0;
		for (int i = 1; i < numtasks; ++i){
			displs[i] = size[i-1] + displs[i-1];
		}
		recvSize=displs[numtasks-1]+size[numtasks-1];

		gathered=new int[recvSize];
	}
	if(sizeLocal==0){

		MPI_Gatherv(NULL,0,MPI_INT,gathered,size,displs,MPI_INT,0,MPI_COMM_WORLD);
	} else {
		MPI_Gatherv(gJoined->relation,sizeLocal,MPI_INT,gathered,size,displs,MPI_INT,0,MPI_COMM_WORLD);

	}
	if(taskid==0){
		a1=unionSize(var1,var2);
		s1=recvSize/a1;

	}

	return new Graph(gathered,a1,s1);

}

Graph* Graph::HyperCubeJoin(Graph* g){
	int taskid,numtasks;
	MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

	int m = (int)cbrt(numtasks);
	if(pow(m, 3)!=numtasks) {cerr<<"the number of machines must be a cuber of an integer!"; exit(EXIT_FAILURE);}
	vector<int>  rbuf[3];
	int m1,m2,m3;//encode the machine
	m1 = taskid/(m*m);
	m2 = taskid%(m*m)/m;
	m3 = taskid%(m*m)%m;
   
    //distribute the entries
	for(int i=0;i<g->size;i++){
		if((myhash(g->at(i,0))%m==m1)&&(myhash(g->at(i,1))%m==m2))
			for(int j=0;j<g->arity;j++)
				rbuf[0].push_back(g->at(i,j));
		if((myhash(g->at(i,0))%m==m2)&&(myhash(g->at(i,1))%m==m3))
			for(int j=0;j<g->arity;j++)
				rbuf[1].push_back(g->at(i,j));
		if((myhash(g->at(i,0))%m==m1)&&(myhash(g->at(i,1))%m==m3))
			for(int j=0;j<g->arity;j++)
				rbuf[2].push_back(g->at(i,j));
	}
	int sRes;
	int a=g->arity;
	int s[3];
	int* r[3];
	for(int i=0;i<3;i++){
		s[i]=rbuf[i].size();
		r[i]=new int[s[i]];
		for(int j=0;j<s[i];j++){
			r[i][j]=rbuf[i][j];
		}
		s[i]/=a;
	}

	for(int i=0;i<3;i++){
		vector<int>().swap(rbuf[i]);
	}

	
	//join the sub-relations in each machine
	vector<string> var1,var2,var3,var;
	var1.push_back("x1");var1.push_back("x2");
	var2.push_back("x2");var2.push_back("x3");
	var3.push_back("x1");var3.push_back("x3");
	var = joinedVar(var1, var2);
	Graph g1(r[0],a,s[0]); Graph g2(r[1],a,s[1]);
	Graph* gJoined = Graph::join(&g1,var1,&g2,var2);
	Graph g3(r[2],a,s[2]);
	gJoined = Graph::join(gJoined,var,&g3,var3);
	//gather the results
	int* gathered;
	int lengthLocal=gJoined->size*gJoined->arity;
	int lengths[numtasks];
	MPI_Gather(&lengthLocal,1,MPI_INT,lengths,1,MPI_INT,0,MPI_COMM_WORLD);
	int displs[numtasks];
	int length;

	if(taskid==0){
		length=0;
		displs[0]=0;
		for(int i=0;i<numtasks;i++){
			length+=lengths[i];
		}
		for(int i=1;i<numtasks;i++){
			displs[i]=displs[i-1]+lengths[i-1];
		}
		gathered=new int[length];

	}
	if(lengthLocal==0){
		MPI_Gatherv(NULL,0,MPI_INT,gathered,lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	} else {
		MPI_Gatherv(gJoined->relation,lengthLocal,MPI_INT,gathered,lengths,displs,MPI_INT,0,MPI_COMM_WORLD);
	}
	if(taskid==0){
		a=unionSize(var,var3);
		sRes=length/a;
	}

	return new Graph(gathered,a,sRes);

}