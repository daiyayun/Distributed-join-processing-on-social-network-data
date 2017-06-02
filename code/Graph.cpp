/*
 * Graph.cpp
 *
 *  Created on: May 1, 2017
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

//construct a graph from a data file
Graph::Graph(string path){
	using namespace std;
	string line;
	ifstream myProbe(path.c_str());
	//check lines and rows
	if(myProbe.is_open()){
		size=0;
		arity=0;
		string lastLine="";
		while(getline(myProbe,line)){
			if(line.compare(lastLine)){
				size++;
				istringstream iss(line);
				string s;
				if(size==1){
					while(getline(iss,s,' ')){
						arity++;
					}
				}
				lastLine=line;
			}

		}
		myProbe.close();
		relation=new int[arity*size];

	}
	else{ std::cerr<<"Unable to open file"<<endl;return;}
	// cerr<<"size: "<<size<<", arity: "<<arity<<endl;

	ifstream myfile(path.c_str());
	if(myfile.is_open()){
		int i=0;
		string lastLine="";
		while(getline(myfile,line)){
			//write data into the class
			if(line.compare(lastLine)){
				// cerr<<"lastLine: "<<lastLine<<endl;
				// cerr<<"this line: "<<line<<endl;
				// cerr<<"press enter to continue"<<endl;
				// cin.get();
				istringstream iss(line);
				string s;
				while(getline(iss, s, ' ')){
					//tuple.push_back(stoi(s));
					relation[i]=atoi(s.c_str());
					i++;
				}
				lastLine=line;
			}

		}
		myfile.close();

	}
	else std::cerr<<"Unable to open file"<<endl;
}
void Graph::print(){
	using namespace std;
		for(int i=0;i<size;i++){
			for(int j=0;j<arity;j++){
				cerr<<this->at(i,j);
				if(j<arity-1)
					cerr<<" ";
			}
			cerr << endl;
		}	
}
//save the relation to a given path
void Graph::saveTo(string path){
	using namespace std;
	ofstream myfile(path.c_str());
	if(myfile.is_open()){
		for(int i=0;i<size;i++){
			for(int j=0;j<arity;j++){
				myfile<<this->at(i,j);
				if(j<arity-1)
					myfile<<" ";
			}
			myfile << endl;
		}
		myfile.close();
	}
	else cerr<<"Unable to open file";
}


//order a relation with a given permutation
void Graph::order(const vector<int>& perm){
	using namespace std;
	vector<int*> head(size);
	for(int i=0;i<size;i++){
		head[i]=relation+i*arity;
	}	
	int* relationSorted=new int[size*arity];
	sort(head.begin(),head.end(),Compare(perm));
	for(int i=0;i<size;i++){
		for(int j=0;j<arity;j++){
			relationSorted[i*arity+j]=*(head[i]+j);
		}
	}
	for(int i=0;i<size*arity;i++){
		relation[i]=relationSorted[i];
	}
	delete[] relationSorted;
}
void Graph::joinTo(Graph* g1, vector<string> v1, Graph* g2, vector<string> v2, string path){
	using namespace std;
	int arityJoined = unionSize(v1,v2);//arity of joined relation	
	vector<vector<int> > x = findCommon(v1, v2);
	int l1 = v1.size();//nuSmber of variables in graph r1
	int l2 = v2.size();//number of variables in graph r2
	vector<int> perm1(l1);
	vector<int> perm2(l2);
	for(int i = 0; i < l1; i++) perm1[i] = 0;
	for(int i = 0; i < l2; i++) perm2[i] = 0;

	//create a permutation array for each relation, and order the two relations
	int n = x[0].size();//number of common variables
	for(int i = 0; i < n; i++){
		perm1[x[0][i]] = (i+1);
		perm2[x[1][i]] = (i+1);
	}
	int count = n;
	for(int i = 0; i < l1; i++){
		if(perm1[i] == 0){
			count++;
			perm1[i] = count;		
		} 
	}
	count = n;
	for(int i = 0; i < l2; i++){
		if(perm2[i] == 0){
			count++;
			perm2[i] = count;
		}
	}
	g1->order(perm1);
	g2->order(perm2);

	// join the two relations:
	// iterate over the two relations in parallel starting at the first tuple in each
	// and call the currently considered tuples t and t'. 
	// If t and t' coincide on X, add all combinations of tuples that agree with t and t' on X,
	// add the result to the output and jump in R and R' to the first tuples that disagree with t and t 0 on X;
	// If π(t) and π(t') are different two cases can occur: 
	//   If π(t) is lexicographically smaller than π(t'), go to the next tuple t;
	//   Otherwise, go to the next tuple t'.
	int* it1 = g1->relation;
	int* it2 = g2->relation;
	int* g1Last=g1->relation+(g1->size-1)*l1;
	int* g2Last=g2->relation+(g2->size-1)*l2;
	//Graph g;//the joined graph
	//vector<int> gRel;
	ofstream myfile(path.c_str());
	if(myfile.is_open()){
		while((it1<=g1Last)&&(it2<=g2Last)){
			bool flag = customCompare(it1,it2,x);
			if(flag){           //If t and t' coincide on X
				int pos = 1;
				while(((it2+pos*l2)<=g2Last)&&customCompare(it1,it2+pos*l2,x)){
					pos++;
				}

				while((it1<=g1Last)&&(it2<=g2Last)&&customCompare(it1,it2,x)){
					for(int i = 0; i < pos; i++){
						int count=0;
						for(int j = 0; j < l1; j++){
							myfile<<*(it1+j);
							if(count<arityJoined-1){
								myfile<<" ";
							}
							count++;
						}

						for(int j = 0; j < l2; j++){
							if(find(x[1].begin(),x[1].end(),j)==x[1].end()){
								myfile<<*(it2+i*l2+j);
								if(count<arityJoined-1){
									myfile<<" ";
								}
								count++;
							}	
						}
						myfile<<endl;

					}
					it1+=l1;

				}
				it2+=pos*l2;
			} else{           //If π(t) and π(t') are different
				vector<int> v1tmp, v2tmp;
				vector<int> perm;
				for(int i=1;i<=n;i++)
					perm.push_back(i);
				for(int i=0;i<n;i++){
					v1tmp.push_back(*(it1+x[0][i]));
					v2tmp.push_back(*(it2+x[1][i]));
				}
				if(Compare(perm)(&v1tmp[0],&v2tmp[0])){
					it1+=l1;
				} else {
					it2+=l2;
				}


			}
		}	
	myfile.close();
	}
	else cerr<<"Unable to open file";	
}


//join two relations
Graph* Graph::join(Graph* g1, vector<string> v1, Graph* g2, vector<string> v2){
	using namespace std;
	vector<vector<int> > x = findCommon(v1, v2);
	int l1 = v1.size();//number of variables in graph r1
	int l2 = v2.size();//number of variables in graph r2
	vector<int> perm1(l1);
	vector<int> perm2(l2);
	for(int i = 0; i < l1; i++) perm1[i] = 0;
	for(int i = 0; i < l2; i++) perm2[i] = 0;

	//create a permutation array for each relation, and order the two relations
	int n = x[0].size();//number of common variables
	for(int i = 0; i < n; i++){
		perm1[x[0][i]] = (i+1);
		perm2[x[1][i]] = (i+1);
	}
	int count = n;
	for(int i = 0; i < l1; i++){
		if(perm1[i] == 0){
			count++;
			perm1[i] = count;		
		} 
	}
	count = n;
	for(int i = 0; i < l2; i++){
		if(perm2[i] == 0){
			count++;
			perm2[i] = count;
		}
	}
	g1->order(perm1);
	g2->order(perm2);

	// join the two relations:
	// iterate over the two relations in parallel starting at the first tuple in each
	// and call the currently considered tuples t and t'. 
	// If t and t' coincide on X, add all combinations of tuples that agree with t and t' on X,
	// add the result to the output and jump in R and R' to the first tuples that disagree with t and t 0 on X;
	// If π(t) and π(t') are different two cases can occur: 
	//   If π(t) is lexicographically smaller than π(t'), go to the next tuple t;
	//   Otherwise, go to the next tuple t'.
	int* it1 = g1->relation;
	int* it2 = g2->relation;
	int* g1Last=g1->relation+(g1->size-1)*l1;
	int* g2Last=g2->relation+(g2->size-1)*l2;
	//Graph g;//the joined graph
	vector<int> gRel;
	int gArity=0;
	int gSize=0;
	while((it1<=g1Last)&&(it2<=g2Last)){
		bool flag = customCompare(it1,it2,x);
		if(flag){           //If t and t' coincide on X
			int pos = 1;
			while(((it2+pos*l2)<=g2Last)&&customCompare(it1,it2+pos*l2,x)){
				pos++;
			}

			while((it1<=g1Last)&&(it2<=g2Last)&&customCompare(it1,it2,x)){
				for(int i = 0; i < pos; i++){
					for(int j = 0; j < l1; j++){
						gRel.push_back(*(it1+j));
						if(gSize==0) gArity++;
					}

					for(int j = 0; j < l2; j++){
						if(find(x[1].begin(),x[1].end(),j)==x[1].end()){
							gRel.push_back(*(it2+i*l2+j));
							if(gSize==0) gArity++;
						}	
					}
					gSize++;
				}
				it1+=l1;

			}
			it2+=pos*l2;
		} else{           //If π(t) and π(t') are different
			vector<int> v1tmp, v2tmp;
			vector<int> perm;
			for(int i=1;i<=n;i++)
				perm.push_back(i);
			for(int i=0;i<n;i++){
				v1tmp.push_back(*(it1+x[0][i]));
				v2tmp.push_back(*(it2+x[1][i]));
			}
			if(Compare(perm)(&v1tmp[0],&v2tmp[0])){
				it1+=l1;
			} else {
				it2+=l2;
			}


		}
	}
	int* gRelation=new int[gRel.size()];
	for(int i=0;i<gRel.size();i++)
		gRelation[i]=gRel[i];

	return new Graph(gRelation,gArity,gSize);
};



//find the common variables in two varaible lists

vector<vector<int> > findCommon(vector<string>& v1, vector<string>& v2){
	int n1=v1.size();
	int n2=v2.size();
	string s1,s2;
	vector<vector<int> > result;
	vector<int> pos1,pos2;
	for(int i=0;i<n1;i++){
		s1=v1[i];
		for(int j=0;j<n2;j++){
			s2=v2[j];
			if(s1.compare(s2)==0){
				pos1.push_back(i);
				pos2.push_back(j);
			}
		}
	}
	result.push_back(pos1);
	result.push_back(pos2);
	return result;
};

//find the number of different elements in total
int unionSize(vector<string> v1, vector<string> v2){
	return joinedVar(v1,v2).size();
}

//get the combined list of variables
vector<string> joinedVar(vector<string>& v1, vector<string>& v2){
	vector<string> result;
	int n1=v1.size();
	int n2=v2.size();
	string s1,s2;
	bool contains;
	for(int i=0;i<n1;i++){
		result.push_back(v1[i]);
	}
	for(int i=0;i<n2;i++){
		s2=v2[i];
		contains=false;
		for(int j=0;j<n1;j++){
			s1=v1[j];
			if(s1.compare(s2)==0){
				contains=true;
				break;
			}
		}
		if(!contains){
			result.push_back(s2);
		}
	}
	return result;
}

//compare the restrictions of two tuples onto X, the set of common variables 

bool customCompare(int* v1,int* v2, vector<vector<int> > commonPos){
	int n=commonPos[0].size();
	for(int i = 0; i < n; i++){
		if(v1[commonPos[0][i]] != v2[commonPos[1][i]])
			return false;
	}
	return true;

};



int myhash(int key)
{
	using namespace std;
	return key*2654435761%(int)pow(2,32);
}


