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

#include "Graph.hpp"

//construct a graph from a data file
Graph::Graph(string path){
	using namespace std;
	string line;
	ifstream myfile(path.c_str());
	if(myfile.is_open()){
		while(getline(myfile,line)){
			//write data into the class
			istringstream iss(line);
			string s;
			vector<unsigned int> tuple;
			while(getline(iss, s, ' ')){
				tuple.push_back(stoi(s));
			}
			relation.push_back(tuple);
		}
		myfile.close();
	}
	else std::cerr<<"Unable to open file"<<endl;
}

//order a relation with a given permutation
void Graph::order(vector<unsigned int> perm){
	using namespace std;	
	sort(this->relation.begin(),this->relation.end(),Compare(perm));
}

//save the relation to a given path
void Graph::saveTo(string path){
	using namespace std;
	int l=getArity();
	ofstream myfile(path.c_str());
	if(myfile.is_open()){
		vector<vector<unsigned int>>::iterator it;
		for(it = relation.begin(); it != relation.end(); it++){
			int i=0;
			for(vector<unsigned int>::iterator jt=it->begin();jt!=it->end();jt++){
				myfile<<(*jt);
				if(i<l-1)
					myfile<<" ";
				i++;
			}
			myfile << endl;
		}
		myfile.close();
	}
	else cerr<<"Unable to open file";
}

//join two relations
Graph Graph::join(Graph r1, vector<string> v1, Graph r2, vector<string> v2){
	using namespace std;
	vector<vector<unsigned int> > x = findcommon(v1, v2);
	int l1 = v1.size();//number of variables in graph r1
	int l2 = v2.size();//number of variables in graph r2
	vector<unsigned int> perm1(l1);
	vector<unsigned int> perm2(l2);
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
	r1.order(perm1);
	r2.order(perm2);

	// join the two relations:
	// iterate over the two relations in parallel starting at the first tuple in each
	// and call the currently considered tuples t and t'. 
	// If t and t' coincide on X, add all combinations of tuples that agree with t and t' on X,
	// add the result to the output and jump in R and R' to the first tuples that disagree with t and t 0 on X;
	// If π(t) and π(t') are different two cases can occur: 
	//   If π(t) is lexicographically smaller than π(t'), go to the next tuple t;
	//   Otherwise, go to the next tuple t'.

	vector<vector<unsigned int> >::iterator it1 = r1.relation.begin();
	vector<vector<unsigned int> >::iterator it2 = r2.relation.begin();
	Graph g;//the joined graph
	while((it1!=r1.relation.end())&&(it2!=r2.relation.end())){
		bool flag = customCompare((*it1),(*it2),x);
		if(flag){           //If t and t' coincide on X
			int pos = 1;
			while(((it2+pos)!=r2.relation.end())&&customCompare(*it1,*(it2+pos),x)){
				pos++;
			}

			while((it1!=r1.relation.end())&&(it2!=r2.relation.end())&&customCompare(*it1,*it2,x)){
				for(int i = 0; i < pos; i++){
					vector<unsigned int> v;
					for(int j = 0; j < l1; j++){
						v.push_back((*it1)[j]);
					}

					for(unsigned int j = 0; j < l2; j++){
						// if(!contains(x[1],j)){
						if(find(x[1].begin(),x[1].end(),j)==x[1].end()){
							v.push_back((*(it2+i))[j]);
						}
						
					}
					g.relation.push_back(v);
				}
				it1++;

			}
			it2+=pos;
		} else{           //If π(t) and π(t') are different
			vector<unsigned int> v1tmp, v2tmp, perm;
			for(int i=1;i<=n;i++)
				perm.push_back(i);
			for(int i=0;i<n;i++){
				v1tmp.push_back((*it1)[x[0][i]]);
				v2tmp.push_back((*it2)[x[1][i]]);
			}
			if(Compare(perm)(v1tmp,v2tmp)){
				it1++;
			} else {
				it2++;
			}


		}
	}
	return g;
};



Graph::~Graph(){}

//find the common variables in two varaible lists

vector<vector<unsigned int>> findcommon(vector<string> v1, vector<string> v2){
	int n1=v1.size();
	int n2=v2.size();
	string s1,s2;
	vector<vector<unsigned int> > result;
	vector<unsigned int> pos1,pos2;
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

//compare the restrictions of two tuples onto X, the set of common variables 

bool customCompare(vector<unsigned int> v1, vector<unsigned int> v2, vector<vector<unsigned int> > commonPos){
	int n=commonPos[0].size();
	for(int i = 0; i < n; i++){
		if(v1[commonPos[0][i]] != v2[commonPos[1][i]])
			return false;
	}
	return true;

};


