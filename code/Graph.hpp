/*
 * Graph.hpp
 *
 *  Created on: May 1, 2017
 *		Author: yayundai & zejianli
*/

#include <string>
#include <vector>
#include <algorithm>

using namespace std;

//define a classe named Graph to store a relation and related methods
class Graph{
public:
	Graph(string path);//construct a graph from a given data path;
	Graph(){};
	~Graph();

	vector<vector<unsigned int>> relation;

	int getSize(){return relation.size();}//get the size of the relation
	int getArity(){return relation[0].size();}//get the arity of the relation
<<<<<<< HEAD
=======
	bool isEmpty(){return relation.empty();}
>>>>>>> 281ce98070220edc477f387f7881df88958376d4
	void order(vector<unsigned int> perm);//order the relation with a given permuation
	void saveTo(string path);//save the relation to a fiven file path
	static Graph join(Graph r1, vector<string> v1, Graph r2, vector<string> v2);//join two relations
};

//define a comparator to order the relation
struct Compare{
	vector<unsigned int> perm;
	Compare(vector<unsigned int> perm){this->perm=perm;}

	bool operator()(vector<unsigned int> l1, vector<unsigned int> l2){
		int n=perm.size();
		unsigned int prio[n];
		unsigned int i=0;
		//get the priority orders according to the permutation
		for(vector<unsigned int>::iterator it=perm.begin();it!=perm.end();it++){
			prio[(*it)-1]=i;
			i++;
		}

		int index;
		for(i=0;i<n;i++){
			index=prio[i];
			int diff=l1[index]-l2[index];
			if(diff<0){
				return true;
			}else if(diff>0){
				return false;
			}

		}
		return false;
	}
};

/**
 * \brief find the common variables in two variable lists
 * \param v1 the first variable list
 * \param v2 the second variable list
 *
 * \return two vectors that contain the indexes of the common variables in correspondant list.
*/

vector<vector<unsigned int>> findcommon(vector<string> v1, vector<string> v2);

/**
 * \brief compare the restrictions of two tuples onto X, the set of common variables 
 * \param v1 the first tuple
 * \param v2 the second tuple
 * \param commonPos two vectors that contain the indexes of the common variables in correspondant tuple
 * 
 * \return true if v1<v2
*/

bool customCompare(vector<unsigned int> v1, vector<unsigned int> v2, vector<vector<unsigned int> > commonPos);