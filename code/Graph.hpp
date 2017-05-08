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
	Graph(vector<vector<int> >& r){this->relation = r;};
	Graph(){this->relation=vector<vector<int> >();};
	~Graph();

	vector<vector<int> > relation;

	int getSize(){return relation.size();}//get the size of the relation
	int getArity(){
		if(relation.size()==0)
			return 0;
		return relation[0].size();
	}//get the arity of the relation
	bool isEmpty(){return relation.empty();}
	void order(vector<int> perm);//order the relation with a given permuation
	void saveTo(string path);//save the relation to a fiven file path
	static Graph join(Graph r1, vector<string> v1, Graph r2, vector<string> v2);//join two relations
	static void saveRelation(vector<vector<int> > r, string path);
};

//define a comparator to order the relation
struct Compare{
	vector<int> perm;
	Compare(vector<int> perm){this->perm=perm;}

	bool operator()(vector<int> l1, vector<int> l2){
		int n=perm.size();
		int prio[n];
		int i=0;
		//get the priority orders according to the permutation
		for(vector<int>::iterator it=perm.begin();it!=perm.end();it++){
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

vector<vector<int> > findcommon(vector<string> v1, vector<string> v2);

/**
 * \brief compare the restrictions of two tuples onto X, the set of common variables 
 * \param v1 the first tuple
 * \param v2 the second tuple
 * \param commonPos two vectors that contain the indexes of the common variables in correspondant tuple
 * 
 * \return true if v1<v2
*/

bool customCompare(vector<int> v1, vector<int> v2, vector<vector<int> > commonPos);
/**
 * \brief compute the size of union of two vectors of string, i.e. the number of different elements
 * \param v1 the first vector
 * \param v2 the second vector
*/

int unionSize(vector<string> v1, vector<string> v2);

/**
* \brief reconstruct a 2D vector from a 1D vector
* \param unfolded an 1D vector
* \param blockSize the length of each line
*/

vector<vector<int> > fold(vector<int> unfolded, int blockSize);
/**
* \brief unfold a 2D vector into an 1D vector by laying out the original vector line by line.
* \param mat the 2D vector to by unfolded
*/

vector<int> unfold(vector<vector<int> > mat);
