/*
 * Graph.hpp
 *
 *  Created on: May 1, 2017
 *		Author: yayundai & zejianli
*/

#include <string>
#include <vector>
#include <algorithm>
#include <stdint.h>

;


using namespace std;
const bool USE_HASH=true;
const bool NO_HASH=false;
//define a classe named Graph to store a relation and related methods
class Graph{
public:
	Graph(){}
	Graph(string path);//construct a graph from a given data path;
	Graph(int* r,int arity, int size){this->relation=r;this->arity=arity;this->size=size;};
	~Graph(){delete[] relation;};


	int arity;
	int size;
	int* relation;

	//void clean(){delete[] relation;}

	int at(int i,int j){ return relation[i*arity+j];};
	bool isEmpty(){return size==0;}
	void order(const vector<int>& perm);//order the relation with a given permuation
	void saveTo(string path);//save the relation to a fiven file path
	static void joinTo(Graph* g1, vector<string> v1, Graph* g2, vector<string> v2, string path);
	void print();
	static Graph* join(Graph* g1, vector<string> v1, Graph* g2, vector<string> v2);//join two relations
	static Graph* MPIJoin(Graph* g1, vector<string> var1, Graph* g2, vector<string> var2,bool useHash);//join two reltions using mpi
	//static Graph MPIJoinHash(Graph* g1, vector<string> var1, Graph* g2, vector<string> var2);
	static Graph* multiMPIJoin(Graph** g, vector<string>* v, int n);//join any number of relations
	static Graph* HyperCubeJoin(Graph* g);// a join method that does not need to communicate the intermediate results
	//static void saveRelation(vector<vector<int> >& r, string& path);//save a relation to a file
	//static uint32_t hash(uint32_t a);
};

//define a comparator to order the relation
struct Compare{
	vector<int> perm;
	//int arity;
	Compare(const vector<int>& perm){this->perm=perm;}

	bool operator()(int* l1,int* l2){
		int n=perm.size();
		for(int i=0;i<n; i++){
			if(l1[perm[i]-1]<l2[perm[i]-1]) return true;
			if(l1[perm[i]-1]>l2[perm[i]-1]) return false;
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

vector<vector<int> > findCommon(vector<string>& v1, vector<string>& v2);

/**
 * \brief calculate the list of variables of the joined relation
*/

vector<string> joinedVar(vector<string>& v1, vector<string>& v2) ;

/**
 * \brief compare the restrictions of two tuples onto X, the set of common variables 
 * \param v1 the first tuple
 * \param v2 the second tuple
 * \param commonPos two vectors that contain the indexes of the common variables in correspondant tuple
 * 
 * \return true if v1<v2
*/

bool customCompare(int* v1, int* v2, vector<vector<int> > commonPos);

/**
 * \brief compute the size of union of two vectors of string, i.e. the number of different elements
 * \param v1 the first vector
 * \param v2 the second vector
*/

int unionSize(vector<string> v1, vector<string> v2);



/**
* \brief an integer hash function
*/
uint32_t myhash(uint32_t a);