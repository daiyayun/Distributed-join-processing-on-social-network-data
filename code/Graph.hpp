#include <string>
#include <vector>
#include <algorithm>
using namespace std;

//define a classe named Graph to store a relation
class Graph{
public:
	Graph(string path);
	Graph(){};
	~Graph();

	vector<vector<unsigned int>> relation;

	int getSize(){return relation.size();}
	int getArity(){return relation[0].size();}
	void order(vector<unsigned int> perm);
	void saveTo(string path);
	static Graph join(Graph r1, vector<string> v1, Graph r2, vector<string> v2);


};

//define a comparator to order the relation
struct Compare{
	vector<unsigned int> perm;
	Compare(vector<unsigned int> perm){this->perm=perm;}

	bool operator()(vector<unsigned int> l1, vector<unsigned int> l2){
		int n=perm.size();
		unsigned int prio[n];
		unsigned int i=0;
		for(vector<unsigned int>::iterator it=perm.begin();it!=perm.end();it++){
			prio[(*it)-1]=i;
			i++;
		}

		int index;
		for(i=0;i<n;i++){
			index=prio[i];
		//	cerr<<"current index : "<<index;
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

//find the common variables in two varaible lists
vector<vector<unsigned int>> findcommon(vector<string> v1, vector<string> v2);

bool customCompare(vector<unsigned int> v1, vector<unsigned int> v2, vector<vector<unsigned int> > commonPos);


// bool contains(vector<unsigned int> const &v, unsigned int const &elem){
// 	return find(v.begin(),v.end(),elem)!=v.end();
// };




