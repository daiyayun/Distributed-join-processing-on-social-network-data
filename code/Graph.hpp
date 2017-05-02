#include <string>
#include <vector>
using namespace std;

class Graph{
public:
	Graph(string path);
	~Graph();

	vector < vector <unsigned int> > relation;

	int getSize(){return relation.size();}
	int getArity(){return relation[0].size();}
	void order(vector<unsigned int> perm);
	void saveTo(string path);


};

struct Local{
	vector<unsigned int> perm;
	Local(vector<unsigned int> perm){this->perm=perm;}

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

