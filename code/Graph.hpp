#include <list>
#include <string>
#include <vector>
using namespace std;

class Graph{
public:
	Graph(string path);
	~Graph();
	vector<unsigned int> perm;

	list < list <unsigned int> > relation;

	int getSize(){return relation.size();}
	int getArity(){return relation.front().size();}
	void order(vector<unsigned int> perm);
	struct Local{
		vector<unsigned int> perm;
		Local(vector<unsigned int> perm){this->perm=perm;}

		bool operator()(list<unsigned int> l1, list<unsigned int> l2){
			int n=perm.size();
			vector<unsigned int> prio(n);
			int i=0;
			for(vector<unsigned int>::iterator it=perm.begin();it!=perm.end();it++){
				prio[*it-1]=i;
				i++;
			}
			int copyL1[n];
			int copyL2[n];

			i=0;
			for(list<unsigned int>::iterator it=l1.begin();it!=l1.end();it++){
				copyL1[i]=*it;
			}

			i=0;
			for(list<unsigned int>::iterator it=l2.begin();it!=l2.end();it++){
				copyL2[i]=*it;
			}

			int index;
			for(vector<unsigned int>::iterator it=prio.begin();it!=prio.end();it++){
				index=*it;
				int diff=copyL1[index]-copyL2[index];
				if(diff<0){
					return true;
				}else if(diff>0){
					return false;
				}

			}
			return false;
		}
	};

};

