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

bool customCompare(vector<unsigned int> v1, vector<unsigned int> v2, vector<vector<unsigned int> > commonPos){
	int n=commonPos[0].size();
	for(int i = 0; i < n; i++){
		if(v1[commonPos[0][i]] != v2[commonPos[1][i]])
			return false;

	}
	return true;

}


// bool contains(vector<unsigned int> const &v, unsigned int const &elem){
// 	return find(v.begin(),v.end(),elem)!=v.end();
// };


//join two relations
Graph join(Graph r1, vector<string> v1, Graph r2, vector<string> v2){
	vector<vector<unsigned int> > x = findcommon(v1, v2);
	int l1 = v1.size();//number of variables in graph r1
	int l2 = v2.size();//number of variables in graph r2
	vector<unsigned int> perm1(l1);
	vector<unsigned int> perm2(l2);
	for(int i = 0; i < l1; i++) perm1[i] = 0;
	for(int i = 0; i < l2; i++) perm2[i] = 0;

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

	vector<vector<unsigned int> >::iterator it1 = r1.relation.begin();
	vector<vector<unsigned int> >::iterator it2 = r2.relation.begin();
	Graph g;
	while((it1!=r1.relation.end())&&(it2!=r2.relation.end())){
		bool flag = customCompare((*it1),(*it2),x);
		if(flag){
			int pos = 0;
			while(customCompare(*it1,*(it2+pos+1),x)){
				pos++;
			}

			while(customCompare(*(it1+1),*it2,x)){
				for(int i = 0; i < pos; i++){
					vector<unsigned int> v;
					for(int j = 0; j < l1; j++){
						v.push_back((*it1)[j]);
					}

					for(unsigned int j = 0; j< l2; j++){
						// if(!contains(x[1],j)){
						if(find(x[1].begin(),x[1].end(),j)!=x[1].end()){
							v.push_back((*it2)[j]);
						}
						
					}
					g.relation.push_back(v);
				}
			}
		} else{
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

