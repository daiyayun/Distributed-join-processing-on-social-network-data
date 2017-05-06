#include<string>
#include<vector>
using namespace std;

vector<vector<int> > findCommon(vector<string> v1, vector<string> v2){
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
}