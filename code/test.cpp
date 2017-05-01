#include <string>
#include <vector>
#include <iostream>
#include "Graph.hpp"

//#include "Graph.hpp"
int main(void){
	using namespace std;
	const string fileName="twitter";
	const string path = "../"+fileName+".dat";
	const string pathOut="../"+fileName+"_sorted.dat";
	Graph g(path);

	cerr<<"Size: "<<g.getSize()<<endl;
	cerr<<"Arity: "<<g.getArity()<<endl;
	// cerr<<"Before sorting: "<<endl;
	// vector<vector<unsigned int>>::iterator it;
	// for(it = g.relation.begin(); it != g.relation.end(); it++){
	// 	cerr << (*it).front() <<" "<<(*it).back()<< endl;
	// }
	// cerr<<"After sorting: "<<endl;
	vector<unsigned int> p={2,1};
	g.order(p);
	g.saveTo(pathOut);
	// for(it = g.relation.begin(); it != g.relation.end(); it++){
	// 	cout << (*it).front()<<" "<<(*it).back() << endl;
	// }

}
