#include <string>
#include <iostream>
#include "Graph.hpp"

//#include "Graph.hpp"
int main(void){
	using namespace std;
	const string path = "../test.dat";
	Graph g(path);
	cerr<<"Before sorting: "<<endl;
	list<list<unsigned int>>::iterator it;
	for(it = g.relation.begin(); it != g.relation.end(); it++){
		cerr << (*it).front() <<" "<<(*it).back()<< endl;
	}
	cerr<<"After sorting: "<<endl;
	vector<unsigned int> p;
	p.push_back(1);
	p.push_back(2);
	g.order(p);
	for(it = g.relation.begin(); it != g.relation.end(); it++){
		cout << (*it).front()<<" "<<(*it).back() << endl;
	}

}
