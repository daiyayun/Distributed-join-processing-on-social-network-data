#include <string>
#include <iostream>
#include "Graph.hpp"

//#include "Graph.hpp"
int main(void){
	using namespace std;
	const string path = "../twitter.dat.txt";
	Graph g(path);
	cerr <<g.getSize()<<endl<<g.getArity()<<endl;
	cerr <<g.relation.front().front()<<endl;
	cerr<<g.relation.front().back()<<endl;
	vector<unsigned int> p;
	p.push_back(1);
	p.push_back(2);
	g.order(p);
	list<list<unsigned int>>::iterator it;
	for(it = g.relation.begin(); it != g.relation.end(); it++){
		cout << (*it).front() << endl;
	}

}
