#include <list>
#include <string>
using namespace std;

class Graph{
public:
	Graph(string path);
	~Graph();

	list < list <unsigned int> > relation;

	int getSize(){return relation.size();}
	int getArity(){return relation.front().size();}
};

