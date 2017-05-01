#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>

#include "Graph.hpp"

Graph::Graph(string path){
	using namespace std;
	string line;
	ifstream myfile(path.c_str());
	if(myfile.is_open()){
		while(getline(myfile,line)){
			//write data into the class
			istringstream iss(line);
			string s;
			vector<unsigned int> tuple;
			while(getline(iss, s, ' ')){
				tuple.push_back(stoi(s));
			}
			relation.push_back(tuple);
		}
		myfile.close();
	}
	else std::cerr<<"Unable to open file"<<endl;
}

void Graph::order(vector<unsigned int> perm){
	using namespace std;	
	sort(this->relation.begin(),this->relation.end(),Local(perm));
}


void Graph::saveTo(string path){
	using namespace std;
	ofstream myfile(path.c_str());
	if(myfile.is_open()){
		vector<vector<unsigned int>>::iterator it;
		for(it = relation.begin(); it != relation.end(); it++){
			myfile << (*it).front() <<" "<<(*it).back()<< endl;
		}
		myfile.close();
	}
	else cerr<<"Unable to open file";
}


Graph::~Graph(){}

