#include <string>
#include <list>
#include <fstream>
#include <iostream>

//#include "Graph.hpp"
int main(void){
	using namespace std;
	string line;
	const string path = "../twitter.dat.txt";
	ifstream myfile(path.c_str());
	if(myfile.is_open()){
		while(getline(myfile,line)){
			//write data into the class
			// istringstream iss(line);
			// string s;
			// list<unsigned int> tuple;
			// while(getline(iss, s, ' ')){
			// 	tuple.push_back(stoi(s));
			// }
			// relation.push_back(tuple);
		}
		myfile.close();
	}
	else std::cerr<<"Unable to open file"<<endl;
}
