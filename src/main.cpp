#include <fstream>
#include <chrono>
#include <iostream>
#include <vector>
#include <string>
#include "omp.h"
#include "./BTreeOLC/BTreeOLC.h"

using namespace std::string_literals;


class Operation {
	public:
		static const long INSERT = 0;
		static const long READ = 1;

	private:
		long op_type, key;


	public:
		Operation(const std::string &op, long key) : key(key) {
			if (op == "INSERT"s) 
				op_type = INSERT;
			else if(op == "READ"s) 
				op_type = READ;
			else
				throw std::runtime_error("Unknown op type "s + op);
		}

		long get_op_type() const {
			return op_type;
		}

		long get_key() const {
			return key;
		}
};

std::vector<Operation> read_workload(const std::string &fname) {
	std::ifstream ifs(fname);
	std::vector<Operation> ops {};
	std::string op;
	long key;
	// format [READ|INSERT] <KEY>
	while(!ifs.eof()) {
		ifs >> op >> key;
		ops.emplace_back(op, key);
	}

	return ops;
}

template<typename K, typename V, template <typename, typename> class T>
double execute_workload(T<K,V> &tree, const std::vector<Operation> &ops) {
	auto start = std::chrono::high_resolution_clock::now();
	// run in parallel with omp
	#pragma omp parallel for schedule(dynamic)
	for (size_t i = 0; i < ops.size(); i++) {
		const Operation &op = ops[i];
		const long k = op.get_key();
		long v;

		switch (op.get_op_type()) {
			case Operation::INSERT:
				tree.insert(k, k);
				break;
			case Operation::READ:
				bool success = tree.lookup(k, v);
				break;
		};
	}
    auto finish = std::chrono::high_resolution_clock::now();
	auto s = std::chrono::duration_cast<std::chrono::seconds>(finish-start).count();
	std::cerr << "total time : " << s << '\n';
	return ops.size() / s;
}

int main(int argc, char **argv) {
	if (argc != 2) {
		std::cerr << "usage <workload file>";
		return 1;
	}

	std::string fname = argv[1];
	auto workload = read_workload(fname);

	std::cerr << "number of ops in workload : " << workload.size() << '\n';
	
	btreeolc::BTree<long, long> tree {};
	
	const double ops = execute_workload(tree, workload);
	std::cout << "ops per second "<< ops;

	return 0;
}

