#include <fstream>
#include <chrono>
#include <iostream>
#include <vector>
#include <locale>
#include <string>
#include "omp.h"
#include "./opt_btree/BTreeOLC.h"
#include "./opt_btree/BufferBTree.h"
#include "./opt_btree/RingBufferBTree.h"

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
bool verify(T<K,V> &tree, const std::vector<Operation> &ops) {
	V result;
	bool passed = true;
	for (const auto &op : ops) {
		const long k = op.get_key();

		switch (op.get_op_type()) {
			case Operation::INSERT:
				if (!tree.lookup(k, result)) {
					std::cerr << "Key missing : " << k << '\n';
					tree.lookup(k, result);
					passed = false;
					break;
				}
				if (result != k) {
					std::cerr << "Unexpected value from lookup : {key = " << k
							<< ", value = " << result << "}\n";
					passed = false;
				}
				break;
		};

		if (!passed)
			break;
	}
	return passed;
}


template<typename K, typename V, template <typename, typename> class T>
double execute_workload(T<K,V> &tree, const std::vector<Operation> &ops) {
	auto start = std::chrono::high_resolution_clock::now();
	// run in parallel with omp
#ifndef NO_OMP
	#pragma omp parallel for schedule(dynamic)
#endif
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
	auto s = std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count();
	std::cerr << "total time : " << s << " nanoseconds\n";

	std::cerr << "Verifying results...";
	bool pass = verify(tree, ops);
	if (pass)
		std::cerr << "ok\n";
	else
		std::cerr << "FAILED\n";

	return ops.size() * 1000000000 / s;
}

int main(int argc, char **argv) {
	if (argc != 2) {
		std::cerr << "usage <workload file>";
		return 1;
	}
	// show commas
	std::cout.imbue(std::locale(""));
	std::cerr.imbue(std::locale(""));
	
	std::string fname = argv[1];
	auto workload = read_workload(fname);
	
	std::cerr << "number of ops in workload : " << workload.size() << '\n';
	
	//std::cerr << "running baseline\n";
	//{
	//btreeolc::BTree<long, long> tree {};
	//
	//double ops = execute_workload(tree, workload);
	//std::cerr << "ops per second : "<< (long)ops << "\n\n";
	//}

	{
	std::cerr << "running BufferedBTree\n";
	BufferedBTree<long, long> buffered_tree {};
	
	double ops = execute_workload(buffered_tree, workload);
	std::cout << "ops per second : "<< (long)ops << "\n\n";
	}

	//{
	//std::cerr << "running RingBufferBTree\n";
	//RingBufferBTree<long, long> ring_buffer_tree {};
	//
	//double ops = execute_workload(ring_buffer_tree, workload);
	//std::cout << "ops per second : "<< (long)ops << "\n\n";
	//}
	return 0;
}

