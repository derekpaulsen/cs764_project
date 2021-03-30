.PHONY: workload test

CXX = g++ -std=c++17 -O3
LIBS =  -latomic -fopenmp -lpthread


test: vanilla
	./vanilla ./workload/seq_insert.txt

vanilla:
	$(CXX) ./main.cpp  -o vanilla $(LIBS)

workload:
	python3 ./generate_workload.py --n 10000000 --nreads 5000000
