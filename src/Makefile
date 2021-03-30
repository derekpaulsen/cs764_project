.PHONY: workload test clean

CXX = g++ -std=c++17 -O3 -Wno-invalid-offsetof -mcx16 -DNDEBUG
LIBS =  -latomic -fopenmp -lpthread #-ltcmalloc_minimal


test: vanilla
	./vanilla ./workload/seq_insert.txt

vanilla: main.cpp
	$(CXX) ./main.cpp  -o vanilla $(LIBS) 


clean:
	rm vanilla

workload:
	python3 ./generate_workload.py --n 50000000 --nreads 10000000
