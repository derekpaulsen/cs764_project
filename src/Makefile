.PHONY: workload test clean

CXX = g++-11 -std=c++20 -O3 -Wno-invalid-offsetof -mcx16 -DNDEBUG 
LIBS =  -fopenmp -lpthread -latomic -ltcmalloc_minimal

FILES = main.cpp ./opt_btree/*

test: vanilla
	./vanilla ./workload/seq_insert.txt

debug: $(FILES)
	$(CXX) ./main.cpp  -o debug $(LIBS) -O0 -g #-DNO_OMP


vanilla: $(FILES)
	$(CXX) ./main.cpp  -o vanilla $(LIBS) 

static_1: $(FILES)
	$(CXX) ./main.cpp  -o static_1 $(LIBS)  -DOMP_MODE=static,1


clean:
	rm vanilla debug static_1

workload:
	python3 ./generate_workload.py --n 50000000 --nreads 10000000
