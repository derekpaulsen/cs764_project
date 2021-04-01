.PHONY: workload test clean

CXX = g++-11 -std=c++20 -O3 -Wno-invalid-offsetof -mcx16 -DNDEBUG
LIBS =  -fopenmp -lpthread -ltcmalloc_minimal

FILES = main.cpp ./opt_btree/BTreeOLC.h ./opt_btree/BufferBTree.h

test: vanilla
	@echo "number of threads = $(OMP_NUM_THREADS)"
	./vanilla ./workload/seq_insert.txt

debug: $(FILES)
	$(CXX) ./main.cpp  -o debug $(LIBS) -O0 -g


vanilla: $(FILES)
	$(CXX) ./main.cpp  -o vanilla $(LIBS) 

clean:
	rm vanilla debug

workload:
	python3 ./generate_workload.py --n 50000000 --nreads 10000000
