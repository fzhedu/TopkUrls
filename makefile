all: topk.out

topk.out: src/main.cpp
	g++ src/main.cpp -std=c++0x -lrt -g -O2 -o topk.out
	
clean:
	rm topk.out
	
