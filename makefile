all: topk.out gen.out check.out

topk.out: src/main.cpp
	g++ src/main.cpp -std=c++0x -lrt -g -O2 -o topk.out
	
gen.out: src/generate_data.cpp
	g++ src/generate_data.cpp -std=c++0x -lrt -g -O2 -o gen.out
	
check.out: src/check.cpp
	g++ src/check.cpp -std=c++0x -lrt -g -O2 -o check.out

clean:
	rm topk.out gen.out check.out
	
