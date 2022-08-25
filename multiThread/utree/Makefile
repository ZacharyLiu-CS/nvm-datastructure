.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -pthread -lpmemobj -lpmem
INCLUDES=-I ./atomic_ops
CFLAGS=-O3 -std=c++11 -g -D_REENTRANT -m64 -fno-strict-aliasing -DINTEL -Wno-unused-value -Wno-format

output = main-gu-zipfian

all: main

main: main-gu-zipfian.c 
	g++ ${CFLAGS} -c utree.cc
	ar -r libutree.a utree.o
	g++ $(CFLAGS) -o main-gu-zipfian main-gu-zipfian.c libutree.a $(LIBS) ${INCLUDES}
	
clean: 
	rm $(output)
	rm *.a
	rm *.o

