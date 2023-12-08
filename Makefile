
ifdef D
   CXXFLAGS=-Wall -std=c++11 -g -pg -DDEBUG
else
   CXXFLAGS=-Wall -std=c++11 -g -O3 
endif



#CXXFLAGS=-Wall -std=c++11 -g -pg

CC=g++

all: test test_logging_restore generate

test: test.cpp betree.hpp swap_space.o backing_store.o

test_logging_restore: test_logging_restore.cpp betree.hpp swap_space.o backing_store.o

generate: generate.cpp

swap_space.o: swap_space.cpp swap_space.hpp backing_store.hpp

backing_store.o: backing_store.hpp backing_store.cpp

clean_tmpdir:
	$(RM) tmpdir/* tmpdir_backup/* test.logg ss_objects.txt loggingFileStatus.txt

clean:
	$(RM) *.o test test_logging_restore generate test.logg ss_objects.txt loggingFileStatus.txt tmpdir/* tmpdir_backup/*


