# Makefile for bdb storage layer

# THRIFT MACROS
THRIFT_DIR = /usr/local/include/thrift
GEN_SRC = ../gen-cpp/storage_types.cpp ../gen-cpp/Storage.cpp

BOOST_DIR = /usr/local/boost/include/boost-1_33_1/


# C/C++ MACROS
CXX=g++

CFLAGS = -I${THRIFT_DIR} -I${BOOST_DIR} -I../gen-cpp

LIBS = -L/usr/local/lib -lthrift -ldb-4.7

default: client

# C/C++ RULES
client: Client.cpp
	$(CXX) $(CFLAGS) -o $@ Client.cpp ${GEN_SRC} $(LIBS)

clean:
	rm client
