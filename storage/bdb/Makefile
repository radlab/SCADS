# Makefile for bdb storage layer

# THRIFT MACROS
THRIFT = thrift
THRIFT_FLAGS = --gen cpp
THRIFT_FILE = ../storage.thrift
THRIFT_DIR = /usr/local/include/thrift
SRCS_TO_GEN = gen-cpp/storage_constants.cpp gen-cpp/Storage.h gen-cpp/storage_types.h gen-cpp/storage_constants.h gen-cpp/Storage_server.skeleton.cpp gen-cpp/Storage.cpp gen-cpp/storage_types.cpp
GEN_SRC = gen-cpp/storage_types.cpp gen-cpp/Storage.cpp

BOOST_DIR = /usr/local/boost/include/boost-1_33_1/


# C/C++ MACROS
CXX=g++

CFLAGS = -I${THRIFT_DIR} -I${BOOST_DIR} -Igen-cpp

LIBS = -L/usr/local/lib -lthrift -ldb-4.7

default: storage.bdb

# THRIFT RULES
$(SRCS_TO_GEN): $(THRIFT_FILE)
	$(THRIFT) $(THRIFT_FLAGS) $(THRIFT_FILE)

# C/C++ RULES
storage.bdb: $(SRCS_TO_GEN) StorageDB.cpp
	$(CXX) $(CFLAGS) -o $@ StorageDB.cpp ${GEN_SRC} $(LIBS)

clean:
	rm -rf gen-cpp storage.bdb