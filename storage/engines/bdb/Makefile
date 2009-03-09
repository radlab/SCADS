include ./makefile.inc

# Generic thrift macros
THRIFT = thrift
THRIFT_FLAGS = --gen cpp
THRIFT_FILE = ../../storage.thrift

# Source Listings
SRCS_TO_GEN = gen-cpp/storage_constants.cpp gen-cpp/Storage.h gen-cpp/storage_types.h gen-cpp/storage_constants.h gen-cpp/Storage_server.skeleton.cpp gen-cpp/Storage.cpp gen-cpp/storage_types.cpp
GEN_SRC = gen-cpp/storage_types.cpp gen-cpp/Storage.cpp


# C/C++ MACROS
CXX=g++
CFLAGS = -I${THRIFT_DIR} -I${BOOST_DIR} -Igen-cpp ${BDB}
LIBS = ${LIB_DIRS} -lthrift -ldb-4.7

default: storage.bdb

# THRIFT RULES
$(SRCS_TO_GEN): $(THRIFT_FILE)
	$(THRIFT) $(THRIFT_FLAGS) $(THRIFT_FILE)

# C/C++ RULES
storage.bdb: $(SRCS_TO_GEN) StorageDB.cpp
	$(CXX) $(CFLAGS) -g -o $@ StorageDB.cpp ${GEN_SRC} $(LIBS)

clean:
	rm -rf gen-cpp storage.bdb