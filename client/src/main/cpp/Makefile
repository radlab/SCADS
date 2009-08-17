# Makefile for client

# do a little platform detection
UNAME=$(shell uname)
ARCH=$(shell uname -m)
DIDINC=no

# Linux
ifeq ($(UNAME), Linux)
ifeq ($(ARCH), x86_64)
include ./makefile.inc.linux_x64
DIDINC=yes
endif
ifeq ($(ARCH), i386)
include ./makefile.inc.linux_i386
DIDINC=yes
endif
ifeq ($(ARCH), i686)
include ./makefile.inc.linux_i686
DIDINC=yes
endif
endif

#MACOS
ifeq ($(UNAME), Darwin)
ifeq ($(ARCH), i386)
include ./makefile.inc.osx_i386
DIDINC=yes
endif
endif


# Generic thrift macros
THRIFT = thrift
THRIFT_FLAGS = --gen cpp
THRIFT_FILE = ../../../../interface/scads.thrift

# Source Listings
SRCS_TO_GEN = gen-cpp/KeyStore.cpp gen-cpp/KeyStore.h gen-cpp/KeyStore_server.skeleton.cpp gen-cpp/scads_constants.cpp gen-cpp/scads_constants.h gen-cpp/scads_types.cpp gen-cpp/scads_types.h gen-cpp/StorageEngine.cpp gen-cpp/StorageEngine.h gen-cpp/StorageEngine_server.skeleton.cpp
GEN_SRC = gen-cpp/KeyStore.cpp gen-cpp/scads_constants.cpp gen-cpp/scads_types.cpp gen-cpp/StorageEngine.cpp

# C/C++ MACROS
CXX=g++
CFLAGS = -g -I${THRIFT_DIR} -I${BOOST_DIR} -Igen-cpp
LIBS = ${LIB_DIRS} -lthrift -lreadline

default: arch_warning client

#print warning if couldn't determine the arch
arch_warning:
ifneq ($(DIDINC), yes)
	@read -p "WARNING no include file for $(UNAME) $(ARCH).  Your build will probably fail. (Enter to continue)" dummyvariable;
endif

# THRIFT RULES
$(SRCS_TO_GEN): $(THRIFT_FILE)
	$(THRIFT) $(THRIFT_FLAGS) $(THRIFT_FILE)

# C/C++ RULES
client: $(SRCS_TO_GEN) client.cpp
	$(CXX) $(CFLAGS) -o $@ client.cpp ${GEN_SRC} $(LIBS)

clean:
	rm -rf gen-cpp client
