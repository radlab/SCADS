# do a little platform detection
UNAME=$(shell uname)
ARCH=$(shell uname -m)
DIDINC=no

# Linux
ifeq ($(UNAME), Linux)
ifeq ($(ARCH), x86_64)
include ./makefile.inc/linux_x64
DIDINC=yes
endif
ifeq ($(ARCH), i386)
include ./makefile.inc/linux_i386
DIDINC=yes
endif
ifeq ($(ARCH), i686)
include ./makefile.inc/linux_i686
DIDINC=yes
endif
endif

#MACOS
ifeq ($(UNAME), Darwin)
ifeq ($(ARCH), i386)
include ./makefile.inc/osx_i386
DIDINC=yes
endif
endif

# Generic thrift macros
THRIFT = thrift
THRIFT_FLAGS = --gen cpp
THRIFT_FILE = ../../storage.thrift

# Source Listings
SRCS_TO_GEN = gen-cpp/storage_constants.cpp gen-cpp/Storage.h gen-cpp/storage_types.h gen-cpp/storage_constants.h gen-cpp/Storage_server.skeleton.cpp gen-cpp/Storage.cpp gen-cpp/storage_types.cpp
GEN_SRC = gen-cpp/storage_types.cpp gen-cpp/Storage.cpp

# C/C++ MACROS
CXX=g++
CFLAGS = -I${THRIFT_DIR} -I${BOOST_DIR} -Igen-cpp ${BDB_DIR}
LIBS = ${LIB_DIRS} -lthrift -lthriftnb -levent -l${BDB_LIB} -l${RUBY_LIB}

default: arch_warning storage.bdb

debug: arch_warning storage.bdb.debug

#print warning if couldn't determine the arch
arch_warning:
ifneq ($(DIDINC), yes)
	@read -p "WARNING no include file for $(UNAME) $(ARCH).  Your build will probably fail. (Enter to continue)" dummyvariable;
endif

# THRIFT RULES
$(SRCS_TO_GEN): $(THRIFT_FILE)
	$(THRIFT) $(THRIFT_FLAGS) $(THRIFT_FILE)

# C/C++ RULES
storage.bdb: $(SRCS_TO_GEN) StorageDB.cpp Network.cpp
	$(CXX) $(CFLAGS) $(RUBY_INCLUDES) -o $@ StorageDB.cpp Network.cpp ${GEN_SRC} $(LIBS)

storage.bdb.debug: $(SRCS_TO_GEN) StorageDB.cpp Network.cpp
	$(CXX) -g $(CFLAGS) $(RUBY_INCLUDES) -DDEBUG -o storage.bdb StorageDB.cpp Network.cpp ${GEN_SRC} $(LIBS)

clean:
	rm -rf gen-cpp storage.bdb