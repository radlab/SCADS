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
STORAGE_SRCS = StorageDB.cpp MerkleDB.cpp Network.cpp TQueue.h

# C/C++ MACROS
CXX=g++
CFLAGS = -I${THRIFT_DIR} -I${BOOST_DIR} -Igen-cpp ${BDB_DIR}
LIBS = ${LIB_DIRS} -lthrift -lthriftnb -levent -l${BDB_LIB} -l${RUBY_LIB} -lmhash

# deb files
DEB = scads-storage-bdb_0.1.deb
DEBCTRL64 = debian/control.amd64
DEBCTRL386 = debian/control.i386
DEBDIR = gendeb

default: arch_warning storage.bdb

debug: arch_warning storage.bdb.debug

noxtrace: arch_warning storage.bdb.noxtrace

noxtraced: arch_warning storage.bdb.noxtrace.debug

debugquiet: arch_warning storage.bdb.debugquiet

#print warning if couldn't determine the arch
arch_warning:
ifneq ($(DIDINC), yes)
	@read -p "WARNING no include file for $(UNAME) $(ARCH).  Your build will probably fail. (Enter to continue)" dummyvariable;
endif

# THRIFT RULES
$(SRCS_TO_GEN): $(THRIFT_FILE)
	$(THRIFT) $(THRIFT_FLAGS) $(THRIFT_FILE)

# C/C++ RULES
storage.bdb: $(SRCS_TO_GEN) $(STORAGE_SRCS)
	$(CXX) $(CFLAGS) $(RUBY_INCLUDES) -o $@ $(STORAGE_SRCS) ${GEN_SRC} $(LIBS)

storage.bdb.noxtrace: $(SRCS_TO_GEN) $(STORAGE_SRCS)
	$(CXX) $(CFLAGS) $(RUBY_INCLUDES) -o storage.bdb $(STORAGE_SRCS) ${GEN_SRC} $(LIBS) -DNOXTRACE

storage.bdb.noxtrace.debug: $(SRCS_TO_GEN) $(STORAGE_SRCS)
	$(CXX) $(CFLAGS) $(RUBY_INCLUDES) -g -o storage.bdb $(STORAGE_SRCS) ${GEN_SRC} $(LIBS) -DNOXTRACE

storage.bdb.debug: $(SRCS_TO_GEN) $(STORAGE_SRCS)
	$(CXX) -g $(CFLAGS) $(RUBY_INCLUDES) -DDEBUG=DEBUG -o storage.bdb $(STORAGE_SRCS) ${GEN_SRC} $(LIBS)

storage.bdb.debugquiet: $(SRCS_TO_GEN) $(STORAGE_SRCS)
	$(CXX) -g $(CFLAGS) $(RUBY_INCLUDES) -o storage.bdb $(STORAGE_SRCS) ${GEN_SRC} $(LIBS)

merkle: $(SRCS_TO_GEN) MerkleDB.cpp
	$(CXX) $(CFLAGS) -g -DMERKLETEST -o MerkleDB MerkleDB.cpp $(LIBS)

tq: TQueue.cpp
	$(CXX) $(CFLAGS) -g -DDEBUG -o TQueue TQueue.cpp $(LIBS)

merklebench: $(SRCS_TO_GEN) MerkleDB.cpp MerkleBench.cpp
	$(CXX) $(CFLAGS) -g -o MerkleBench MerkleDB.cpp MerkleBench.cpp $(LIBS)

builddb: BuildDB.cpp MerkleDB.cpp
	$(CXX) $(CFLAGS) -g -o buildDB BuildDB.cpp MerkleDB.cpp $(LIBS)

catdb: CatDB.cpp
	$(CXX) $(CFLAGS) -g -o catDB CatDB.cpp $(LIBS)


# other rules

debamd64: storage.bdb $(DEBCTRL64)
	rm -rf $(DEBDIR)
	mkdir -p $(DEBDIR)/opt/scads/storage/engines/bdb
	mkdir -p $(DEBDIR)/DEBIAN
	cp storage.bdb $(DEBDIR)/opt/scads/storage/engines/bdb
	cp $(DEBCTRL64) $(DEBDIR)/DEBIAN/control
	dpkg -b $(DEBDIR) $(DEB)
	rm -rf $(DEBDIR)

debi386: storage.bdb $(DEBCTRL386)
	rm -rf $(DEBDIR)
	mkdir -p $(DEBDIR)/opt/scads/storage/engines/bdb
	mkdir -p $(DEBDIR)/DEBIAN
	cp storage.bdb $(DEBDIR)/opt/scads/storage/engines/bdb
	cp $(DEBCTRL386) $(DEBDIR)/DEBIAN/control
	dpkg -b $(DEBDIR) $(DEB)
	rm -rf $(DEBDIR)

clean:
	rm -rf gen-cpp storage.bdb MerkleDB MerkleBench $(DEB)
