#!/usr/local/bin/thrift --gen rb --gen java --gen cpp

namespace rb SCADS
namespace cpp SCADS
namespace java edu.berkeley.cs.scads.thrift

typedef string RecordKey 
typedef string RecordValue
typedef string NameSpace
typedef string Host

struct Record {
	1: RecordKey key,
	2: optional RecordValue value
}

struct ExistingValue {
  1: optional RecordValue value,
	2: optional i32 prefix
}

enum RecordSetType {
	RST_ALL = 1,
	RST_NONE = 2
	RST_RANGE = 3,
	RST_LIST = 4,
	RST_KEY_FUNC = 5,
	RST_KEY_VALUE_FUNC = 6
	RST_FILTER = 7
}

struct RangeSet {
	1: optional RecordKey start_key,
	2: optional RecordKey end_key,
	3: optional i32 offset,
	4: optional i32 limit
}

enum Language {
	LANG_RUBY = 1,
}

struct UserFunction {
	1: Language lang
	2: string func
}

struct RecordSet {
	1: RecordSetType type,
	2: optional RangeSet range,
	3: optional UserFunction func,
	4: optional string filter
}

enum ConflictPolicyType {
	CPT_GREATER = 1,
	CPT_FUNC = 2,
}

struct ConflictPolicy {
	1: ConflictPolicyType type,
	2: UserFunction func
}

exception NotResponsible {
	2:RecordSet policy
}

exception NotImplemented {
	1: string function_name
}

exception InvalidSetDescription {
	1: RecordSet s,
	2: string info
}

exception TestAndSetFailure {
  1: RecordValue currentValue
}

struct DataPlacement {
	1:Host node,
	2:i32 thriftPort,
	3:i32 syncPort,
	4:RecordSet rset
}

service DataPlacementServer {
	list<DataPlacement> lookup_namespace(1:NameSpace ns),
	DataPlacement lookup_node(1: NameSpace ns, 2:Host node 3: i32 thriftPort, 4: i32 syncPort),
	list<DataPlacement> lookup_key(1: NameSpace ns, 2:RecordKey key),
	list<DataPlacement> lookup_range(1: NameSpace ns, 2:RangeSet rset)
}

service KnobbedDataPlacementServer extends DataPlacementServer {
	void move(1: NameSpace ns, 2: RecordSet rset, 3: Host src_host, 4: i32 	src_thrift, 5: i32 src_sync,6: Host dest_host, 7: i32 dest_thrift, 8: i32 dest_sync),
	void copy(1: NameSpace ns, 2: RecordSet rset, 3: Host src_host, 4: i32 	src_thrift, 5: i32 src_sync,6: Host dest_host, 7: i32 dest_thrift, 8: i32 dest_sync),
	bool add(1:NameSpace ns, 2:list<DataPlacement> entries),
	bool remove(1:NameSpace ns, 2:list<DataPlacement> entries)
}

service KeyStore {
	Record get(1:NameSpace ns, 2:RecordKey key) throws (1: NotResponsible nr),
	list<Record> get_set(1: NameSpace ns, 2:RecordSet rs) throws (1: InvalidSetDescription bs, 2: NotImplemented ni, 3: NotResponsible nr),
	bool put(1:NameSpace ns, 2: Record rec) throws (1: NotResponsible nr, 2:InvalidSetDescription bs),
	bool test_and_set(1:NameSpace ns, 2:Record rec, 3:ExistingValue existingValue) throws (1: TestAndSetFailure tsf),
	i32 count_set(1:NameSpace ns, 2: RecordSet rs) throws (1: InvalidSetDescription bs, 2: NotImplemented ni, 3: NotResponsible nr)
}

service StorageEngine extends KeyStore {
	bool set_responsibility_policy(1:NameSpace ns, 2:list<RecordSet> policy) throws (1: NotImplemented ni, 2: InvalidSetDescription bs),
	list<RecordSet> get_responsibility_policy(1:NameSpace ns),
	
	bool sync_set(1:NameSpace ns, 2:RecordSet rs, 3:Host h, 4:ConflictPolicy policy) throws (1: NotImplemented ni),
	bool copy_set(1:NameSpace ns, 2:RecordSet rs, 3:Host h) throws (1: NotImplemented ni),
	bool remove_set(1:NameSpace ns, 2:RecordSet rs) throws (1: NotImplemented ni)
}
