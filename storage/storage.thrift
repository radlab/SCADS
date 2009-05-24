#!/usr/local/bin/thrift --gen rb

namespace rb SCADS.Storage
namespace cpp SCADS
namespace java SCADS

typedef string RecordKey 
typedef binary RecordValue
typedef string NameSpace
typedef string Host

struct Record {
	1: RecordKey key,
	2: optional RecordValue value
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
	4: optional i32 limit,
	5: optional bool reverse
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

service Storage {
	Record get(1:NameSpace ns, 2:RecordKey key) throws (1: NotResponsible nr),
	list<Record> get_set(1: NameSpace ns, 2:RecordSet rs) throws (1: InvalidSetDescription bs, 2: NotImplemented ni, 3: NotResponsible nr),
	bool put(1:NameSpace ns, 2: Record rec) throws (1: NotResponsible nr, 2:InvalidSetDescription bs),
	i32 count_set(1:NameSpace ns, 2: RecordSet rs) throws (1: InvalidSetDescription bs, 2: NotImplemented ni, 3: NotResponsible nr),
	
	bool set_responsibility_policy(1:NameSpace ns, 2:RecordSet policy) throws (1: NotImplemented ni, 2: InvalidSetDescription bs),
	RecordSet get_responsibility_policy(1:NameSpace ns),
	
	bool sync_set(1:NameSpace ns, 2:RecordSet rs, 3:Host h, 4:ConflictPolicy policy) throws (1: NotImplemented ni),
	bool copy_set(1:NameSpace ns, 2:RecordSet rs, 3:Host h) throws (1: NotImplemented ni),
	bool remove_set(1:NameSpace ns, 2:RecordSet rs) throws (1: NotImplemented ni),
}
