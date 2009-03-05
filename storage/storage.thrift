#!/usr/local/bin/thrift --gen rb

namespace ruby SCADS

typedef string RecordKey 
typedef binary RecordValue
typedef string NameSpace
typedef string Host

struct Record {
	1: RecordKey key,
	2: RecordValue value
}

enum RecordSetType {
	ALL = 1,
	NONE = 2
	RANGE = 3,
	LIST = 4,
	KEY_FUNC = 5,
	KEY_VALUE_FUNC = 6
}

struct RangeSet {
	1: RecordKey start_key,
	2: RecordKey end_key,
	3: i32 start_limit
	4: i32 end_limit
}

enum Language {
	RUBY = 1,
}

struct UserFunction {
	1: Language lang
	2: string func
}

struct RecordSet {
	1: RecordSetType type,
	2: RangeSet range,
	3: UserFunction func
}

enum ConflictPolicyType {
	GREATER = 1,
	FUNC = 2,
}

struct ConflictPolicy {
	1: ConflictPolicyType type,
	2: UserFunction func
}

exception NotResponsible {
	1:RecordKey key,
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
	
	bool set_responsibility_policy(1:NameSpace ns, 2:RecordSet policy) throws (1: NotImplemented ni, 2: InvalidSetDescription bs),
	RecordSet get_responsibility_policy(1:NameSpace ns),
	
	bool sync_set(1:NameSpace ns, 2:RecordSet rs, 3:Host h, 4:ConflictPolicy policy) throws (1: NotImplemented ni),
	bool copy_set(1:NameSpace ns, 2:RecordSet rs, 3:Host h) throws (1: NotImplemented ni),
	bool remove_set(1:NameSpace ns, 2:RecordSet rs) throws (1: NotImplemented ni),
}