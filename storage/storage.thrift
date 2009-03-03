#!/usr/local/bin/thrift --gen rb

typedef string RecordKey 
typedef string RecordValue
typedef string NameSpace
typedef string Host

struct Record {
	1: RecordKey key,
	2: RecordValue value
}

enum RecordSetType {
	RANGE = 1,
	RUBY_FUNCTION = 2,
	ALL = 3,
	NONE = 4
}

struct RangeSet {
	1: RecordKey start_key,
	2: RecordKey end_key,
	3: i32 limit
}

struct RubySet {
	1: string ruby_function
}

struct RecordSet {
	1: RecordSetType type,
	2: RangeSet range,
	3: RubySet ruby
}

enum ConflictPolicyType {
	RUBY_FUNCTION = 1,
	GREATER = 2
}

struct RubyConflictFunction {
	1: string ruby_function
}

struct ConflictPolicy {
	1: ConflictPolicyType type,
	2: RubyConflictFunction ruby
}

service Storage {
	Record get(1:NameSpace ns, 2:RecordKey key),
	list<Record> get_set(1: NameSpace ns, 2:RecordSet rs),
	bool put(1:NameSpace ns, 2: Record rec),
	
	bool install_responsibility_policy(1:NameSpace ns, 2:RecordSet policy),	
	bool install_conflict_policy(1:NameSpace ns, 2:ConflictPolicy policy),
	
	bool sync_set(1:NameSpace ns, 2:RecordSet rs, 3:Host h),
	bool copy_set(1:NameSpace ns, 2:RecordSet rs, 3:Host h),
	bool remove_set(1:NameSpace ns, 2:RecordSet rs)
}