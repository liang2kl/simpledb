grammar Sql; // Use Sql instead of SQL to avoid name conflict.

EqualOrAssign: '=';
Less: '<';
LessEqual: '<=';
Greater: '>';
GreaterEqual: '>=';
NotEqual: '<>';

Count: 'COUNT';
Average: 'AVG';
Max: 'MAX';
Min: 'MIN';
Sum: 'SUM';
Null: 'NULL';

WhereNot: 'NOT';

Identifier: [a-zA-Z_] [a-zA-Z_0-9]*;
Integer: [0-9]+;
String: '\'' (~'\'')* '\'';
Float: ('-')? [0-9]+ '.' [0-9]*;
Whitespace: [ \t\n\r]+ -> skip;
Annotation: '-' '-' (~';')+;

program: statement* EOF;

statement:
	db_statement ';'
	| io_statement ';'
	| table_statement ';'
	| alter_statement ';'
	| Annotation ';'
	| Null ';';

db_statement:
	'CREATE' 'DATABASE' Identifier	# create_db
	| 'DROP' 'DATABASE' Identifier	# drop_db
	| 'SHOW' 'DATABASES'			# show_dbs
	| 'USE' Identifier						# use_db
	| 'SHOW' 'TABLES'						# show_tables
	| 'SHOW' 'INDEXES' 'FROM' Identifier	# show_indexes;

io_statement:
	'LOAD' 'FROM' 'FILE' String 'TO' 'TABLE' Identifier		# load_data
	| 'DUMP' 'TO' 'FILE' String 'FROM' 'TABLE' Identifier	# dump_data;

table_statement:
	'CREATE' 'TABLE' Identifier '(' field_list ')'			# create_table
	| 'DROP' 'TABLE' Identifier								# drop_table
	| 'DESC' Identifier										# describe_table
	| 'INSERT' 'INTO' Identifier 'VALUES' insert_value_list	# insert_into_table
	| 'DELETE' 'FROM' Identifier 'WHERE' where_and_clause			# delete_from_table
	| 'UPDATE' Identifier 'SET' set_clause 'WHERE' where_and_clause	# update_table
	| select_table													# select_table_;

select_table:
	'SELECT' selectors 'FROM' identifiers (
		'WHERE' where_and_clause
	)? ('GROUP' 'BY' column)? (
		'LIMIT' Integer ('OFFSET' Integer)?
	)?;

alter_statement:
	'ALTER' 'TABLE' Identifier 'ADD' 'INDEX' '(' identifiers ')'	# alter_add_index
	| 'ALTER' 'TABLE' Identifier 'DROP' 'INDEX' '(' identifiers ')'	# alter_drop_index
	| 'ALTER' 'TABLE' Identifier 'DROP' 'PRIMARY' 'KEY' (
		Identifier

	)?																					# alter_table_drop_pk
	| 'ALTER' 'TABLE' Identifier 'DROP' 'FOREIGN' 'KEY' Identifier						# alter_table_drop_foreign_key
	| 'ALTER' 'TABLE' Identifier 'ADD' 'CONSTRAINT' 'PRIMARY' 'KEY' '(' Identifier ')'	#
		alter_table_add_pk
	| 'ALTER' 'TABLE' Identifier 'ADD' 'CONSTRAINT' (Identifier)? 'FOREIGN' 'KEY' '(' identifiers
		')' 'REFERENCES' Identifier '(' identifiers ')'				# alter_table_add_foreign_key
	| 'ALTER' 'TABLE' Identifier 'ADD' 'UNIQUE' '(' identifiers ')'	# alter_table_add_unique;

field_list: field (',' field)*;

field:
	Identifier type_ ('NOT' Null)? ('DEFAULT' value)?	# normal_field
	| 'PRIMARY' 'KEY' '(' Identifier ')'				# primary_key_field
	| 'FOREIGN' 'KEY' (Identifier)? '(' identifiers ')' 'REFERENCES' Identifier '(' identifiers ')'	#
		foreign_key_field;

type_: 'INT' | 'VARCHAR' '(' Integer ')' | 'FLOAT';

// value_lists: value_list (',' value_list)*;

insert_value: value | 'DEFAULT';

insert_value_list: '(' insert_value (',' insert_value)* ')';

value_list: '(' value (',' value)* ')';

value: Integer | String | Float | Null;

where_and_clause: where_clause ('AND' where_clause)*;

where_clause:
	column operator_ expression				# where_operator_expression
	| column operator_ '(' select_table ')'	# where_operator_select
	| column 'IS' (WhereNot)? Null			# where_null
	| column 'IN' value_list				# where_in_list
	| column 'IN' '(' select_table ')'		# where_in_select
	| column 'LIKE' String					# where_like_string;

column: (Identifier '.')? Identifier;

expression: value | column;

set_clause:
	Identifier EqualOrAssign value (
		',' Identifier EqualOrAssign value
	)*;

selectors: '*' | selector (',' selector)*;

selector:
	column
	| aggregator '(' column ')'
	| Count '(' '*' ')';

identifiers: Identifier (',' Identifier)*;

operator_:
	EqualOrAssign
	| Less
	| LessEqual
	| Greater
	| GreaterEqual
	| NotEqual;

aggregator: Count | Average | Max | Min | Sum;
