grammar vql;


var_prop: VAR('.'VAR)?;

var_props: var_prop(','var_prop)*;

int_range: INT '-' INT;

property_filter: ('id:' INT) |('tag:'  int_range );

vertex_filter: '{' property_filter '}';

vertex: '(' VAR (vertex_filter)? ')';

fixed_length: '[' INT? ']';

variable_length: '[' INT? '..' INT ']';

length: fixed_length | variable_length;

shortest: 's';

forward_edge: '-' length shortest? '->' | '->';

backward_edge: '<-' length shortest? '-' | '<-';

undirected_edge: '-' length shortest? '-' | '-';

edge: forward_edge | backward_edge | undirected_edge;

path_append: edge vertex;

pattern: vertex path_append*;

patterns: pattern (',' pattern)*;

count_only: 'count of';

query: 'vmatch' patterns 'return' count_only?  var_props ';';

// Lexer rules
INT: [0-9]+;

WS: [ \t\r\n]+ -> skip; // skip spaces, tabs, newlines

VAR: [a-zA-Z_] [a-zA-Z_0-9]*;






