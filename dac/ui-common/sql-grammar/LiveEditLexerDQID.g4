lexer grammar LiveEditLexerDQID;

// This rule is here just to make ANTLR not complain about no rules in the default mode
NEVER_MATCH : 'NEVER_MATCH' {false}?  ;

mode DQID;
/** Overrides DremioLexer QUOTED_IDENTIFIER to allow empty double quotes */
QUOTED_IDENTIFIER : '"' ((~["\n\r] | '""'))* '"'  ;
