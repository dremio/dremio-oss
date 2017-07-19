<#--

    Copyright (C) 2017 Dremio Corporation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
/**
 * Parses a Dremio compound identifier.
 */
SqlIdentifier CompoundIdentifier() :
{
    CompoundIdentifier.Builder builder = CompoundIdentifier.newBuilder();
    String p;
    int index;
}
{
    p = Identifier()
    {
        builder.addString(p, false, getPos());
    }
    (
        (
        <DOT>
        (
            p = Identifier() {
                builder.addString(p, false, getPos());
            }
        |
            <STAR> {
                builder.addString("*", true, getPos());
            }
        )
        )
        |
        (
          <LBRACKET>
          index = UnsignedIntLiteral()
          <RBRACKET> 
          {
              builder.addIndex(index, getPos());
          }
        )
    ) *
    {
        return builder.build();
    }
}


/**
 * Parses a comma-separated list of compound identifiers.
 */
void CompoundIdentifierCommaList(List<SqlNode> list) :
{
    SqlIdentifier id;
}
{
    id = CompoundIdentifier() {list.add(id);}
    (<COMMA> id = CompoundIdentifier() {list.add(id);}) *
}

/**
  * List of compound identifiers in parentheses. The position extends from the
  * open parenthesis to the close parenthesis.
  */
SqlNodeList ParenthesizedCompoundIdentifierList() :
{
    SqlParserPos pos;
    List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { pos = getPos(); }
    CompoundIdentifierCommaList(list)
    <RPAREN>
    {
        return new SqlNodeList(list, pos.plus(getPos()));
    }
}
