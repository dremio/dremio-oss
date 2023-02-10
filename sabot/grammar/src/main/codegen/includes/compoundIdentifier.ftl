<#--

    Copyright (C) 2017-2019 Dremio Corporation

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
    SqlIdentifier id;
    int index;
}
{
    id = SimpleIdentifier()
    {
        builder.addString(id.names.get(0), false, id.getComponentParserPosition(0));
    }
    (
        (
        <DOT>
        (
            id = SimpleIdentifier() {
                builder.addString(id.names.get(0), false, id.getComponentParserPosition(0));
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
void CompoundIdentifierTypeCommaList(List<SqlNode> list, List<SqlNode> extendList) :
{
}
{
    CompoundIdentifierType(list, extendList)
    (<COMMA> CompoundIdentifierType(list, extendList))*
}

/**
  * List of compound identifiers in parentheses. The position extends from the
  * open parenthesis to the close parenthesis.
  */
Pair<SqlNodeList, SqlNodeList> ParenthesizedCompoundIdentifierList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    final List<SqlNode> extendList = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    CompoundIdentifierTypeCommaList(list, extendList)
    <RPAREN> {
        return Pair.of(new SqlNodeList(list, s.end(this)), new SqlNodeList(extendList, s.end(this)));
    }
}
