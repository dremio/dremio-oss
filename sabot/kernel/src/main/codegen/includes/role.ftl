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
* CREATE ROLE roleToCreate
*/
SqlNode SqlCreateRole() :
{
  SqlParserPos pos;
  SqlIdentifier roleToCreate;
}
{
  <CREATE> { pos = getPos(); }
  <ROLE>
  roleToCreate = SimpleIdentifier()
  {
    return new SqlCreateRole(pos, roleToCreate);
  }
}

/**
* GRANT ROLE roleToGrant TO granteeType grantee
*/
SqlNode SqlGrantRole() :
{
  SqlParserPos pos;
  SqlIdentifier roleToGrant;
  SqlLiteral granteeType;
  SqlIdentifier grantee;
}
{
  <GRANT> { pos = getPos(); }
  <ROLE>
  roleToGrant = SimpleIdentifier()
  <TO>
  granteeType = ParseGranteeType()
  grantee = SimpleIdentifier()
  {
    return new SqlGrantRole(pos, roleToGrant, granteeType, grantee);
  }
}

/**
* REVOKE ROLE roleToRevoke FROM revokeeType revokee
*/
SqlNode SqlRevokeRole() :
{
  SqlParserPos pos;
  SqlIdentifier roleToRevoke;
  SqlLiteral revokeeType;
  SqlIdentifier revokee;
}
{
  <REVOKE> { pos = getPos(); }
  <ROLE>
  roleToRevoke = SimpleIdentifier()
  <FROM>
  revokeeType = ParseGranteeType()
  revokee = SimpleIdentifier()
  {
    return new SqlRevokeRole(pos, roleToRevoke, revokeeType, revokee);
  }
}

/**
* DROP ROLE roleToDrop
*/
SqlNode SqlDropRole() :
{
  SqlParserPos pos;
  SqlIdentifier roleToDrop;
}
{
  <DROP> { pos = getPos(); }
  <ROLE>
  roleToDrop = SimpleIdentifier()
  {
    return new SqlDropRole(pos, roleToDrop);
  }
}
