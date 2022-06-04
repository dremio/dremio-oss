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
* GRANT priv1 [,...] ON entity [entityId] TO granteeType grantee
*/
SqlNode SqlGrant() :
{
  SqlParserPos pos;
}
{
  <GRANT> { pos = getPos(); }
  (
    <OWNERSHIP>
    (
      {return SqlGrantOwnership(pos);}
    )
    |
    <ROLE>
    (
      {return SqlGrantRole(pos);}
    )
    |
    {
      return SqlGrantPrivilege(pos);
    }
  )
}

SqlNode SqlGrantPrivilege(SqlParserPos pos) :
{
  SqlNodeList privilegeList = new SqlNodeList(getPos());
  SqlGrant.Grant grant = null;
  SqlGrantOnProjectEntities.Grant grantOnProjectEntities = null;
  SqlIdentifier entity;
  SqlIdentifier grantee;
  SqlLiteral granteeType;
  boolean isCatalog = true;
  boolean isGrantOnAll = false;
  boolean isDCSEntity = false;
}
{
  PrivilegeCommaList(privilegeList.getList())

  <ON>
    (
      (<SYSTEM> | <PROJECT>) {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.PROJECT, getPos()));
        isCatalog = false;
        entity = null;
      }
      |
      (<PDS> | <TABLE>) {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.PDS, getPos()));
        entity = CompoundIdentifier();
      }
      |
      (<VDS> | <VIEW>) {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.VDS, getPos()));
        entity = CompoundIdentifier();
      }
      |
      (<FOLDER> | <SCHEMA>) {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
        entity = CompoundIdentifier();
      }
      |
      <SOURCE> {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SOURCE, getPos()));
        entity = SimpleIdentifier();
      }
      |
      <SPACE> {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SPACE, getPos()));
        entity = SimpleIdentifier();
      }
      |
      <ORG> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.ORG, getPos()));
        entity = null;
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <CLOUD> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.CLOUD, getPos()));
        entity = SimpleIdentifier();
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <ENGINE> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.ENGINE, getPos()));
        entity = SimpleIdentifier();
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <IDENTITY> <PROVIDER> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.IDENTITY_PROVIDER, getPos()));
        entity = SimpleIdentifier();
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <OAUTH> <APPLICATION> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.OAUTH_APPLICATION, getPos()));
        entity = SimpleIdentifier();
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <EXTERNAL> <TOKENS> <PROVIDER> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.EXTERNAL_TOKENS_PROVIDER, getPos()));
        entity = SimpleIdentifier();
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <ALL> <DATASETS> <IN>
      (
        (<FOLDER> | <SCHEMA>) {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
          entity = CompoundIdentifier();
          isGrantOnAll = true;
        }
        |
        <SOURCE> {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SOURCE, getPos()));
          entity = SimpleIdentifier();
          isGrantOnAll = true;
        }
        |
        <SPACE> {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SPACE, getPos()));
          entity = SimpleIdentifier();
          isGrantOnAll = true;
        }
      )
    )
  <TO>
    granteeType = ParseGranteeType()
    grantee = SimpleIdentifier()
    {
      if (isDCSEntity) {
        return new SqlGrantOnProjectEntities(pos, privilegeList, grantOnProjectEntities.getType(), entity, granteeType, grantee);
      }
      if (isGrantOnAll) {
        return new SqlGrantOnAllDatasets(pos, privilegeList, grant.getType(), entity, granteeType, grantee);
      }
      if (isCatalog) {
        return new SqlGrantOnCatalog(pos, privilegeList, grant.getType(), entity, granteeType, grantee);
      }

      return new SqlGrant(pos, privilegeList, grant.getType(), grantee, granteeType);
    }
}

SqlLiteral ParseGranteeType() :
{
  SqlLiteral granteeType;
}
{
  (
    <USER> {
      granteeType = SqlLiteral.createSymbol(SqlGrant.GranteeType.USER, getPos());
    }
    |
    <ROLE> {
      granteeType = SqlLiteral.createSymbol(SqlGrant.GranteeType.ROLE, getPos());
    }
  )
  { return granteeType; }
}

void PrivilegeCommaList(List<SqlNode> list) :
{
}
{
  (Privilege(list))

  (<COMMA> Privilege(list))*
  {
  }
}

void Privilege(List<SqlNode> list) :
{
}
{
  (
    <VIEW> <JOB> <HISTORY>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.VIEW_JOB_HISTORY, getPos())); }
    |
    <ALTER> <REFLECTION>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.ALTER_REFLECTION, getPos())); }
    |
    <ALTER>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.ALTER, getPos())); }
    |
    <SELECT>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.SELECT, getPos())); }
    |
    <VIEW> <REFLECTION>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.VIEW_REFLECTION, getPos())); }
    |
    <MODIFY>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.MODIFY, getPos())); }
    |
    <MANAGE> <GRANTS>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.MANAGE_GRANTS, getPos())); }
    |
    <CREATE> <TABLE>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.CREATE_TABLE, getPos())); }
    |
    <DROP>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.DROP, getPos())); }
    |
    <EXTERNAL> <QUERY>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.EXTERNAL_QUERY, getPos())); }
    |
    <OWNERSHIP>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.OWNERSHIP, getPos())); }
    |
    <MONITOR>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.MONITOR, getPos())); }
    |
    <OPERATE>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.OPERATE, getPos())); }
    |
    <USAGE>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.USAGE, getPos())); }
    |
    <CREATE> <CLOUD>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.CREATE_CLOUD, getPos())); }
    |
    <CREATE> <PROJECT>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.CREATE_PROJECT, getPos())); }
    |
    <CONFIGURE> <SECURITY>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.CONFIGURE_SECURITY, getPos())); }
    |
    <CREATE> <OAUTH> <APPLICATION>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.CREATE_OAUTH_APPLICATION, getPos())); }
    |
    <CREATE> <EXTERNAL> <TOKENS> <PROVIDER>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.CREATE_EXTERNAL_TOKENS_PROVIDER, getPos())); }
    |
    <ALL>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.ALL, getPos())); }
  )
}

/**
* REVOKE priv1 [,...] ON object FROM
*/
SqlNode SqlRevoke() :
{
  SqlParserPos pos;
  SqlNodeList privilegeList = new SqlNodeList(getPos());
  SqlGrant.Grant grant = null;
  SqlGrantOnProjectEntities.Grant revokeOnProjectEntities = null;
  SqlIdentifier entity;
  SqlIdentifier grantee;
  SqlLiteral granteeType;
  boolean isCatalog = true;
  boolean isGrantOnAll = false;
  boolean isDCSEntity = false;
}
{
  <REVOKE> { pos = getPos(); }
    PrivilegeCommaList(privilegeList.getList())
  <ON>
    (
      (<SYSTEM> | <PROJECT>) {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.PROJECT, getPos()));
        isCatalog = false;
        entity = null;
      }
      |
      (<PDS> | <TABLE>) {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.PDS, getPos()));
        entity = CompoundIdentifier();
      }
      |
      (<VDS> | <VIEW>) {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.VDS, getPos()));
        entity = CompoundIdentifier();
      }
      |
      (<FOLDER> | <SCHEMA>) {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
        entity = CompoundIdentifier();
      }
      |
      <SOURCE> {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SOURCE, getPos()));
        entity = SimpleIdentifier();
      }
      |
      <SPACE> {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SPACE, getPos()));
        entity = SimpleIdentifier();
      }
      |
      <ORG> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.ORG, getPos()));
        entity = null;
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <CLOUD> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.CLOUD, getPos()));
        entity = SimpleIdentifier();
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <ENGINE> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.ENGINE, getPos()));
        entity = SimpleIdentifier();
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <IDENTITY> <PROVIDER> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.IDENTITY_PROVIDER, getPos()));
        entity = SimpleIdentifier();
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <OAUTH> <APPLICATION> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.OAUTH_APPLICATION, getPos()));
        entity = SimpleIdentifier();
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <EXTERNAL> <TOKENS> <PROVIDER> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.EXTERNAL_TOKENS_PROVIDER, getPos()));
        entity = SimpleIdentifier();
        isDCSEntity = true;
        isCatalog = false;
      }
      |
      <ALL> <DATASETS> <IN>
      (
        (<FOLDER> | <SCHEMA>) {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
          entity = CompoundIdentifier();
          isGrantOnAll = true;
        }
        |
        <SOURCE> {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SOURCE, getPos()));
          entity = SimpleIdentifier();
          isGrantOnAll = true;
        }
        |
        <SPACE> {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SPACE, getPos()));
          entity = SimpleIdentifier();
          isGrantOnAll = true;
        }
      )
    )
  <FROM>
    granteeType = ParseGranteeType()
    grantee = SimpleIdentifier()
    {
      if (isDCSEntity) {
        return new SqlRevokeOnProjectEntities(pos, privilegeList, revokeOnProjectEntities.getType(), entity, granteeType, grantee);
      }
      if (isGrantOnAll) {
        return new SqlRevokeOnAllDatasets(pos, privilegeList, grant.getType(), entity, granteeType, grantee);
      }
      if (isCatalog) {
        return new SqlRevokeOnCatalog(pos, privilegeList, grant.getType(), entity, granteeType, grantee);
      }

      return new SqlRevoke(pos, privilegeList, grant.getType(), grantee, granteeType);
    }
}

SqlNode SqlGrantOwnership(SqlParserPos pos) :
{
  SqlNodeList privilegeList = new SqlNodeList(getPos());
  SqlGrantOwnership.Grant grant = null;
  SqlIdentifier entity;
  SqlIdentifier grantee;
  SqlLiteral granteeType;
}
{
  <ON>
  (
    <USER> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.USER, getPos()));
      entity = SimpleIdentifier();
    }
    |
    (<PDS> | <TABLE>) {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.PDS, getPos()));
      entity = CompoundIdentifier();
    }
    |
    (<VDS> | <VIEW>) {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.VDS, getPos()));
      entity = CompoundIdentifier();
    }
    |
    (<FOLDER> | <SCHEMA>) {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.FOLDER, getPos()));
      entity = CompoundIdentifier();
    }
    |
    <SPACE> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.SPACE, getPos()));
      entity = SimpleIdentifier();
    }
    |
    <SOURCE> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.SOURCE, getPos()));
      entity = SimpleIdentifier();
    }
    |
    <PROJECT> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.PROJECT, getPos()));
      entity = null;
    }
    |
    <ORG> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.ORG, getPos()));
      entity = null;
    }
    |
    <CLOUD> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.CLOUD, getPos()));
      entity = SimpleIdentifier();
    }
    |
    <ENGINE> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.ENGINE, getPos()));
      entity = SimpleIdentifier();
    }
    |
    <ROLE> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.ROLE, getPos()));
      entity = SimpleIdentifier();
    }
    |
    <IDENTITY> <PROVIDER> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.IDENTITY_PROVIDER, getPos()));
      entity = SimpleIdentifier();
    }
    |
    <OAUTH> <APPLICATION> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.OAUTH_APPLICATION, getPos()));
      entity = SimpleIdentifier();
    }
    |
    <EXTERNAL> <TOKENS> <PROVIDER> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.EXTERNAL_TOKENS_PROVIDER, getPos()));
      entity = SimpleIdentifier();
    }

  )
  <TO>
    granteeType = ParseGranteeType()
    grantee = SimpleIdentifier()
    {
      return new SqlGrantOwnership(pos, entity, grant.getType(), grantee, granteeType);
    }
}

/**
  * GRANT ROLE roleToGrant TO granteeType grantee
  */
SqlNode SqlGrantRole(SqlParserPos pos) :
{
  SqlIdentifier roleToGrant;
  SqlLiteral granteeType;
  SqlIdentifier grantee;
}
{
  roleToGrant = SimpleIdentifier()
  <TO>
  granteeType = ParseGranteeType()
  grantee = SimpleIdentifier()
  {
    return new SqlGrantRole(pos, roleToGrant, granteeType, grantee);
  }
}
