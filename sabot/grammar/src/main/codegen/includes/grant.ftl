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
* GRANT priv1 [,...] ON entity [entityId]
* [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue ]
* TO granteeType grantee
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
  SqlGrantCatalog.Grant grantCatalog = null;
  SqlIdentifier entity;
  SqlIdentifier grantee;
  SqlLiteral granteeType;
  ReferenceType refType = null;
  SqlIdentifier refValue = null;
  boolean isGrantOnCatalog = true;
  boolean isScript = false;
  boolean isGrantOnAllDatasets = false;
  boolean isGrantOnAllFolders = false;
  boolean isGrantOnReference = false;
  boolean isGrantOnProjectEntities = false;
  boolean isGrantCatalog = false;
}
{
  PrivilegeCommaList(privilegeList.getList())

  <ON>
    (
      (<SYSTEM> | <PROJECT>) {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.PROJECT, getPos()));
        isGrantOnCatalog = false;
        entity = null;
      }
      |
      (
        (<PDS> | <TABLE>) {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.PDS, getPos()));
          entity = CompoundIdentifier();
        }
        [
          <AT> {
            refType = ParseReferenceType();
            refValue = SimpleIdentifier();
          }
        ]
      )
      |
      (
        (<VDS> | <VIEW>) {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.VDS, getPos()));
          entity = CompoundIdentifier();
        }
        [
          <AT> {
            refType = ParseReferenceType();
            refValue = SimpleIdentifier();
          }
        ]
      )
      |
      (
        <FUNCTION> {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FUNCTION, getPos()));
          entity = CompoundIdentifier();
        }
        [
          <AT> {
            refType = ParseReferenceType();
            refValue = SimpleIdentifier();
          }
        ]
      )
      |
      (
        <FOLDER> {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
          entity = CompoundIdentifier();
        }
        [
          <AT> {
            refType = ParseReferenceType();
            refValue = SimpleIdentifier();
          }
        ]
      )
      |
      <SCHEMA> {
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
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <CATALOG> {
        grantCatalog = new SqlGrantCatalog.Grant(SqlLiteral.createSymbol(SqlGrantCatalog.GrantType.CATALOG, getPos()));
        entity = SimpleIdentifier();
        isGrantCatalog = true;
      }
      |
      <CLOUD> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.CLOUD, getPos()));
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <ENGINE> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.ENGINE, getPos()));
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <IDENTITY> <PROVIDER> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.IDENTITY_PROVIDER, getPos()));
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <OAUTH> <APPLICATION> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.OAUTH_APPLICATION, getPos()));
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <EXTERNAL> <TOKENS> <PROVIDER> {
        grantOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.EXTERNAL_TOKENS_PROVIDER, getPos()));
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <SCRIPT> {
        pos = getPos();
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = false;
        isGrantOnCatalog = false;
        isScript = true;
      }
      |
      <ALL> <FOLDERS> <IN>
      (
        (
          <CATALOG> {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.CATALOG, getPos()));
          }
          |
          <FOLDER> {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
          }
        ) {
          entity = CompoundIdentifier();
          isGrantOnAllFolders  = true;
        }
      )
      |
      <ALL> <DATASETS> <IN>
      (
        (
          (<FOLDER> | <SCHEMA>) {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
          }
          |
          <SOURCE> {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SOURCE, getPos()));
          }
          |
          <SPACE> {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SPACE, getPos()));
          }
          |
          <CATALOG> {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.CATALOG, getPos()));
          }
        ) {
          entity = CompoundIdentifier();
          isGrantOnAllDatasets  = true;
        }
      )
      |
      (
        <BRANCH> refValue = SimpleIdentifier()
        <IN> <CATALOG> {
          entity = SimpleIdentifier();
          refType = ReferenceType.BRANCH;
          isGrantOnReference = true;
        }
      )
      |
      (
        <TAG> refValue = SimpleIdentifier()
        <IN> <CATALOG> {
          entity = SimpleIdentifier();
          refType = ReferenceType.TAG;
          isGrantOnReference = true;
        }
      )
    )
  <TO>
    granteeType = ParseGranteeType()
    grantee = SimpleIdentifier()
    {
      if (isGrantCatalog) {
        return new SqlGrantCatalog(pos, privilegeList, grantCatalog.getType(), entity, granteeType, grantee);
      }
      if (isGrantOnProjectEntities) {
        return new SqlGrantOnProjectEntities(pos, privilegeList, grantOnProjectEntities.getType(), entity, granteeType, grantee);
      }
      if (isGrantOnAllDatasets) {
        return new SqlGrantOnAllDatasets(pos, privilegeList, grant.getType(), entity, granteeType, grantee);
      }
      if (isGrantOnAllFolders) {
        return new SqlGrantOnAllFolders(pos, privilegeList, grant.getType(), entity, granteeType, grantee);
      }
      if (isGrantOnReference) {
        return new SqlGrantOnReference(pos, privilegeList, refType, refValue, entity, granteeType, grantee);
      }
      if (isGrantOnCatalog) {
        return new SqlGrantOnCatalog(pos, privilegeList, grant.getType(), entity, granteeType, grantee, refType, refValue);
      }
      if (isScript) {
        return new SqlGrantOnScript(pos, privilegeList, entity, granteeType, grantee);
      }

      return new SqlGrant(pos, privilegeList, grant.getType(), grantee, granteeType);
    }
}

ReferenceType ParseReferenceType() :
{
  ReferenceType refType = null;
}
{
  (
    <REF> { refType = ReferenceType.REFERENCE; }
    |
    <REFERENCE> { refType = ReferenceType.REFERENCE; }
    |
    <BRANCH> { refType = ReferenceType.BRANCH; }
    |
    <TAG> { refType = ReferenceType.TAG; }
    |
    <COMMIT> { refType = ReferenceType.COMMIT; }
  )
  { return refType; }
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
    <READ> <METADATA>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.READ_METADATA, getPos())); }
    |
    <VIEW> <REFLECTION>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.VIEW_REFLECTION, getPos())); }
    |
    <VIEW>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnScript.Privilege.VIEW, getPos())); }
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
    <CREATE> <VIEW>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.CREATE_VIEW, getPos())); }
    |
    <CREATE> <FOLDER>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.CREATE_FOLDER, getPos())); }
    |
    <CREATE> <FUNCTION>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.CREATE_FUNCTION, getPos())); }
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
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.USAGE, getPos())); }
    |
    <CREATE> <CLOUD>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.CREATE_CLOUD, getPos())); }
    |
    <CREATE> <PROJECT>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.CREATE_PROJECT, getPos())); }
    |
    <CREATE> <CATALOG>
    { list.add(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.Privilege.CREATE_CATALOG, getPos())); }
    |
    <CREATE> <BRANCH>
    { list.add(SqlLiteral.createSymbol(SqlGrantCatalog.Privilege.CREATE_BRANCH, getPos())); }
    |
    <CREATE> <TAG>
    { list.add(SqlLiteral.createSymbol(SqlGrantCatalog.Privilege.CREATE_TAG, getPos())); }
    |
    <COMMIT>
    { list.add(SqlLiteral.createSymbol(SqlGrantCatalog.Privilege.COMMIT, getPos())); }
    |
    <MODIFY>
    { list.add(SqlLiteral.createSymbol(SqlGrantCatalog.Privilege.MODIFY, getPos())); }
    |
    <CONFIGURE> <SECURITY>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.CONFIGURE_SECURITY, getPos())); }
    |
    <INSERT>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.INSERT, getPos())); }
    |
    <TRUNCATE>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.TRUNCATE, getPos())); }
    |
    <DELETE>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.DELETE, getPos())); }
    |
    <UPDATE>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.UPDATE, getPos())); }
    |
    <CREATE> <USER>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.CREATE_USER, getPos())); }
    |
    <CREATE> <ROLE>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.CREATE_ROLE, getPos())); }
    |
    <EXECUTE>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.EXECUTE, getPos())); }
    |
    <CREATE> <SOURCE>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.CREATE_SOURCE, getPos())); }
    |
    <UPLOAD>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.UPLOAD_FILE, getPos())); }
    |
    <WRITE>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.WRITE, getPos())); }
    |
    <SHOW>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.SHOW, getPos())); }
    |
    <EXPORT> <DIAGNOSTICS>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.EXPORT_DIAGNOSTICS, getPos())); }
    |
    <ALL>
    { list.add(SqlLiteral.createSymbol(SqlGrant.Privilege.ALL, getPos())); }
  )
}

/**
* REVOKE priv1 [,...] ON object FROM
* [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue ]
* FROM granteeType grantee
*/
SqlNode SqlRevoke() :
{
  SqlParserPos pos;
  SqlNodeList privilegeList = new SqlNodeList(getPos());
  SqlGrant.Grant grant = null;
  SqlGrantOnProjectEntities.Grant revokeOnProjectEntities = null;
  SqlGrantCatalog.Grant revokeCatalog = null;
  SqlIdentifier entity;
  SqlIdentifier grantee;
  SqlLiteral granteeType;
  ReferenceType refType = null;
  SqlIdentifier refValue = null;
  boolean isGrantOnCatalog = true;
  boolean isRevokeOnAllDatasets = false;
  boolean isRevokeOnAllFolders = false;
  boolean isRevokeOnReference = false;
  boolean isGrantOnProjectEntities = false;
  boolean isScript = false;
  boolean isGrantCatalog = false;
}
{
  <REVOKE> { pos = getPos(); }
    PrivilegeCommaList(privilegeList.getList())
  <ON>
    (
      (<SYSTEM> | <PROJECT>) {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.PROJECT, getPos()));
        isGrantOnCatalog = false;
        entity = null;
      }
      |
      (
        (<PDS> | <TABLE>) {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.PDS, getPos()));
          entity = CompoundIdentifier();
        }
        [
          <AT> {
            refType = ParseReferenceType();
            refValue = SimpleIdentifier();
          }
        ]
      )
      |
      (
        (<VDS> | <VIEW>) {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.VDS, getPos()));
          entity = CompoundIdentifier();
        }
        [
          <AT> {
            refType = ParseReferenceType();
            refValue = SimpleIdentifier();
          }
        ]
      )
      |
      (
        <FUNCTION> {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FUNCTION, getPos()));
          entity = CompoundIdentifier();
        }
        [
          <AT> {
            refType = ParseReferenceType();
            refValue = SimpleIdentifier();
          }
        ]
      )
      |
      (
        <FOLDER> {
          grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
          entity = CompoundIdentifier();
        }
        [
          <AT> {
            refType = ParseReferenceType();
            refValue = SimpleIdentifier();
          }
        ]
      )
      |
      <SCHEMA> {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
        entity = CompoundIdentifier();
      }
      |
      <SOURCE> {
        grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SOURCE, getPos()));
        entity = SimpleIdentifier();
      }
      |
      <CATALOG> {
        revokeCatalog = new SqlGrantCatalog.Grant(SqlLiteral.createSymbol(SqlGrantCatalog.GrantType.CATALOG, getPos()));
        entity = SimpleIdentifier();
        isGrantCatalog = true;
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
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <CLOUD> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.CLOUD, getPos()));
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <ENGINE> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.ENGINE, getPos()));
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <IDENTITY> <PROVIDER> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.IDENTITY_PROVIDER, getPos()));
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <OAUTH> <APPLICATION> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.OAUTH_APPLICATION, getPos()));
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <EXTERNAL> <TOKENS> <PROVIDER> {
        revokeOnProjectEntities = new SqlGrantOnProjectEntities.Grant(SqlLiteral.createSymbol(SqlGrantOnProjectEntities.GrantType.EXTERNAL_TOKENS_PROVIDER, getPos()));
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = true;
        isGrantOnCatalog = false;
      }
      |
      <SCRIPT> {
        pos = getPos();
        entity = SimpleIdentifier();
        isGrantOnProjectEntities = false;
        isGrantOnCatalog = false;
        isScript = true;
      }
      |
      <ALL> <FOLDERS> <IN>
      (
        (
          <CATALOG> {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.CATALOG, getPos()));
          }
          |
          <FOLDER> {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
          }
        ) {
          entity = CompoundIdentifier();
          isRevokeOnAllFolders  = true;
        }
      )
      |
      <ALL> <DATASETS> <IN>
      (
        (
          (<FOLDER> | <SCHEMA>) {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.FOLDER, getPos()));
          }
          |
          <SOURCE> {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SOURCE, getPos()));
          }
          |
          <SPACE> {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.SPACE, getPos()));
          }
          |
          <CATALOG> {
            grant = new SqlGrant.Grant(SqlLiteral.createSymbol(SqlGrant.GrantType.CATALOG, getPos()));
          }
        ) {
          entity = CompoundIdentifier();
          isRevokeOnAllDatasets  = true;
        }
      )
      |
      (
        <BRANCH> refValue = SimpleIdentifier()
        <IN> <CATALOG> {
          entity = SimpleIdentifier();
          refType = ReferenceType.BRANCH;
          isRevokeOnReference = true;
        }
      )
      |
      (
        <TAG> refValue = SimpleIdentifier()
        <IN> <CATALOG> {
          entity = SimpleIdentifier();
          refType = ReferenceType.TAG;
          isRevokeOnReference = true;
        }
      )
    )
  <FROM>
    granteeType = ParseGranteeType()
    grantee = SimpleIdentifier()
    {
      if (isGrantCatalog) {
        return new SqlRevokeCatalog(pos, privilegeList, revokeCatalog.getType(), entity, granteeType, grantee);
      }
      if (isGrantOnProjectEntities) {
        return new SqlRevokeOnProjectEntities(pos, privilegeList, revokeOnProjectEntities.getType(), entity, granteeType, grantee);
      }
      if (isRevokeOnAllDatasets) {
        return new SqlRevokeOnAllDatasets(pos, privilegeList, grant.getType(), entity, granteeType, grantee);
      }
      if (isRevokeOnAllFolders) {
        return new SqlRevokeOnAllFolders(pos, privilegeList, grant.getType(), entity, granteeType, grantee);
      }
      if (isRevokeOnReference) {
        return new SqlRevokeOnReference(pos, privilegeList, refType, refValue, entity, granteeType, grantee);
      }
      if (isGrantOnCatalog) {
        return new SqlRevokeOnCatalog(pos, privilegeList, grant.getType(), entity, granteeType, grantee, refType, refValue);
      }
      if (isScript) {
        return new SqlRevokeOnScript(pos, privilegeList, entity, granteeType, grantee);
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
  ReferenceType refType = null;
  SqlIdentifier refValue = null;
  SqlGrantCatalogOwnership.Grant grantCatalog = null;
  boolean isGrantOnCatalog = false;
}
{
  <ON>
  (
    <USER> {
      grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.USER, getPos()));
      entity = SimpleIdentifier();
    }
    |
    (
      (<PDS> | <TABLE>) {
        grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.PDS, getPos()));
        entity = CompoundIdentifier();
      }
      [
        <AT> {
          refType = ParseReferenceType();
          refValue = SimpleIdentifier();
        }
      ]
    )
    |
    (
      (<VDS> | <VIEW>) {
        grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.VDS, getPos()));
        entity = CompoundIdentifier();
      }
      [
        <AT> {
          refType = ParseReferenceType();
          refValue = SimpleIdentifier();
        }
      ]
    )
    |
    (
      (<FUNCTION>) {
        grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.FUNCTION, getPos()));
        entity = CompoundIdentifier();
      }
      [
        <AT> {
          refType = ParseReferenceType();
          refValue = SimpleIdentifier();
        }
      ]
    )
    |
    (
      (<FOLDER> | <SCHEMA>) {
        grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.FOLDER, getPos()));
        entity = CompoundIdentifier();
      }
      [
        <AT> {
          refType = ParseReferenceType();
          refValue = SimpleIdentifier();
        }
      ]
    )
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
    <CATALOG> {
      grantCatalog = new SqlGrantCatalogOwnership.Grant(SqlLiteral.createSymbol(SqlGrantCatalogOwnership.GrantType.CATALOG, getPos()));
      entity = SimpleIdentifier();
      isGrantOnCatalog = true;
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
    |
    (
      <BRANCH> refValue = SimpleIdentifier()
      <IN> <CATALOG> {
        grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.BRANCH, getPos()));
        entity = SimpleIdentifier();
        refType = ReferenceType.BRANCH;
      }
    )
    |
    (
      <TAG> refValue = SimpleIdentifier()
      <IN> <CATALOG> {
        grant = new SqlGrantOwnership.Grant(SqlLiteral.createSymbol(SqlGrantOwnership.GrantType.TAG, getPos()));
        entity = SimpleIdentifier();
        refType = ReferenceType.TAG;
      }
    )
  )
  <TO>
    granteeType = ParseGranteeType()
    grantee = SimpleIdentifier()
    {
      if (isGrantOnCatalog) {
        return new SqlGrantCatalogOwnership(pos, entity, grantCatalog.getType(), grantee, granteeType);
      }
      return new SqlGrantOwnership(pos, entity, grant.getType(), grantee, granteeType, refType, refValue);
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
