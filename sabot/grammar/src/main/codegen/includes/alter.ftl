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
 * ALTER TABLE tblname CREATE ACCELERATION
 */
 SqlNode SqlAccel() :
{
    SqlParserPos pos;
    SqlIdentifier tblName;
    SqlIdentifier name;
    SqlIdentifier columnName;
    SqlLiteral deleteUnavail = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral promotion = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral forceUp = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral dropColumnKeywordPresent = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
    SqlLiteral raw = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
    SqlLiteral allFilesRefresh = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral fileRefresh = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral allPartitionsRefresh = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral partitionRefresh = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral enableSchemaLearning = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
    SqlNodeList filesList = SqlNodeList.EMPTY;
    SqlNodeList partitionList = SqlNodeList.EMPTY;
    SqlPartitionTransform partitionTransform;
    SqlPolicy sqlPolicy;
    SqlTableVersionSpec sqlTableVersionSpec = SqlTableVersionSpec.NOT_SPECIFIED ;
    SqlNodeList tablePropertyNameList;
    SqlNodeList tablePropertyValueList;
    SqlNodeList sortList;

}
{
    {
        tablePropertyNameList = SqlNodeList.EMPTY;
        tablePropertyValueList = SqlNodeList.EMPTY;
    }
    <ALTER> { pos = getPos(); }
    (
      <SOURCE>
      (
        tblName = SimpleIdentifier()
        (
          <REFRESH> <STATUS> {return new SqlRefreshSourceStatus(pos, tblName);}
        )
      )
      |
      <SPACE>
      (
        tblName = SimpleIdentifier()
        (
          <ROUTE> (
            (<ALL> <REFLECTIONS> | <REFLECTIONS>) { return SqlAlterDatasetReflectionRouting(pos, tblName, SqlLiteral.createSymbol(SqlAlterDatasetReflectionRouting.RoutingType.SPACE, pos)); }
          )
        )
      )
      |
      <FOLDER>
      (
        tblName = CompoundIdentifier()
        (
          <ROUTE> (
            (<ALL> <REFLECTIONS> | <REFLECTIONS>) { return SqlAlterDatasetReflectionRouting(pos, tblName, SqlLiteral.createSymbol(SqlAlterDatasetReflectionRouting.RoutingType.FOLDER, pos)); }
          )
        )
      )
      |
      (<TABLE> | <VDS> | <VIEW> | <PDS> | <DATASET>)
        tblName = CompoundIdentifier()
        [ sqlTableVersionSpec = ATVersionSpec() ]
          (
          <ADD> <ROW> <ACCESS> <POLICY> sqlPolicy = Policy()
          { return new SqlAlterTableAddRowAccessPolicy(pos, tblName, sqlPolicy); }
          |
          <DROP> <ROW> <ACCESS> <POLICY> sqlPolicy = Policy()
          { return new SqlAlterTableDropRowAccessPolicy(pos, tblName, sqlPolicy); }
          |
          <ADD> <PRIMARY> <KEY> { return new SqlAlterTableAddPrimaryKey(pos, tblName, ParseRequiredFieldList("Primary Key"), sqlTableVersionSpec); }
          |
          <DROP> <PRIMARY> <KEY> { return new SqlAlterTableDropPrimaryKey(pos, tblName, sqlTableVersionSpec); }
          |
          <ROUTE> (
            (<ALL> <REFLECTIONS> | <REFLECTIONS>)  { return SqlAlterDatasetReflectionRouting(pos, tblName, SqlLiteral.createSymbol(SqlAlterDatasetReflectionRouting.RoutingType.TABLE, pos)); }
          )
          |
          <SET> (
            <TBLPROPERTIES>
            <LPAREN>
                {
                    tablePropertyNameList = new SqlNodeList(getPos());
                    tablePropertyValueList = new SqlNodeList(getPos());
                }
                ParseTableProperty(tablePropertyNameList, tablePropertyValueList)
                (
                    <COMMA>
                    ParseTableProperty(tablePropertyNameList, tablePropertyValueList)
                )*
            <RPAREN>
            {
              return new SqlAlterTableProperties(pos, tblName,
                SqlLiteral.createSymbol(SqlAlterTableProperties.Mode.SET, pos), tablePropertyNameList, tablePropertyValueList);
            }
          )
          |
          <UNSET> (
            <TBLPROPERTIES>
            {
                tablePropertyNameList = new SqlNodeList(getPos());
            }
            <LPAREN>
                StringLiteralCommaList(tablePropertyNameList.getList())
            <RPAREN>
            {
              return new SqlAlterTableProperties(pos, tblName,
                SqlLiteral.createSymbol(SqlAlterTableProperties.Mode.UNSET, pos), tablePropertyNameList, tablePropertyValueList);
            }
          )
          |
          <ADD> (
            <COLUMNS> { return new SqlAlterTableAddColumns(pos, tblName, TableElementList(), sqlTableVersionSpec); }
            |
            <PARTITION> <FIELD> partitionTransform = ParsePartitionTransform()
            {
              return new SqlAlterTablePartitionColumns(pos, tblName,
                SqlLiteral.createSymbol(SqlAlterTablePartitionColumns.Mode.ADD, pos), partitionTransform, sqlTableVersionSpec);
            }
          )
          |
          (<CHANGE> | <ALTER> | <MODIFY>)
            [<COLUMN>]
            columnName = SimpleIdentifier() {
              SqlSetOption option;
              DremioSqlColumnDeclaration typedElement;
            }
            (
              typedElement = TypedElement() { return new SqlAlterTableChangeColumn(pos, tblName, columnName, typedElement, sqlTableVersionSpec); }
            |
              (
                <SET> <MASKING> <POLICY> sqlPolicy = Policy()
                { return new SqlAlterTableSetColumnMasking(pos, tblName, columnName, sqlPolicy); }
              )
            |
              (
                <UNSET> <MASKING> <POLICY>  sqlPolicy = PolicyWithoutArgs()
                { return new SqlAlterTableUnsetColumnMasking(pos, tblName, columnName, sqlPolicy); }
              )
            |
              { return new SqlAlterTableChangeColumnSetOption(pos, tblName, columnName, SqlSetOption(Span.of(), "COLUMN")); }
            )
          |
          <DROP> (
            <REFLECTION> {return SqlDropReflection(pos, tblName, sqlTableVersionSpec);}
            |
            <PARTITION> <FIELD> partitionTransform = ParsePartitionTransform()
            {
              return new SqlAlterTablePartitionColumns(pos, tblName,
                SqlLiteral.createSymbol(SqlAlterTablePartitionColumns.Mode.DROP, pos), partitionTransform, sqlTableVersionSpec);
            }
            |
            (
              <COLUMN>
              { dropColumnKeywordPresent = SqlLiteral.createBoolean(true, pos); }
            )?
            { return new SqlAlterTableDropColumn(pos, tblName, dropColumnKeywordPresent, SimpleIdentifier(), sqlTableVersionSpec); }
          )
          |
          <CREATE> (
            <AGGREGATE> <REFLECTION> name = SimpleIdentifier() {return SqlCreateAggReflection(pos, tblName, name, sqlTableVersionSpec);}
            |
            <RAW> <REFLECTION> name = SimpleIdentifier() {return SqlCreateRawReflection(pos, tblName, name, sqlTableVersionSpec);}
            |
            <EXTERNAL> <REFLECTION> name = SimpleIdentifier() { return SqlAddExternalReflection(pos, tblName, name);}
          )
          |
          <FORGET> <METADATA> {return new SqlForgetTable(pos, tblName);}
          |
          <REFRESH> <METADATA>
          (
            <FOR> <ALL>
            (
              <FILES> { allFilesRefresh = SqlLiteral.createBoolean(true, pos); }
            |
              <PARTITIONS> { allPartitionsRefresh = SqlLiteral.createBoolean(true, pos); }
            )
          |
            <FOR> <FILES> {
              fileRefresh = SqlLiteral.createBoolean(true, pos);
              filesList = ParseRequiredFilesList();
            }
          |
            <FOR> <PARTITIONS> {
              partitionRefresh = SqlLiteral.createBoolean(true, pos);
              partitionList = ParseRequiredPartitionList();
            }
          )?
          (
            <AUTO> <PROMOTION> { promotion = SqlLiteral.createBoolean(true, pos); }
          |
            <AVOID> <PROMOTION> { promotion = SqlLiteral.createBoolean(false, pos); }
          )?
          (
            <FORCE> <UPDATE> { forceUp = SqlLiteral.createBoolean(true, pos); }
          |
            <LAZY> <UPDATE> { forceUp = SqlLiteral.createBoolean(false, pos); }
          )?
          (
            <DELETE> <WHEN> <MISSING> { deleteUnavail = SqlLiteral.createBoolean(true, pos); }
          |
            <MAINTAIN> <WHEN> <MISSING> { deleteUnavail = SqlLiteral.createBoolean(false, pos); }
          )?
          { return new SqlRefreshTable(pos, tblName, deleteUnavail, forceUp, promotion, allFilesRefresh,
              allPartitionsRefresh, fileRefresh, partitionRefresh, filesList, partitionList); }
          |
          <ENABLE> (
            <SCHEMA> <LEARNING> { return new SqlAlterTableToggleSchemaLearning(pos, tblName, SqlLiteral.createBoolean(true, SqlParserPos.ZERO)); }
            |
            <APPROXIMATE> <STATS> {return new SqlSetApprox(pos, tblName, SqlLiteral.createBoolean(true, pos));}
            |
            (
                (
                  <RAW> { raw = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); }
                  |
                  <AGGREGATE> { raw = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); }
                )
                <ACCELERATION>
                {
                    return new SqlAccelToggle(pos, tblName, raw, SqlLiteral.createBoolean(true, SqlParserPos.ZERO), sqlTableVersionSpec);
                }
            )
           )
          |
          <DISABLE> (
            <SCHEMA> <LEARNING> { return new SqlAlterTableToggleSchemaLearning(pos, tblName, SqlLiteral.createBoolean(false, SqlParserPos.ZERO)); }
            |
            <APPROXIMATE> <STATS> {return new SqlSetApprox(pos, tblName, SqlLiteral.createBoolean(false, pos));}
            |
            (
                (
                  <RAW> { raw = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); }
                  |
                  <AGGREGATE> { raw = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); }
                )
                <ACCELERATION>
                {
                    return new SqlAccelToggle(pos, tblName, raw, SqlLiteral.createBoolean(false, SqlParserPos.ZERO), sqlTableVersionSpec);
                }
            )
           )
          |
          <LOCALSORT> <BY> { sortList = ParseRequiredFieldList("Sort"); }
          {
              return new SqlAlterTableSortOrder(pos, tblName, sortList);
          }
          |
          { return new SqlAlterTableSetOption(pos, tblName, SqlSetOption(Span.of(), "TABLE"), sqlTableVersionSpec); }
        )
    )
}

/** Parses a required list of files (SQLLiteral) and makes sure the list is not empty. */
SqlNodeList ParseRequiredFilesList() :
{
    SqlNodeList filesList = new SqlNodeList(getPos());
}
{
    <LPAREN>
    StringLiteralCommaList(filesList.getList())
    <RPAREN>
    {
        return filesList;
    }
}

void StringLiteralCommaList(List<SqlNode> list) :
{
    SqlNode literal;
}
{
    literal = StringLiteral() { list.add(literal); }
    (
        <COMMA> literal = StringLiteral() {
            list.add(literal);
        }
    )*
}

/** Parses a required list of partition key-values and mkes sure teh list is not empty */
SqlNodeList ParseRequiredPartitionList() :
{
    SqlNodeList partitionList = new SqlNodeList(getPos());
}
{
    <LPAREN>
    KeyValueCommaList(partitionList.getList())
    <RPAREN>
    {
        return partitionList;
    }
}


void KeyValueCommaList(List<SqlNode> list) :
{
    SqlNodeList pair;
}
{
    pair = KeyValuePair() { list.add(pair); }
    (
        <COMMA> pair = KeyValuePair() {
            list.add(pair);
        }
    )*
}

SqlNodeList KeyValuePair() :
{
    SqlNodeList pair = new SqlNodeList(getPos());
    SqlNode name;
    SqlNode value;
}
{
    name = SimpleIdentifier() { pair.add(name); }
    <EQ>
    (
      <NULL> {
          pair.add(SqlLiteral.createNull(getPos()));
          return pair;
      }
      | value = StringLiteral()
      {
          pair.add(value);
          return pair;
      }
    )
}

/**
   ALTER TABLE tblname
   ADD AGGREGATE REFLECTION name
   DIMENSIONS (field1, field2)
   MEASURES (field1, field2)
   [ DISTRIBUTE BY (field1, field2, ..) ]
   [ (STRIPED, CONSOLIDATED) PARTITION BY (field1, field2, ..) ]
   [ LOCALSORT BY (field1, field2, ..) ]
 */
SqlNode SqlCreateAggReflection(SqlParserPos pos, SqlIdentifier tblName, SqlIdentifier name, SqlTableVersionSpec sqlTableVersionSpec) :
{
    SqlNodeList dimensionList;
    SqlNodeList measureList;
    SqlNodeList partitionTransformList;
    SqlNodeList distributionList;
    SqlNodeList sortList;
    SqlLiteral arrowCachingEnabled;
    PartitionDistributionStrategy partitionDistributionStrategy;
}
{
    {
        dimensionList = SqlNodeList.EMPTY;
        measureList = SqlNodeList.EMPTY;
        distributionList = SqlNodeList.EMPTY;
        partitionTransformList =  SqlNodeList.EMPTY;
        sortList = SqlNodeList.EMPTY;
        arrowCachingEnabled = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
        partitionDistributionStrategy = PartitionDistributionStrategy.UNSPECIFIED;
    }
    <USING>
    (   <DIMENSIONS>
        dimensionList = ParseFieldListWithGranularity("Dimensions")
    )?
    (   <MEASURES>
        measureList = ParseFieldListWithMeasures("Measures")
    )?

    {
      if (dimensionList.size() == 0 && measureList.size() == 0) {
        throw new ParseException("Both Dimensions and Measures cannot be empty.");
      }
    }

    (   <DISTRIBUTE> <BY>
        distributionList = ParseRequiredFieldList("Distribution")
    )?
    (
        (
            <STRIPED> {
                partitionDistributionStrategy = PartitionDistributionStrategy.STRIPED;
            }
        |
            <CONSOLIDATED> {
                // system makes a choice
                partitionDistributionStrategy = PartitionDistributionStrategy.UNSPECIFIED;
            }
        )?
        <PARTITION> <BY>
        partitionTransformList = ParsePartitionTransformList()
    )?
    (   <LOCALSORT> <BY>
        sortList = ParseRequiredFieldList("Sort")
    )?
    (   <ARROW> <CACHE>
        { arrowCachingEnabled = SqlLiteral.createBoolean(true, pos); }
    )?
    {
        return SqlCreateReflection.createAggregation(pos, tblName, dimensionList, measureList, distributionList,
          partitionTransformList, sortList, arrowCachingEnabled, partitionDistributionStrategy, name, sqlTableVersionSpec);
    }
}


/** Parses a field list and makes sure no field is a "*". */
SqlNodeList ParseFieldListWithGranularity(String relType) :
{
    SqlNodeList fieldList = new SqlNodeList(getPos());
}
{
    (<LPAREN>
    (SimpleIdentifierCommaListWithGranularity(fieldList.getList()))?
    <RPAREN>
    )?
    {
        for(SqlNode node : fieldList)
        {
            if (((SqlIdentifier)node).isStar())
                throw new ParseException(String.format("%s's field list has a '*', which is invalid.", relType));
        }
        return fieldList;
    }
}

void SimpleIdentifierCommaListWithGranularity(List<SqlNode> list) :
{
    SqlIdentifier id;
    SqlLiteral byDay = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
    SqlParserPos pos;
}
{
    id = SimpleIdentifier() {pos = getPos();}
    (
      <BY> <DAY>
      { byDay = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);}
    )?
    {list.add(new IdentifierWithGranularity(id, byDay, pos));}

    (<COMMA> SimpleIdentifierCommaListWithGranularity(list)) *
}

/** Parses a field list and makes sure no field is a "*". */
SqlNodeList ParseFieldListWithMeasures(String relType) :
{
    SqlNodeList fieldList = new SqlNodeList(getPos());
}
{
    (<LPAREN>
    (SimpleIdentifierCommaListWithMeasures(fieldList.getList()))?
    <RPAREN>
    )?
    {
        for(SqlNode node : fieldList)
        {
            if (((SqlIdentifier)node).isStar())
                throw new ParseException(String.format("%s's field list has a '*', which is invalid.", relType));
        }
        return fieldList;
    }
}

void SimpleIdentifierCommaListWithMeasures(List<SqlNode> list) :
{

    SqlIdentifier id;
    SqlNodeList measures;
    SqlParserPos pos;
}
{
    id = SimpleIdentifier() {pos = getPos(); measures = new SqlNodeList(getPos());}
    (
    <LPAREN>
    MeasureList(measures.getList())
    <RPAREN>
    )?
    {list.add(new IdentifierWithMeasures(id, measures, getPos()));}

    (<COMMA> SimpleIdentifierCommaListWithMeasures(list)) *
}

void MeasureList(List<SqlNode> measures) :
{}
{
        (
        <MIN>  { measures.add(SqlLiteral.createSymbol(MeasureType.MIN, getPos()));}
        |
        <MAX>  { measures.add(SqlLiteral.createSymbol(MeasureType.MAX, getPos()));}
        |
        <COUNT>  { measures.add(SqlLiteral.createSymbol(MeasureType.COUNT, getPos()));}
        |
        <SUM>  { measures.add(SqlLiteral.createSymbol(MeasureType.SUM, getPos()));}
        |
        (<APPROXIMATE> | <APPROX>) <COUNT> <DISTINCT>  { measures.add(SqlLiteral.createSymbol(MeasureType.APPROX_COUNT_DISTINCT, getPos()));}
        )

        (<COMMA> MeasureList(measures)) *
}


/**
   ALTER TABLE tblname
   [AT (BRANCH | TAG | COMMIT | SNAPSHOT | TIMESTAMP (versionSpec)]
   ADD RAW REFLECTION name
   USING
   DISPLAY (field1, field2)
   [ DISTRIBUTE BY (field1, field2, ..) ]
   [ (STRIPED, CONSOLIDATED) PARTITION BY (field1, field2, ..) ]
   [ LOCALSORT BY (field1, field2, ..) ]
 */
SqlNode SqlCreateRawReflection(SqlParserPos pos, SqlIdentifier tblName, SqlIdentifier name, SqlTableVersionSpec sqlTableVersionSpec) :
{
    SqlNodeList displayList;
    SqlNodeList distributionList;
    SqlNodeList partitionTransformList;
    SqlNodeList sortList;
    SqlLiteral arrowCachingEnabled;
    PartitionDistributionStrategy partitionDistributionStrategy;
}
{
    {
        displayList = SqlNodeList.EMPTY;
        distributionList = SqlNodeList.EMPTY;
        partitionTransformList =  SqlNodeList.EMPTY;
        sortList = SqlNodeList.EMPTY;
        arrowCachingEnabled = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
        partitionDistributionStrategy = PartitionDistributionStrategy.UNSPECIFIED;
    }
    <USING>
    <DISPLAY>
    displayList = ParseOptionalFieldList("Display")

    (   <DISTRIBUTE> <BY>
        distributionList = ParseRequiredFieldList("Distribution")
    )?
    (
        (
            <STRIPED> {
                partitionDistributionStrategy = PartitionDistributionStrategy.STRIPED;
            }
        |
            <CONSOLIDATED> {
                // system makes a choice
                partitionDistributionStrategy = PartitionDistributionStrategy.UNSPECIFIED;
            }
        )?
        <PARTITION> <BY>
        partitionTransformList = ParsePartitionTransformList()
    )?
    (   <LOCALSORT> <BY>
        sortList = ParseRequiredFieldList("Sort")
    )?
    (   <ARROW> <CACHE>
        { arrowCachingEnabled = SqlLiteral.createBoolean(true, pos); }
    )?
    {
        return SqlCreateReflection.createRaw(pos, tblName, displayList, distributionList, partitionTransformList, sortList,
          arrowCachingEnabled, partitionDistributionStrategy, name, sqlTableVersionSpec);
    }
}

/**
 * ALTER TABLE tblname DROP REFLECTION [string reflection id]
 */
 SqlNode SqlDropReflection(SqlParserPos pos, SqlIdentifier tblName, SqlTableVersionSpec sqlTableVersionSpec) :
{
    SqlIdentifier reflectionId;
}
{
    { reflectionId = SimpleIdentifier(); }
    {
        return new SqlDropReflection(pos, tblName, reflectionId, sqlTableVersionSpec);
    }
}

/**
 * ALTER TABLE tblname CREATE EXTERNAL REFLECTION name USING target
 */
 SqlNode SqlAddExternalReflection(SqlParserPos pos, SqlIdentifier tblName, SqlIdentifier name) :
{
    SqlIdentifier target;
}
{
    <USING> { target = CompoundIdentifier(); }
    {
        return new SqlAddExternalReflection(pos, tblName, name, target);
    }
}

/**
 * ALTER TABLE tblname ROUTE ALL REFLECTIONS TO QUEUE queuename
 */
SqlNode SqlAlterDatasetReflectionRouting(SqlParserPos pos, SqlIdentifier tblName, SqlLiteral type) :
{
  SqlLiteral isDefault;
  SqlIdentifier queueOrEngineName;
  SqlLiteral isQueue;
}
{
  {
    isDefault = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
    isQueue = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
  }
  <TO>
  (
    <DEFAULT_> { isDefault = SqlLiteral.createBoolean(true, pos);}
  )?
  (
    <QUEUE> { isQueue = SqlLiteral.createBoolean(true, pos); }
    |
    <ENGINE>
  )
  {
    if (isDefault.booleanValue())
      return new SqlAlterDatasetReflectionRouting(pos, tblName, isDefault, isQueue, null, type);
  }
  queueOrEngineName = SimpleIdentifier()
  {
    return new SqlAlterDatasetReflectionRouting(pos, tblName, isDefault, isQueue, queueOrEngineName, type);
  }
}

SqlPolicy Policy() :
{
    SqlIdentifier name;
    SqlNodeList columns = SqlNodeList.EMPTY;
}
{
  name = CompoundIdentifier()
  columns = ParseColumns()
  {
    return new SqlPolicy(getPos(), name, columns);
  }
}

SqlPolicy PolicyWithoutArgs() :
{
    SqlIdentifier name;
    SqlNodeList columns = SqlNodeList.EMPTY;
}
{
  name = CompoundIdentifier()
  {
    return new SqlPolicy(getPos(), name, SqlNodeList.EMPTY);
  }
}

SqlNodeList ParseColumns() :
 {
     SqlNodeList columnList = new SqlNodeList(getPos());
 }
 {
     <LPAREN>
     IdentifierCommaList(columnList.getList())
     <RPAREN>
     {
         return columnList;
     }
 }

 void IdentifierCommaList(List<SqlNode> list) :
 {
     SqlNode literal;
 }
 {
     literal = SimpleIdentifier() { list.add(literal); }
     (
         <COMMA> literal = SimpleIdentifier() {
             list.add(literal);
         }
     )*
 }

DremioSqlColumnDeclaration TypedElement() :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final Span s = Span.of();
}
{
    id = SimpleIdentifier()
    type = DataType()
    nullable = NullableOptDefaultTrue()
    {
        return new DremioSqlColumnDeclaration(s.add(id).end(this), new SqlColumnPolicyPair(id.getParserPosition(), id, null),
                new SqlComplexDataTypeSpec(type.withNullable(nullable)), null);
    }
}

/**
 * ALTER CLEAR PLAN CACHE
 */
 SqlNode SqlAlterClearPlanCache() :
{
    final Span s;
    SqlParserPos pos;
    final String scope;
}
{
    <ALTER> { pos = getPos(); s = span(); }
    scope = Scope()
    (
        <CLEAR> <PLAN> <CACHE>
        {
            return new SqlAlterClearPlanCache(pos, scope);
        }
        |
        {
        return SqlSetOption(s, scope);
        }
    )
}
