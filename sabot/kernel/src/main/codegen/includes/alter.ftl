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
    SqlLiteral deleteUnavail = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral promotion = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlLiteral forceUp = SqlLiteral.createNull(SqlParserPos.ZERO);
}
{
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
      (<TABLE> | <VDS> | <PDS> | <DATASET>)
        tblName = CompoundIdentifier()
        (
          <DROP> <REFLECTION> {return SqlDropReflection(pos, tblName);}
          |
          <CREATE> (
            <AGGREGATE> <REFLECTION> name = SimpleIdentifier() {return SqlCreateAggReflection(pos, tblName, name);}
            |
            <RAW> <REFLECTION> name = SimpleIdentifier() {return SqlCreateRawReflection(pos, tblName, name);}
            |
            <EXTERNAL> <REFLECTION> name = SimpleIdentifier() { return SqlAddExternalReflection(pos, tblName, name);}
          )
          |
          <FORGET> <METADATA> {return new SqlForgetTable(pos, tblName);}
          |
          <REFRESH> <METADATA>
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
          { return new SqlRefreshTable(pos, tblName, deleteUnavail, forceUp, promotion); }
          |
          <ENABLE> (
            <APPROXIMATE> <STATS> {return new SqlSetApprox(pos, tblName, SqlLiteral.createBoolean(true, pos));}
            |
            <HIVE> <VARCHAR> <COMPATIBILITY> {return new SqlSetHiveVarcharCompatible(pos, tblName, SqlLiteral.createBoolean(true, pos));}
           )
          |
          <DISABLE> (
            <APPROXIMATE> <STATS> {return new SqlSetApprox(pos, tblName, SqlLiteral.createBoolean(false, pos));}
            |
            <HIVE> <VARCHAR> <COMPATIBILITY> {return new SqlSetHiveVarcharCompatible(pos, tblName, SqlLiteral.createBoolean(false, pos));}
           )
          |
          {return SqlEnableRaw(pos, tblName);}
        )
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
   [ PARTITION BY (field1, field2, ..) ]
   [ LOCALSORT BY (field1, field2, ..) ] 
 */
SqlNode SqlCreateAggReflection(SqlParserPos pos, SqlIdentifier tblName, SqlIdentifier name) :
{
    SqlNodeList dimensionList;
    SqlNodeList measureList;
    SqlNodeList partitionList;
    SqlNodeList distributionList;
    SqlNodeList sortList;
    PartitionDistributionStrategy partitionDistributionStrategy;
}
{
    {
        dimensionList = SqlNodeList.EMPTY;
        measureList = SqlNodeList.EMPTY;
        distributionList = SqlNodeList.EMPTY;
        partitionList =  SqlNodeList.EMPTY;
        sortList = SqlNodeList.EMPTY;
        partitionDistributionStrategy = PartitionDistributionStrategy.UNSPECIFIED;
    }
    <USING>
    <DIMENSIONS>
    dimensionList = ParseRequiredFieldListWithGranularity("Dimensions")
    <MEASURES>
    measureList = ParseRequiredFieldListWithMeasures("Measures")
    
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
        partitionList = ParseRequiredFieldList("Partition")
    )?
    (   <LOCALSORT> <BY>
        sortList = ParseRequiredFieldList("Sort")
    )?  
    {
        return SqlCreateReflection.createAggregation(pos, tblName, dimensionList, measureList, distributionList,
           partitionList, sortList, partitionDistributionStrategy, name);
    }
}


/** Parses a required field list and makes sure no field is a "*". */
SqlNodeList ParseRequiredFieldListWithGranularity(String relType) :
{
    SqlNodeList fieldList = new SqlNodeList(getPos());
}
{
    <LPAREN>
    SimpleIdentifierCommaListWithGranularity(fieldList.getList())
    <RPAREN>
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

/** Parses a required field list and makes sure no field is a "*". */
SqlNodeList ParseRequiredFieldListWithMeasures(String relType) :
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
   ADD RAW REFLECTION name
   USING
   DISPLAY (field1, field2)
   [ (STRIPED, CONSOLIDATED) PARTITION BY (field1, field2, ..) ]
   [ DISTRIBUTE BY (field1, field2, ..) ]
   [ LOCALSORT BY (field1, field2, ..) ]
 */
SqlNode SqlCreateRawReflection(SqlParserPos pos, SqlIdentifier tblName, SqlIdentifier name) :
{
    SqlNodeList displayList;
    SqlNodeList partitionList;
    SqlNodeList distributionList;
    SqlNodeList sortList;
    PartitionDistributionStrategy partitionDistributionStrategy;
}
{
    {
        displayList = SqlNodeList.EMPTY;
        distributionList = SqlNodeList.EMPTY;
        partitionList =  SqlNodeList.EMPTY;
        sortList = SqlNodeList.EMPTY;
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
        partitionList = ParseRequiredFieldList("Partition")
    )?
    (   <LOCALSORT> <BY>
        sortList = ParseRequiredFieldList("Sort")
    )?
    {
        return SqlCreateReflection.createRaw(pos, tblName, displayList, distributionList, partitionList, sortList,
          partitionDistributionStrategy, name);
    }
}

/**
 * ALTER TABLE tblname DROP REFLECTION [string reflection id]
 */
 SqlNode SqlDropReflection(SqlParserPos pos, SqlIdentifier tblName) :
{
    SqlIdentifier reflectionId;
}
{
    { reflectionId = SimpleIdentifier(); }
    {
        return new SqlDropReflection(pos, tblName, reflectionId);
    }
}

/**
 * ALTER TABLE tblname (ENABLE|DISABLE) (RAW|AGGREGATION) ACCELERATION
 */
 SqlNode SqlEnableRaw(SqlParserPos pos, SqlIdentifier tblName) :
{
    SqlLiteral raw;
    SqlLiteral enable;
}
{
    (
      <ENABLE> { enable = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); }
      | 
      <DISABLE> { enable = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); }
    ) 
    (
      <RAW> { raw = SqlLiteral.createBoolean(true, SqlParserPos.ZERO); }
      | 
      <AGGREGATE> { raw = SqlLiteral.createBoolean(false, SqlParserPos.ZERO); }
    )
    <ACCELERATION>
    {
        return new SqlAccelToggle(pos, tblName, raw, enable);
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