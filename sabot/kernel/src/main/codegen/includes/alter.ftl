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
 * ALTER TABLE tblname CREATE ACCELERATION
 */
 SqlNode SqlAccel() :
{
    SqlParserPos pos;
    SqlIdentifier tblName;
}
{
    <ALTER> { pos = getPos(); }
    (<TABLE> | <VDS> | <PDS> | <DATASET>)
    tblName = CompoundIdentifier()
    (
      <CREATE> <ACCELERATION> {return new SqlAccelEnable(pos, tblName);}
      |
      <DROP> <ACCELERATION> {return new SqlAccelDisable(pos, tblName);}
      |
      <DROP> <LAYOUT> {return SqlDropLayout(pos, tblName);}
      |
      <ADD> (
        <AGGREGATE> <LAYOUT> {return SqlAddAggLayout(pos, tblName);}
        |
        <RAW> <LAYOUT> {return SqlAddRawLayout(pos, tblName);}
      )
      |
      <FORGET> <METADATA> {return new SqlForgetTable(pos, tblName);}
      |
      <REFRESH> <METADATA> {return new SqlRefreshTable(pos, tblName);}
      |
      {return SqlEnableRaw(pos, tblName);}
    )
}

/**
   ALTER TABLE tblname 
   ADD AGGREGATE LAYOUT 
   DIMENSIONS (field1, field2)
   MEASURES (field1, field2)
   [ DISTRIBUTE BY (field1, field2, ..) ]
   [ PARTITION BY (field1, field2, ..) ]
   [ LOCALSORT BY (field1, field2, ..) ] 
 */
SqlNode SqlAddAggLayout(SqlParserPos pos, SqlIdentifier tblName) :
{
    SqlNodeList dimensionList;
    SqlNodeList measureList;
    SqlNodeList partitionList;
    SqlNodeList distributionList;
    SqlNodeList sortList;
}
{
    {
        dimensionList = SqlNodeList.EMPTY;
        measureList = SqlNodeList.EMPTY;
        distributionList = SqlNodeList.EMPTY;
        partitionList =  SqlNodeList.EMPTY;
        sortList = SqlNodeList.EMPTY;
    }
    <DIMENSIONS>
    dimensionList = ParseRequiredFieldListWithGranularity("Dimensions")
    <MEASURES>
    measureList = ParseOptionalFieldList("Measures")
    
    (   <DISTRIBUTE> <BY>
        distributionList = ParseRequiredFieldList("Distribution")
    )?
    (   <PARTITION> <BY>
        partitionList = ParseRequiredFieldList("Partition")
    )?
    (   <LOCALSORT> <BY>
        sortList = ParseRequiredFieldList("Sort")
    )?    
    {
        return SqlAddLayout.createAggregation(pos, tblName, dimensionList, measureList, distributionList, partitionList, sortList);
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


/**
   ALTER TABLE tblname 
   ADD RAW LAYOUT 
   DISPLAY (field1, field2)
   [ PARTITION BY (field1, field2, ..) ]
   [ DISTRIBUTE BY (field1, field2, ..) ]
   [ LOCALSORT BY (field1, field2, ..) ] 
 */
SqlNode SqlAddRawLayout(SqlParserPos pos, SqlIdentifier tblName) :
{
    SqlNodeList displayList;
    SqlNodeList partitionList;
    SqlNodeList distributionList;
    SqlNodeList sortList;
}
{
    {
        displayList = SqlNodeList.EMPTY;
        distributionList = SqlNodeList.EMPTY;
        partitionList =  SqlNodeList.EMPTY;
        sortList = SqlNodeList.EMPTY;
    }
    <DISPLAY>
    displayList = ParseOptionalFieldList("Display")
    
    (   <DISTRIBUTE> <BY>
        distributionList = ParseRequiredFieldList("Distribution")
    )?
    (   <PARTITION> <BY>
        partitionList = ParseRequiredFieldList("Partition")
    )?
    (   <LOCALSORT> <BY>
        sortList = ParseRequiredFieldList("Sort")
    )?    
    {
        return SqlAddLayout.createRaw(pos, tblName, displayList, distributionList, partitionList, sortList);
    }
}

/**
 * ALTER TABLE tblname DROP LAYOUT [string layout id]
 */
 SqlNode SqlDropLayout(SqlParserPos pos, SqlIdentifier tblName) :
{
    SqlNode layoutId;
}
{
    { layoutId = StringLiteral(); }
    {
        return new SqlDropLayout(pos, tblName, layoutId);
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
