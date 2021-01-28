/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.deltalake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

import com.dremio.exec.record.BatchSchema;

/**
 * Tests for {@link DeltaLakeSchemaConverter}
 */
public class TestDeltaLakeSchemaConverter {

    @Test
    public void testSimpleTypes() throws IOException {
        /**
         * Spark commands, used for generation are as follows -
         *
         * val simpleSchema = new StructType().add("byteField", ByteType).add("shortField", ShortType).add("intField", IntegerType, false).add("longField", LongType, false).add("floatField", FloatType).add("doubleField", DoubleType).add("stringField", StringType).add("binaryField", BinaryType).add("booleanField", BooleanType).add("timestampField", TimestampType).add("dateField", DateType);
         * val df = spark.createDataFrame(sc.emptyRDD[Row], simpleSchema)
         * df.write.format("delta").mode("overwrite").save("/tmp/delta-tests");
         */
        final String schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
        /**
         * root
         *  |-- byteField: byte (nullable = true)
         *  |-- shortField: short (nullable = true)
         *  |-- intField: integer (nullable = false)
         *  |-- longField: long (nullable = false)
         *  |-- floatField: float (nullable = true)
         *  |-- doubleField: double (nullable = true)
         *  |-- stringField: string (nullable = true)
         *  |-- binaryField: binary (nullable = true)
         *  |-- booleanField: boolean (nullable = true)
         *  |-- timestampField: timestamp (nullable = true)
         *  |-- dateField: date (nullable = true)
         */
        final BatchSchema batchSchema = DeltaLakeSchemaConverter.fromSchemaString(schemaString);

        assertEquals(9, batchSchema.getFieldCount()); // Expected 12 when all types are supported
        ArrowType byteType = batchSchema.findField("byteField").getType();
        assertEquals(ArrowType.ArrowTypeID.Int, byteType.getTypeID());
        assertEquals(8, ((ArrowType.Int) byteType).getBitWidth());
        assertTrue(((ArrowType.Int) byteType).getIsSigned());
        assertTrue(batchSchema.findField("byteField").isNullable());

        ArrowType shortType = batchSchema.findField("shortField").getType();
        assertEquals(ArrowType.ArrowTypeID.Int, shortType.getTypeID());
        assertEquals(16, ((ArrowType.Int) shortType).getBitWidth());
        assertTrue(((ArrowType.Int) shortType).getIsSigned());
        assertTrue(batchSchema.findField("shortField").isNullable());

        ArrowType intType = batchSchema.findField("intField").getType();
        assertEquals(ArrowType.ArrowTypeID.Int, intType.getTypeID());
        assertEquals(32, ((ArrowType.Int) intType).getBitWidth());
        assertTrue(((ArrowType.Int) intType).getIsSigned());
        assertFalse(batchSchema.findField("intField").isNullable());

        ArrowType longType = batchSchema.findField("longField").getType();
        assertEquals(ArrowType.ArrowTypeID.Int, longType.getTypeID());
        assertEquals(64, ((ArrowType.Int) longType).getBitWidth());
        assertTrue(((ArrowType.Int) longType).getIsSigned());
        assertFalse(batchSchema.findField("longField").isNullable());

        ArrowType floatType = batchSchema.findField("floatField").getType();
        assertEquals(ArrowType.ArrowTypeID.FloatingPoint, floatType.getTypeID());
        assertEquals(FloatingPointPrecision.SINGLE, ((ArrowType.FloatingPoint) floatType).getPrecision());
        assertTrue(batchSchema.findField("floatField").isNullable());

        ArrowType doubleType = batchSchema.findField("doubleField").getType();
        assertEquals(ArrowType.ArrowTypeID.FloatingPoint, doubleType.getTypeID());
        assertEquals(FloatingPointPrecision.DOUBLE, ((ArrowType.FloatingPoint) doubleType).getPrecision());
        assertTrue(batchSchema.findField("doubleField").isNullable());

        assertEquals(ArrowType.ArrowTypeID.Utf8, batchSchema.findField("stringField").getType().getTypeID());
        assertTrue(batchSchema.findField("stringField").isNullable());

        assertEquals(ArrowType.ArrowTypeID.Binary, batchSchema.findField("binaryField").getType().getTypeID());
        assertTrue(batchSchema.findField("binaryField").isNullable());

        assertEquals(ArrowType.ArrowTypeID.Bool, batchSchema.findField("booleanField").getType().getTypeID());
        assertTrue(batchSchema.findField("booleanField").isNullable());

        // Update once these data types are supported
        assertFalse(batchSchema.findFieldIgnoreCase("timestampField").isPresent());
        assertFalse(batchSchema.findFieldIgnoreCase("dateField").isPresent());
    }

    @Test
    public void testComplexTypes() throws IOException {
        // simpleList, simpleStruct, mixedStruct, nestedList, nestedListStruct
        /**
         * Spark commands, used for generation are as follows -
         * val complexSchema = new StructType().add("simpleList", ArrayType(StringType)).add("simpleStruct", new StructType().add("child1String", StringType).add("child2Int", IntegerType)).add("mixedStruct", new StructType().add("child1String", StringType).add("child2Struct", new StructType().add("childL2String", StringType).add("childL2Int", IntegerType)).add("child3Arr", ArrayType(LongType))).add("nestedList", ArrayType(ArrayType(DoubleType))).add("nestedListStruct", ArrayType(new StructType().add("childL2Long", LongType).add("childL2Bin", BinaryType)))
         * val df = spark.createDataFrame(sc.emptyRDD[Row], complexSchema)
         * df.write.format("delta").mode("overwrite").save("/tmp/delta-tests1");
         */
        final String schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"simpleList\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"simpleStruct\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"child1String\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"child2Int\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"mixedStruct\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"child1String\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"child2Struct\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"childL2String\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"childL2Int\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"child3Arr\",\"type\":{\"type\":\"array\",\"elementType\":\"long\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"nestedList\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"array\",\"elementType\":\"double\",\"containsNull\":true},\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"nestedListStruct\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[{\"name\":\"childL2Long\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"childL2Bin\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]},\"containsNull\":true},\"nullable\":true,\"metadata\":{}}]}";
        /**
         * root
         *  |-- simpleList: array (nullable = true)
         *  |    |-- element: string (containsNull = true)
         *  |-- simpleStruct: struct (nullable = true)
         *  |    |-- child1String: string (nullable = true)
         *  |    |-- child2Int: integer (nullable = true)
         *  |-- mixedStruct: struct (nullable = true)
         *  |    |-- child1String: string (nullable = true)
         *  |    |-- child2Struct: struct (nullable = true)
         *  |    |    |-- childL2String: string (nullable = true)
         *  |    |    |-- childL2Int: integer (nullable = true)
         *  |    |-- child3Arr: array (nullable = true)
         *  |    |    |-- element: long (containsNull = true)
         *  |-- nestedList: array (nullable = true)
         *  |    |-- element: array (containsNull = true)
         *  |    |    |-- element: double (containsNull = true)
         *  |-- nestedListStruct: array (nullable = true)
         *  |    |-- element: struct (containsNull = true)
         *  |    |    |-- childL2Long: long (nullable = true)
         *  |    |    |-- childL2Bin: binary (nullable = true)
         */
        final BatchSchema batchSchema = DeltaLakeSchemaConverter.fromSchemaString(schemaString);

        assertEquals(5, batchSchema.getFieldCount());
        Field simpleList = batchSchema.findField("simpleList");
        assertEquals(ArrowType.ArrowTypeID.List, simpleList.getType().getTypeID());
        assertEquals(1, simpleList.getChildren().size());
        assertEquals(ArrowType.ArrowTypeID.Utf8, simpleList.getChildren().get(0).getType().getTypeID());

        Field simpleStruct = batchSchema.findField("simpleStruct");
        assertEquals(ArrowType.ArrowTypeID.Struct, simpleStruct.getType().getTypeID());
        assertEquals(2, simpleStruct.getChildren().size());
        Field simpleStructChild1 = simpleStruct.getChildren().get(0);
        assertEquals("child1String", simpleStructChild1.getName());
        assertEquals(ArrowType.ArrowTypeID.Utf8, simpleStructChild1.getType().getTypeID());
        Field simpleStructChild2 = simpleStruct.getChildren().get(1);
        assertEquals("child2Int", simpleStructChild2.getName());
        assertEquals(ArrowType.ArrowTypeID.Int, simpleStructChild2.getType().getTypeID());

        Field mixedStruct = batchSchema.findField("mixedStruct");
        assertEquals(ArrowType.ArrowTypeID.Struct, mixedStruct.getType().getTypeID());
        assertEquals(3, mixedStruct.getChildren().size());
        Field mixedStructChild1 = mixedStruct.getChildren().get(0);
        assertEquals("child1String", mixedStructChild1.getName());
        assertEquals(ArrowType.ArrowTypeID.Utf8, mixedStructChild1.getType().getTypeID());
        Field mixedStructChild2 = mixedStruct.getChildren().get(1);
        assertEquals("child2Struct", mixedStructChild2.getName());
        assertEquals(ArrowType.ArrowTypeID.Struct, mixedStructChild2.getType().getTypeID());
        assertEquals(2, mixedStructChild2.getChildren().size());
        Field mixedStructChild2Child1 = mixedStructChild2.getChildren().get(0);
        assertEquals("childL2String", mixedStructChild2Child1.getName());
        assertEquals(ArrowType.ArrowTypeID.Utf8, mixedStructChild2Child1.getType().getTypeID());
        Field mixedStructChild2Child2 = mixedStructChild2.getChildren().get(1);
        assertEquals("childL2Int", mixedStructChild2Child2.getName());
        assertEquals(ArrowType.ArrowTypeID.Int, mixedStructChild2Child2.getType().getTypeID());

        Field nestedList = batchSchema.findField("nestedList");
        assertEquals(ArrowType.ArrowTypeID.List, nestedList.getType().getTypeID());
        assertEquals(1, nestedList.getChildren().size());
        Field nestedListChild1 = nestedList.getChildren().get(0);
        assertEquals(ArrowType.ArrowTypeID.List, nestedListChild1.getType().getTypeID());
        assertEquals(1, nestedListChild1.getChildren().size());
        Field nestedListChild1Child1 = nestedListChild1.getChildren().get(0);
        assertEquals(ArrowType.ArrowTypeID.FloatingPoint, nestedListChild1Child1.getType().getTypeID());
        assertEquals(FloatingPointPrecision.DOUBLE, ((ArrowType.FloatingPoint) nestedListChild1Child1.getType()).getPrecision());

        Field nestedListStruct = batchSchema.findField("nestedListStruct");
        assertEquals(ArrowType.ArrowTypeID.List, nestedListStruct.getType().getTypeID());
        assertEquals(1, nestedListStruct.getChildren().size());
        Field nestedListStructChild = nestedListStruct.getChildren().get(0);
        assertEquals(ArrowType.ArrowTypeID.Struct, nestedListStructChild.getType().getTypeID());
        assertEquals(2, nestedListStructChild.getChildren().size());
        Field nestedListStructChildChild1 = nestedListStructChild.getChildren().get(0);
        assertEquals("childL2Long", nestedListStructChildChild1.getName());
        assertEquals(ArrowType.ArrowTypeID.Int, nestedListStructChildChild1.getType().getTypeID());
        assertEquals(64, ((ArrowType.Int) nestedListStructChildChild1.getType()).getBitWidth());
        Field nestedListStructChildChild2 = nestedListStructChild.getChildren().get(1);
        assertEquals("childL2Bin", nestedListStructChildChild2.getName());
        assertEquals(ArrowType.ArrowTypeID.Binary, nestedListStructChildChild2.getType().getTypeID());
    }

    @Test
    public void testUnsupportedTypes() throws IOException {
        // Maps are unsupported, using following variations - simpleMap, mapInList, mapInStruct, mapInStructMixed
        /**
         * Spark commands, used for generating this delta table
         * val mapSchema = new StructType().add("simpleMap", MapType(StringType, StringType)).add("mapInList", ArrayType(MapType(StringType, StringType))).add("mapInStruct", new StructType().add("m1", MapType(StringType, StringType)).add("m2", MapType(StringType, StringType))).add("mapInStructMixed", new StructType().add("m1", MapType(StringType, StringType)).add("intField", IntegerType))
         * val df = spark.createDataFrame(sc.emptyRDD[Row], mapSchema)
         * df.write.format("delta").mode("overwrite").save("/tmp/delta-tests2");
         */
        final String schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"simpleMap\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"mapInList\",\"type\":{\"type\":\"array\",\"elementType\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"containsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"mapInStruct\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"m1\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"m2\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}},{\"name\":\"mapInStructMixed\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"m1\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]},\"nullable\":true,\"metadata\":{}}]}";
        /**
         * root
         *  |-- simpleMap: map (nullable = true)
         *  |    |-- key: string
         *  |    |-- value: string (valueContainsNull = true)
         *  |-- mapInList: array (nullable = true)
         *  |    |-- element: map (containsNull = true)
         *  |    |    |-- key: string
         *  |    |    |-- value: string (valueContainsNull = true)
         *  |-- mapInStruct: struct (nullable = true)
         *  |    |-- m1: map (nullable = true)
         *  |    |    |-- key: string
         *  |    |    |-- value: string (valueContainsNull = true)
         *  |    |-- m2: map (nullable = true)
         *  |    |    |-- key: string
         *  |    |    |-- value: string (valueContainsNull = true)
         *  |-- mapInStructMixed: struct (nullable = true)
         *  |    |-- m1: map (nullable = true)
         *  |    |    |-- key: string
         *  |    |    |-- value: string (valueContainsNull = true)
         *  |    |-- intField: integer (nullable = true)
         */
        final BatchSchema batchSchema = DeltaLakeSchemaConverter.fromSchemaString(schemaString);
        assertEquals(1, batchSchema.getFieldCount());

        Field mapInStructMixed = batchSchema.findField("mapInStructMixed");
        assertEquals(ArrowType.ArrowTypeID.Struct, mapInStructMixed.getType().getTypeID());
        assertEquals(1, mapInStructMixed.getChildren().size());

        Field intField = mapInStructMixed.getChildren().get(0);
        assertEquals(ArrowType.ArrowTypeID.Int, intField.getType().getTypeID());
        assertEquals("intField", intField.getName());
    }
}
