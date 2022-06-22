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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;
import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transform;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.PartitionTransformTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;


public class IcebergPartitionTransformTableFunction extends AbstractTableFunction {
    private final List<TransferPair> transfers = new ArrayList<>();
    private final PartitionSpec partitionSpec;
    private final Schema schema;
    private final HashMap<String, CompleteType> inputFieldsType = new HashMap<>();
    private final HashMap<String, ValueVector> outputValueVectorMap = new HashMap<>();
    private boolean doneWithRow = false;

    public IcebergPartitionTransformTableFunction(OperatorContext context, TableFunctionConfig functionConfig) {
        super(context, functionConfig);
      PartitionTransformTableFunctionContext functionContext = (PartitionTransformTableFunctionContext) functionConfig.getFunctionContext();
      partitionSpec = IcebergSerDe.deserializePartitionSpec(deserializedJsonAsSchema(functionContext.getIcebergSchema()),
          functionContext.getPartitionSpec().toByteArray());
        schema = partitionSpec.schema();
    }

    @Override
    public VectorAccessible setup(VectorAccessible accessible) throws Exception {
        super.setup(accessible);

        for (Field field : incoming.getSchema()) {
            ValueVector vvIn = getVectorFromSchemaPath(incoming, field.getName());
            ValueVector vvOut = getVectorFromSchemaPath(outgoing, field.getName());
            TransferPair tp = vvIn.makeTransferPair(vvOut);
            transfers.add(tp);
        }

        for (PartitionField partitionField: partitionSpec.fields()) {
            ValueVector vvOut = getVectorFromSchemaPath(outgoing, IcebergUtils.getPartitionFieldName(partitionField));
            outputValueVectorMap.put(IcebergUtils.getPartitionFieldName(partitionField), vvOut);
        }

        for (Field field : incoming.getSchema()) {
            final TypedFieldId partitionValueField = incoming.getValueVectorId(SchemaPath.getSimplePath(field.getName()));
            inputFieldsType.putIfAbsent(field.getName(), partitionValueField.getFinalType());
        }
        return outgoing;
    }

    @Override
    public void startRow(int row) throws Exception {
        doneWithRow = false;
    }

    @Override
    public int processRow(int startOutIndex, int maxRecords) throws Exception {
        if(doneWithRow) {
            return 0;
        }

        for (PartitionField partitionField: partitionSpec.fields()) {
            ValueVector inputVector = getInputVectorForPartitionField(partitionField, schema);
            String inputColumnName = schema.findField(partitionField.sourceId()).name();
            Object columnValue = null;
            columnValue = getValue(startOutIndex, inputVector, inputColumnName);
            Transform transform= partitionField.transform();
            Object transformedValue = transform.apply(columnValue);
            ValueVector outputVector = outputValueVectorMap.get(IcebergUtils.getPartitionFieldName(partitionField));
            writeToVector(outputVector, startOutIndex, transformedValue);
        }

        if(startOutIndex == incoming.getRecordCount() - 1) {
            transfers.forEach(TransferPair::transfer);
            outgoing.setAllCount(incoming.getRecordCount());
        }
        doneWithRow = true;
        return 1;
    }

    @Override
    public void closeRow() throws Exception {

    }

    private ValueVector getInputVectorForPartitionField(PartitionField partitionField, Schema schema) {
        String inputColumnName = schema.findField(partitionField.sourceId()).name();
        return getVectorFromSchemaPath(incoming, inputColumnName);
    }

    public Object getValue(int index, ValueVector vvIn, String inputColumnName) {
        CompleteType completeType = inputFieldsType.get(inputColumnName);
        if (vvIn.isNull(index)) {
            return null;
        }

        switch (completeType.toMinorType()) {
            case TINYINT:
            case UINT1:
                return Integer.valueOf((Byte) (vvIn.getObject(index)));

            case SMALLINT:
            case UINT2:
                return Integer.valueOf((Short) (vvIn.getObject(index)));

            case INT:
            case UINT4:
                return (Integer) vvIn.getObject(index);

            case UINT8:
            case BIGINT:
                return (Long) (vvIn.getObject(index));

            case FLOAT4:
                return ((Float) (vvIn.getObject(index)));

            case FLOAT8:
                return ((Double) (vvIn.getObject(index)));

            case BIT:
                return ((Boolean) (vvIn.getObject(index)));

            case VARBINARY:
                return ((byte[]) (vvIn.getObject(index)));

            case DECIMAL9:
            case DECIMAL18:
            case DECIMAL28SPARSE:
            case DECIMAL38SPARSE:
            case DECIMAL28DENSE:
            case DECIMAL38DENSE:
            case DECIMAL:
                return ((BigDecimal) (vvIn.getObject(index)));


            case DATE:
                if (vvIn instanceof DateMilliVector) {
                    return Math.toIntExact(TimeUnit.MILLISECONDS.toDays(((DateMilliVector) vvIn).get(index)));
                } else {
                    //TODO: needs further tuning
                    return null;
                }

            case TIME:
            case TIMETZ:
            case TIMESTAMPTZ:
            case TIMESTAMP:
            case INTERVAL:
            case INTERVALYEAR:
            case INTERVALDAY:
                if (vvIn instanceof TimeStampMilliVector) {
                    return (((TimeStampMilliVector) vvIn).get(index)) * 1000;
                } else {
                    //TODO: needs further tuning
                    return null;
                }


            case VARCHAR:
            case FIXEDCHAR:
            case FIXED16CHAR:
            case FIXEDSIZEBINARY:
            case VAR16CHAR:
                return vvIn.getObject(index).toString();

            case NULL:
            case MONEY:
            case LATE:
            case STRUCT:
            case LIST:
            case GENERIC_OBJECT:
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type in partition data: " + completeType.toString());
        }
    }
}
