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
package com.dremio.sabot;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.DateUtility;
import org.apache.arrow.vector.util.Text;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.joda.time.Period;
import org.junit.Assert;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.common.util.DremioGetObject;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.op.sort.external.RecordBatchData;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;

import de.vandermeer.asciitable.v2.V2_AsciiTable;
import de.vandermeer.asciitable.v2.render.V2_AsciiTableRenderer;
import de.vandermeer.asciitable.v2.render.WidthAbsoluteEven;
import de.vandermeer.asciitable.v2.themes.V2_E_TableThemes;

/**
 * Class which helps to create expected results sets for test comparison purposes.
 */
public final class Fixtures {

  public static final Cell NULL_VARCHAR = new VarChar(null);
  public static final Cell NULL_BINARY = new VarBinary(null);
  public static final Cell NULL_INT = new IntCell(null);
  public static final Cell NULL_BIGINT = new BigInt(null);
  public static final Cell NULL_FLOAT = new Floating(null);
  public static final Cell NULL_DOUBLE = new DoublePrecision(null);
  public static final Cell NULL_BOOLEAN = new BooleanCell(null);
  public static final Cell NULL_TIMESTAMP = new Timestamp(null);
  public static final Cell NULL_TIME = new Time(null);
  public static final Cell NULL_DATE = new Date(null);
  public static final Cell NULL_INTERVAL_DAY_SECOND = new IntervalDaySecond(null);
  public static final Cell NULL_INTERVAL_YEAR_MONTH = new IntervalYearMonth(null);
  public static final Cell NULL_DECIMAL = new Decimal(null);


  private Fixtures(){}

  public static class Table {
    private final boolean batchSequenceMustMatch;
    private final Field[] fields;
    private final DataBatch[] batches;
    private final int records;
    private boolean orderSensitive;

    private final boolean expectZero;

    private Table(boolean batchSequenceMustMatch, boolean expectZero, Field[] fields, DataBatch... batches) {
      super();
      this.batchSequenceMustMatch = batchSequenceMustMatch;
      this.expectZero = expectZero;
      this.fields = fields;
      this.batches = batches;
      int recordCount = 0;
      for(DataBatch b : batches){
        recordCount += b.size();
      }
      this.records = expectZero ? 0 : recordCount;
      this.orderSensitive = true;
    }

    public Table orderInsensitive() {
      orderSensitive = false;
      return this;
    }

    public boolean isExpectZero() {
      return expectZero;
    }

    public Generator toGenerator(BufferAllocator allocator){
      return new TableFixtureGenerator(allocator, this);
    }

    private HashMap<Object, Fixtures.DataRow> makeResultMap() {
      HashMap<Object, DataRow> result = new HashMap<>();
      final int keyColumnIndex = 0;
      for (DataBatch b : batches) {
        for (DataRow r : b.rows) {
          result.put(r.cells[keyColumnIndex].unwrap(), r);
        }
      }
      return result;
    }

    public void checkValid(List<RecordBatchData> actualBatches){
      Preconditions.checkArgument(actualBatches.size() >= 1, "No data returned.");

      boolean okay = true;
      final StringBuilder sb = new StringBuilder();

      { // first, confirm that the number of records are correct.
        int recordCount = 0;
        for(RecordBatchData d : actualBatches){
          recordCount += d.getRecordCount();
        }
        if(recordCount != records){
          okay = false;
          sb.append(" - ")
            .append("Expected ")
            .append(records)
            .append(" records but found ")
            .append(recordCount)
            .append(" records.")
            .append("\n");

        }
      }

      { // second, check that field types match (count and type)
        RecordBatchData data = actualBatches.get(0);
        BatchSchema schema = data.getContainer().getSchema();

        boolean fieldsMatch = true;
        StringBuilder fieldCompare = new StringBuilder();
        for(int i =0; i < fields.length; i++){
          Field expected = fields[i];
          Field actual = schema.getFieldCount() <= i ? null : schema.getColumn(i);
          if(i != 0){
            fieldCompare.append(", ");
          }
          if(!addField(fieldCompare, expected, actual)){
            fieldsMatch = false;
          }
        }

        // if extra fields in result, also add those.
        for(int i =fields.length; i < schema.getFieldCount(); i++){
          fieldsMatch = false;
          fieldCompare.append(", ");
          addField(fieldCompare, null, schema.getColumn(i));
        }

        if(!fieldsMatch){
          sb.append(" - Fields don't match expectation [actual(expected)]: ").append(fieldCompare.toString()).append("\n");
          okay = false;
        }
      }

      if (!expectZero) {
        // thirdly, compare record by record.
        if(!batchSequenceMustMatch) {
          boolean tablesMatched;
          if (orderSensitive) {
            tablesMatched = compare(sb, fields, this.batches, actualBatches, this.records);
          } else {
            HashMap<Object, Fixtures.DataRow> resultMap = makeResultMap();
            tablesMatched = compareTableResultMap(sb, fields, actualBatches, this.records, resultMap);
          }
          if(!tablesMatched){
            okay = false;
          }
        }else{
          throw new UnsupportedOperationException("We don't yet support evaluating batch boundary comparisons.");
        }
      }

      if(!okay){
        Assert.fail("Data didn't match expected.\n" + sb.toString());
      }
    }
  }

  private static class DataHolder {
    SelectionVector2 sv2;
    List<ValueVector> vectors;

    public DataHolder(RecordBatchData data){
      this.sv2 = data.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE ? data.getSv2() : null;
      List<ValueVector> vectors = new ArrayList<>();
      for(VectorWrapper<?> w : data.getContainer()){
        vectors.add(w.getValueVector());
      }
      this.vectors = ImmutableList.copyOf(vectors);
    }

  }

  private static Object getVectorObject(ValueVector vector, int index) {
    return DremioGetObject.getObject(vector, index);
  }

  private static boolean compareTableResultMap(StringBuilder sb, Field[] fields, List<RecordBatchData> actual,
                                               int expectedRecordCount, HashMap<Object, Fixtures.DataRow> resultMap) {

    int failures  = 0;
    NavigableMap<Integer, RangeHolder<DataHolder>> actualRange = new TreeMap<>();
    {
      int offset = 0;
      for(RecordBatchData b : actual){
        actualRange.put(offset, new RangeHolder<>(new DataHolder(b), offset, b.getRecordCount()));
        offset += b.getRecordCount();
      }
    }

    boolean ok = true;

    final V2_AsciiTable actualOutputTable = new V2_AsciiTable();
    final V2_AsciiTable expectedOutputTable = new V2_AsciiTable();

    Object[] header = new Object[fields.length];
    for(int i =0; i < header.length; i++){
      header[i] = Describer.describe(fields[i]);
    }

    actualOutputTable.addRule();
    actualOutputTable.addRow((Object[]) header);
    actualOutputTable.addRule();

    expectedOutputTable.addRule();
    expectedOutputTable.addRow((Object[]) header);
    expectedOutputTable.addRule();

    final int keyColumnIndex = 0;
    for (int rowNumber = 0; rowNumber < expectedRecordCount; rowNumber++) {
      RangeHolder<DataHolder> actualHolder = actualRange.floorEntry(rowNumber).getValue();
      final int localRowNumber = rowNumber - actualHolder.offset;
      final int vectorOffset = actualHolder.data.sv2 == null ? localRowNumber : actualHolder.data.sv2.getIndex(localRowNumber);
      String[] actualValues = new String[fields.length];
      final boolean isValid = actualHolder.check(rowNumber);
      Object actualKey = isValid ? getVectorObject(actualHolder.data.vectors.get(keyColumnIndex), vectorOffset) : null;
      actualValues[keyColumnIndex] = isValid ? (actualKey != null ? actualKey.toString() : "null") : "null";
      if (resultMap.containsKey(actualKey)) {
        final DataRow expectedRowData = resultMap.get(actualKey);
        for (int columnIndex = 1; columnIndex < fields.length; columnIndex++) {
          Object actualCellValue = isValid ? getVectorObject(actualHolder.data.vectors.get(columnIndex), vectorOffset) : null;
          CellCompare comparison = expectedRowData.cells[columnIndex].compare(actualHolder.data.vectors.get(columnIndex), vectorOffset, actualHolder.check(rowNumber));
          if (!comparison.equal) {
            ok = false;
            failures++;
          }
          actualValues[columnIndex] = (actualCellValue == null) ? "null" : actualCellValue.toString();
        }
      } else {
        ok = false;
        failures++;
        for (int columnIndex = 1; columnIndex < fields.length; columnIndex++) {
          Object actualCellValue = isValid ? getVectorObject(actualHolder.data.vectors.get(columnIndex), vectorOffset) : null;
          actualValues[columnIndex] = actualCellValue.toString();
        }
      }

      actualOutputTable.addRow(actualValues);
      actualOutputTable.addRule();
    }

    if(!ok){
      sb.append("Failed to match: ").append(failures).append(" items");
      sb.append("\n\n---------Actual Output Table (order not important) ---------- \n");
      V2_AsciiTableRenderer rend = new V2_AsciiTableRenderer();
      rend.setTheme(V2_E_TableThemes.UTF_LIGHT.get());
      rend.setWidth(new WidthAbsoluteEven(76));
      sb.append(rend.render(actualOutputTable));
      sb.append("\n\n");

      /* build expected output table from provided map */
      for (Map.Entry<Object, DataRow> expectedEntry : resultMap.entrySet()) {
        String[] expectedValues = new String[fields.length];
        expectedValues[0] = expectedEntry.getKey() == null ? "null" : expectedEntry.getKey().toString();
        final DataRow expectedDataRow = expectedEntry.getValue();
        for (int columnIndex = 1; columnIndex < fields.length; columnIndex++) {
          final Object expectedCellValue = ((ValueCell)expectedDataRow.cells[columnIndex]).obj;
          expectedValues[columnIndex] = (expectedCellValue == null) ? "null" : expectedCellValue.toString();
        }

        expectedOutputTable.addRow(expectedValues);
        expectedOutputTable.addRule();
      }

      sb.append(" ---------Expected Output Table (order not important) ---------- \n");
      rend = new V2_AsciiTableRenderer();
      rend.setTheme(V2_E_TableThemes.UTF_LIGHT.get());
      rend.setWidth(new WidthAbsoluteEven(76));
      sb.append(rend.render(expectedOutputTable));
      sb.append("\n\n");
    }
    return ok;
  }

  private static boolean compare(StringBuilder sb, Field[] fields, DataBatch[] expected, List<RecordBatchData> actual, int expectedRecordCount) {
    // build batch ranges.
    NavigableMap<Integer, RangeHolder<DataHolder>> actualRange = new TreeMap<>();
    {
      int offset = 0;
      for(RecordBatchData b : actual){
        actualRange.put(offset, new RangeHolder<DataHolder>(new DataHolder(b), offset, b.getRecordCount()));
        offset += b.getRecordCount();
      }
    }

    NavigableMap<Integer, RangeHolder<DataBatch>> expectedRange = new TreeMap<>();
    {
      int offset = 0;
      for(DataBatch b : expected){
        expectedRange.put(offset, new RangeHolder<DataBatch>(b, offset, b.size()));
        offset += b.size();
      }
    }
    boolean ok = true;

    final V2_AsciiTable outputTable = new V2_AsciiTable();

    Object[] header = new Object[fields.length];
    for(int i =0; i < header.length; i++){
      header[i] = Describer.describe(fields[i]);
    }
    outputTable.addRule();
    outputTable.addRow((Object[]) header);
    outputTable.addRule();

    for (int rowNumber = 0; rowNumber < expectedRecordCount; rowNumber++) {
      RangeHolder<DataBatch> batch = expectedRange.floorEntry(rowNumber).getValue();
      DataRow expectedRowData = batch.data.rows[rowNumber - batch.offset] ;
      assert batch.check(rowNumber);
      RangeHolder<DataHolder> actualHolder = actualRange.floorEntry(rowNumber).getValue();
      final int localRowNumber = rowNumber - actualHolder.offset;
      final int vectorOffset = actualHolder.data.sv2 == null ? localRowNumber : actualHolder.data.sv2.getIndex(localRowNumber);
      Object[] values = new Object[fields.length];
      for(int columnIndex = 0; columnIndex < fields.length; columnIndex++){
        CellCompare comparison = expectedRowData.cells[columnIndex].compare(actualHolder.data.vectors.get(columnIndex), vectorOffset, actualHolder.check(rowNumber));
        if(!comparison.equal){
          ok = false;
        }
        values[columnIndex] = comparison.s;
      }
      outputTable.addRow(values);
      outputTable.addRule();
    }

    if(!ok){
      sb.append(" - Values don't match expected. [actual (expected)]. \n");
      V2_AsciiTableRenderer rend = new V2_AsciiTableRenderer();
      rend.setTheme(V2_E_TableThemes.UTF_LIGHT.get());
      rend.setWidth(new WidthAbsoluteEven(300));
      sb.append(rend.render(outputTable));
      sb.append("\n");
    }
    return ok;

  }

  private static class RangeHolder<T> {
    final T data;
    final int offset;
    final int length;

    public RangeHolder(T data, int offset, int length) {
      super();
      this.data = data;
      this.offset = offset;
      this.length = length;
    }

    public boolean check(int index){
      return index >= offset && index < offset + length;
    }

  }

  private static boolean addField(StringBuilder sb, Field expected, Field actual){
    boolean ok = true;
    if(actual == null){
      sb.append("-:missing");
      ok = false;
    } else {
      sb.append(Describer.describe(actual));
    }

    if(expected == null){
      sb.append(" (-:missing)");
      return false;
    }

    if(!expected.equals(actual)){
      ok = false;
      sb.append(" (");
      sb.append(Describer.describe(expected));
      sb.append(")");
    }

    return ok;
  }

  private static Field mergeField(Field field, Cell c){
    Preconditions.checkNotNull(field);
    Preconditions.checkNotNull(c);
    Field newField = c.toField(field.getName());
    if(!newField.getType().equals(field.getType())){
      throw new UnsupportedOperationException(String.format("Not supporting mixed types yet. Initial Field was %s but new field was %s", field.getType(), newField.getType()));
    }
    // they are the same.
    return field;
  }

  public static Field[] getFields(HeaderRow row, DataBatch... data){
    Field[] fields = new Field[row.names.length];
    for(DataBatch b : data){
      for(DataRow r : b.rows){
        Preconditions.checkArgument(row.names.length == r.cells.length, "Row must be equivalent length to header.");
        for(int i =0; i < r.cells.length; i++) {
          Field current = fields[i];
          if(current == null){
            fields[i] = r.cells[i].toField(row.names[i]);
          }else{
            fields[i] = mergeField(current, r.cells[i]);
          }
        }
      }
    }
    return fields;
  }

  public static LocalDateTime ts(String str){
    return LocalDateTime.parse(str);
  }

  public static LocalTime time(String str){
    return LocalTime.parse(str);
  }

  public static LocalDate date(String str){
    return LocalDate.parse(str);
  }

  public static LocalDateTime ts(long val){
    return new LocalDateTime(val);
  }

  public static IntervalDaySecond interval_day(int days, int millis) {
    return new IntervalDaySecond(Period.days(days).plusMillis(millis));
  }

  public static IntervalYearMonth interval_year(int years, int months) {
    return new IntervalYearMonth(Period.years(years).plusMonths(months));
  }

  public static class DataRow {
    Cell[] cells;

    public DataRow(Cell[] cells) {
      super();
      this.cells = cells;
    }
  }

  public static class HeaderRow {
    String[] names;

    public HeaderRow(String[] names) {
      super();
      this.names = names;
    }

  }

  public static class DataBatch {
    private final DataRow[] rows;

    public DataBatch(DataRow... rows) {
      this.rows = rows;
    }

    public int size(){
      return rows.length;
    }

  }

  public static HeaderRow th(String... headers){
    return new HeaderRow(headers);
  }

  public static DataRow tr(Object... objects){
    Cell[] cells = new Cell[objects.length];
    for(int i =0; i < cells.length; i++){
      cells[i] = toCell(objects[i]);
    }
    return new DataRow(cells);
  }

  private static Cell toCell(Object obj){
    Preconditions.checkNotNull(obj, "Use Null constants to express nulls, such as NULL_VARCHAR, NULL_INT, etc.");

    if(obj instanceof Cell){
      return (Cell) obj;
    } else if(obj instanceof String){
      return new VarChar((String) obj);
    }else if(obj instanceof Long){
      return new BigInt((Long)obj);
    }else if(obj instanceof Double){
      return new DoublePrecision((Double)obj);
    }else if(obj instanceof Float){
      return new Floating((Float)obj);
    }else if(obj instanceof Integer){
      return new IntCell((Integer)obj);
    }else if(obj instanceof Boolean){
      return new BooleanCell((Boolean)obj);
    }else if(obj instanceof byte[]){
      return new VarBinary((byte[])obj);
    }else if(obj instanceof LocalDateTime) {
      return new Timestamp((LocalDateTime)obj);
    }else if(obj instanceof LocalTime) {
      return new Time((LocalTime)obj);
    }else if(obj instanceof LocalDate) {
      return new Date((LocalDate)obj);
    }else if(obj instanceof List) {
      return new ListCell((List<Integer>)obj);
    }else if(obj instanceof BigDecimal) {
      return new Decimal((BigDecimal) obj);
    }else if(obj instanceof Period) {
      Period p = (Period) obj;

      if (p.getYears() == 0 && p.getMonths() == 0) {
        return new IntervalDaySecond(p);
      }

      if (p.getDays() == 0) {
        return new IntervalYearMonth(p);
      }
    }
    throw new UnsupportedOperationException(String.format("Unable to interpret object of type %s.", obj.getClass().getSimpleName()));
  }

  public static Table t(HeaderRow header, DataBatch... batches){
    return new Table(true, false, getFields(header, batches), batches);
  }

  public static Table t(HeaderRow header, DataRow... rows){
    return t(header, false, rows);
  }

  public static Table split(HeaderRow header, int desiredBatchSize, DataRow... rows) {
    Preconditions.checkState(desiredBatchSize > 0, "desiredBatchSize must be positive");
    final int numBatches = (int) Math.ceil(rows.length * 1.0 / desiredBatchSize);

    final DataBatch[] batches = new DataBatch[numBatches];
    for (int batch = 0; batch < numBatches; batch++) {
      final int startRow = batch * desiredBatchSize;
      final int lastRow = Math.min(startRow + desiredBatchSize - 1, rows.length - 1);
      final int numRowsToCopy = lastRow - startRow + 1;
      final DataRow[] copied = new DataRow[numRowsToCopy];
      System.arraycopy(rows, startRow, copied, 0, numRowsToCopy);

      batches[batch] = tb(copied);
    }

    return t(header, batches);
  }

  public static Table t(HeaderRow header, boolean zeroRecords, DataRow... rows){
    DataBatch b = new DataBatch(rows);
    Field[] fields = getFields(header, b);
    return new Table(false, zeroRecords, fields, b);
  }

  public static DataBatch tb(DataRow... rows) {
    return new DataBatch(rows);
  }


  public interface Cell {
    Field toField(String name);
    CellCompare compare(ValueVector vector, int index, boolean isValid);
    void set(ValueVector v, int index);
    Object unwrap();
  }

  static abstract class ValueCell<V> implements Cell {
    public final V obj;

    public ValueCell(V obj) {
      super();
      this.obj = obj;
    }

    abstract ArrowType getType();

    @Override
    public Field toField(String name) {
      return new Field(name, true, getType(), Collections.<Field>emptyList());
    }


    @SuppressWarnings("unchecked")
    public CellCompare compare(ValueVector vector, int index, boolean isValid) {
      V obj = isValid ? (V)getVectorObject(vector, index) : null;
      if(obj == null && this.obj == null){
        return new CellCompare(true, toString(obj));
      }
      if(this.obj == null){
        return new CellCompare(false, toString(obj) + " (null)");
      }

      if(obj == null){
        return new CellCompare(false, null + " ("+toString(this.obj)+")");
      }

      if(!obj.getClass().equals(this.obj.getClass())){
        return new CellCompare(false, obj.toString() + "(" + this.obj.toString() + ")");
      }

      boolean isEqual = evaluateEquality(obj, this.obj);
      if(isEqual){
        return new CellCompare(true, toString(this.obj));
      }else{
        return new CellCompare(false, toString(obj) + " ("+toString(this.obj)+")");
      }
    }

    @Override
    public Object unwrap() {
      return obj;
    }

    boolean evaluateEquality(V obj1, V obj2){
      return obj1.equals(obj2);
    }

    String toString(V obj) {
      if(obj == null){
        return "null";
      }
      return obj.toString();
    }
  }

  private static class CellCompare {
    final boolean equal;
    final String s;

    public CellCompare(boolean equal, String s) {
      super();
      this.equal = equal;
      this.s = s;
    }

  }

  private static class ListCell extends ValueCell<List<Integer>> {

    private ValueVector dataVector;

    public ListCell(List<Integer> obj) {
      super(obj);
    }

    @Override
    ArrowType getType() {
      return ArrowType.List.INSTANCE;
    }

    @Override
    public void set(ValueVector v, int index) {
      this.dataVector = ((ListVector)v).getDataVector();
      if(obj != null){
        UnionListWriter listWriter = ((ListVector)v).getWriter();
        listWriter.setPosition(index);
        List<Integer> list = obj;
        listWriter.startList();
        for (int i = 0; i < list.size(); i++) {
          listWriter.bigInt().writeBigInt(list.get(i));
        }
        listWriter.endList();
      }
    }

    @Override
    public Field toField(String name) {
      return new Field(name, true, getType(), ImmutableList.of(CompleteType.BIGINT.toField("$data$")));
    }
  }

  private static class BigInt extends ValueCell<Long> {

    public BigInt(Long obj) {
      super(obj);
    }

    @Override
    ArrowType getType() {
      return new ArrowType.Int(64, true);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((BigIntVector) v).setSafe(index, obj);
      }
    }


  }

  private static class IntCell extends ValueCell<Integer> {
    public IntCell(Integer obj) {
      super(obj);
    }

    @Override
    ArrowType getType() {
      return new ArrowType.Int(32, true);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((IntVector) v).setSafe(index, obj);
      }
    }

  }

  private static class Floating extends ValueCell<Float> {
    public Floating(Float obj) {
      super(obj);
    }

    @Override
    ArrowType getType() {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((Float4Vector) v).setSafe(index, obj);
      }
    }

    @Override
    boolean evaluateEquality(Float f1, Float f2) {
      if(f1.isNaN()){
        return f2.isNaN();
      }

      if(f1.isInfinite()){
        return f2.isInfinite();
      }

      if ((f1 + f2) / 2 != 0) {
        return Math.abs(f1 - f2) / Math.abs((f1 + f2) / 2) < 1.0E-6;
      } else {
        return !(f1 != 0);
      }
    }
  }

  private static class DoublePrecision extends ValueCell<Double> {
    public DoublePrecision(Double obj) {
      super(obj);
    }

    @Override
    ArrowType getType() {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((Float8Vector) v).setSafe(index, obj);
      }
    }

    @Override
    boolean evaluateEquality(Double f1, Double f2) {
      if(f1.isNaN()){
        return f2.isNaN();
      }

      if(f1.isInfinite()){
        return f2.isInfinite();
      }

      if ((f1 + f2) / 2 != 0) {
        return Math.abs(f1 - f2) / Math.abs((f1 + f2) / 2) < 1.0E-6;
      } else {
        return !(f1 != 0);
      }
    }
  }

  private static class VarChar extends ValueCell<Text> {

    public VarChar(String obj) {
      super(obj == null ? null : new Text(obj));
    }

    @Override
    ArrowType getType() {
      return ArrowType.Utf8.INSTANCE;
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        byte[] bytes = obj.getBytes();
        ((VarCharVector) v).setSafe(index, bytes, 0, obj.getLength());
      }
    }


  }

  private static class Timestamp extends ValueCell<LocalDateTime> {

    public Timestamp(LocalDateTime obj) {
      super(obj);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((TimeStampMilliVector) v).setSafe(index, com.dremio.common.util.DateTimes.toMillis(obj));
      }
    }

    @Override
    ArrowType getType() {
      return CompleteType.TIMESTAMP.getType();
    }

  }


  private static class Date extends ValueCell<LocalDate> {

    public Date(LocalDate obj) {
      super(obj);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((DateMilliVector) v).setSafe(index,  obj.toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis());
      }
    }

    @Override
    ArrowType getType() {
      return CompleteType.DATE.getType();
    }

  }

  private static class Time extends ValueCell<LocalTime> {

    public Time(LocalTime obj) {
      super(obj);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((TimeMilliVector) v).setSafe(index, (int) obj.getMillisOfDay());
      }
    }

    @Override
    ArrowType getType() {
      return CompleteType.TIME.getType();
    }

  }

  private static class IntervalDaySecond extends ValueCell<Period> {
    public IntervalDaySecond(Period obj) {
      super(obj);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        int numMillis = obj.getHours() * DateUtility.hoursToMillis
          + obj.getMinutes() * DateUtility.minutesToMillis
          + obj.getSeconds() * DateUtility.secondsToMillis
          + obj.getMillis();
        ((IntervalDayVector) v).setSafe(index, obj.getDays(), numMillis);
      }
    }

    @Override
    ArrowType getType() {
      return CompleteType.INTERVAL_DAY_SECONDS.getType();
    }
  }

  private static class IntervalYearMonth extends ValueCell<Period> {
    public IntervalYearMonth(Period obj) {
      super(obj);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((IntervalYearVector) v).setSafe(index, obj.getMonths() + obj.getYears() * DateUtility.yearsToMonths);
      }
    }

    @Override
    ArrowType getType() {
      return CompleteType.INTERVAL_YEAR_MONTHS.getType();
    }
  }

  private static class BooleanCell extends ValueCell<Boolean> {

    public BooleanCell(Boolean obj) {
      super(obj);
    }

    @Override
    ArrowType getType() {
      return ArrowType.Bool.INSTANCE;
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((BitVector) v).setSafe(index, obj == true ? 1 : 0);
      }
    }

  }

  private static class VarBinary extends ValueCell<byte[]> {

    public VarBinary(byte[] obj) {
      super(obj);
    }

    @Override
    ArrowType getType() {
      return ArrowType.Binary.INSTANCE;
    }

    @Override
    boolean evaluateEquality(byte[] obj1, byte[] obj2) {
      return Arrays.equals(obj1, obj2);
    }

    public String toString(byte[] obj){
      return BaseEncoding.base16().encode(obj);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((VarBinaryVector) v).setSafe(index, obj, 0, obj.length);
      }
    }
  }

  public static Decimal createDecimal(BigDecimal d, int precision, int scale) {
    return new Decimal(d, precision, scale);
  }

  private static class Decimal extends ValueCell<BigDecimal> {
    int precision;
    int scale;

    public Decimal(BigDecimal obj) {
      this(obj, 38, obj == null ? 0 : obj.scale());
    }

    public Decimal(BigDecimal obj, int precision, int scale) {
      super(obj);
      this.precision = precision;
      this.scale = scale;
    }


    @Override
    ArrowType getType() {
      return new ArrowType.Decimal(precision, scale);
    }

    @Override
    public void set(ValueVector v, int index) {
      if(obj != null){
        ((DecimalVector) v).setSafe(index, obj);
      }
    }

    @Override
    boolean evaluateEquality(BigDecimal val1, BigDecimal val2) {
      return val1.equals(val2);
    }
  }

  private static class TableFixtureGenerator implements Generator {
    private Table table;
    private final VectorContainer container;
    private ValueVector[] vectors;
    private int batchOffset = 0;

    public TableFixtureGenerator(BufferAllocator allocator, Table table){
      this.table = table;
      this.container = new VectorContainer(allocator);
      vectors = new ValueVector[table.fields.length];
      for(int i = 0; i < table.fields.length; i++){
        vectors[i] = container.addOrGet(table.fields[i]);
      }
      container.buildSchema(SelectionVectorMode.NONE);
    }
    @Override
    public void close() throws Exception {
      container.close();
    }

    @Override
    public VectorAccessible getOutput() {
      return container;
    }

    @Override
    public int next(int records) {
      container.allocateNew();
      if(batchOffset >= table.batches.length){
        return 0;
      }
      DataRow[] rows = table.batches[batchOffset].rows;
      batchOffset++;
      for(int i =0; i < rows.length; i++){
        DataRow row = rows[i];
        for (int v = 0; v < vectors.length; v++ ) {
          row.cells[v].set(vectors[v], i);
        }
      }
      return container.setAllCount(rows.length);
    }

  }

}
