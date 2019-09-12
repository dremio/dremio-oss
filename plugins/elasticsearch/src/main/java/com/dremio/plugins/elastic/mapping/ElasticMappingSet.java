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
package com.dremio.plugins.elastic.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.plugins.elastic.DateFormats;
import com.dremio.plugins.elastic.DateFormats.FormatterAndType;
import com.dremio.plugins.elastic.ElasticsearchConstants;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Describes the response to a _mapping query in Elastic.
 */
public class ElasticMappingSet implements Iterable<ElasticMappingSet.ElasticIndex> {

  private static final Logger logger = LoggerFactory.getLogger(SchemaMerger.class);

  public final static ObjectMapper MAPPER = new ObjectMapper()
      .registerModule(new GuavaModule())
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .configure(SerializationFeature.INDENT_OUTPUT, true)
      .setInjectableValues(CurentNameInjectable.INSTANCE);

  private static final CompleteType GEO_POINT_TYPE = CompleteType.struct(
      CompleteType.DOUBLE.toField(ElasticsearchConstants.GEO_POINT_LAT),
      CompleteType.DOUBLE.toField(ElasticsearchConstants.GEO_POINT_LON)
      );


  //
  private static final Field DOUBLE = CompleteType.DOUBLE.toField("float8");
  private static final Field DOUBLE_LIST = CompleteType.DOUBLE.asList().toField("list");

  private static final Field UNION_LIST(Field...fields){
    return CompleteType.union(fields).asList().toField("list");
  }

  private static Field COORDS(Field...fields){
    return CompleteType.union(fields).toField(ElasticsearchConstants.GEO_SHAPE_COORDINATES);
  }

  private static final Field COORDINATES =
      COORDS(
          DOUBLE,
          UNION_LIST(
              DOUBLE,
              UNION_LIST(
                  DOUBLE,
                  UNION_LIST(
                      DOUBLE,
                      UNION_LIST(
                            DOUBLE,
                            DOUBLE_LIST)))));


  public static void main(String[] args) {
    System.out.println(Describer.describe(COORDINATES));
  }

  private static final CompleteType GEO_SHAPE_TYPE = CompleteType.struct(
      CompleteType.VARCHAR.toField(ElasticsearchConstants.GEO_SHAPE_TYPE),
      COORDINATES,
      CompleteType.VARCHAR.toField(ElasticsearchConstants.GEO_SHAPE_RADIUS),
      CompleteType.VARCHAR.toField(ElasticsearchConstants.GEO_SHAPE_ORIENTATION),
      CompleteType.struct(
          CompleteType.VARCHAR.toField(ElasticsearchConstants.GEO_SHAPE_TYPE),
          COORDINATES,
          CompleteType.VARCHAR.toField(ElasticsearchConstants.GEO_SHAPE_RADIUS),
          CompleteType.VARCHAR.toField(ElasticsearchConstants.GEO_SHAPE_ORIENTATION)
          ).asList().toField(ElasticsearchConstants.GEO_SHAPE_GEOMETRIES)
      );

  private final ImmutableList<ElasticIndex> indexes;

  @JsonCreator
  public ElasticMappingSet(Map<String, ElasticIndex> indexes) {
    this.indexes = asList(indexes);
  }

  public ElasticMappingSet(List<ElasticIndex> indexes) {
    this.indexes = ImmutableList.copyOf(indexes);
  }

  public ElasticMappingSet filterToType(String typeName){
    List<ElasticIndex> newIndexes = new ArrayList<>();
    for(ElasticIndex i : this.indexes){
      ElasticIndex newIndex = i.filterToType(typeName);
      if(newIndex != null){
        newIndexes.add(newIndex);
      }
    }

    return new ElasticMappingSet(newIndexes);

  }

  public int size(){
    return indexes.size();
  }

  public boolean isEmpty(){
    return indexes.isEmpty();
  }

  public ImmutableList<ElasticIndex> getIndexes() {
    return indexes;
  }

  @Override
  public Iterator<ElasticIndex> iterator() {
    return indexes.iterator();
  }

  /**
   * Combine all index mappings into a single merged mapping. Note that this is only possible if the mappings are compatible.
   * @return A new merged mapping.
   */
  @JsonIgnore
  public ElasticMapping getMergedMapping(){
    ElasticMapping mapping = null;
    String index_name = "";
    for(ElasticIndex index : indexes){
      for(ElasticMapping m : index.mappings){
        if(mapping == null){
          mapping = m;
          index_name = index.name;
        } else {
          mapping = mapping.merge(m, index_name, index.name);
        }
      }
    }
    return mapping;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof ElasticMappingSet)) {
      return false;
    }
    ElasticMappingSet castOther = (ElasticMappingSet) other;
    return Objects.equal(indexes, castOther.indexes);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(indexes);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("indexes", indexes).toString();
  }

  private static Type maxTemporal(Type t1, Type t2){
    switch(t1){
    case DATE:
      switch(t2){
        case DATE:
          return Type.DATE;
        default:
          return Type.TIMESTAMP;
      }
    case TIME:
      switch(t2){
        case TIME:
          return Type.TIME;
        default:
          return Type.TIMESTAMP;
      }
    case TIMESTAMP:
      return Type.TIMESTAMP;

    }

    throw new IllegalStateException("Only temporal types should be compared with this function.");
  }

  public static class ElasticField implements Iterable<ElasticField> {
    private final String name;
    private Type type;
    private final Indexing indexing;
    private final boolean normalized;
    private final List<String> formats;
    private final ImmutableList<ElasticField> children;
    private final boolean docValues;

    private ElasticField(
        String name,
        Type type,
        Indexing indexing,
        boolean normalized,
        List<String> formats,
        boolean docValues,
        List<ElasticField> children) {
      this.name = name;
      this.type = type;
      this.indexing = indexing;
      this.normalized = normalized;
      this.formats = formats;
      this.docValues = docValues;
      this.children = ImmutableList.copyOf(children);
    }

    public ElasticField(
        @JacksonInject(CurentNameInjectable.CURRENT_NAME) String name,
        @JsonProperty("type") Type type,
        @JsonProperty("index") Indexing indexing,
        @JsonProperty("normalizer") String normalizer,
        @JsonProperty("format") String format,
        @JsonProperty("doc_values") Boolean docValues,
        @JsonProperty("properties") Map<String,ElasticField> children){
      this.name = Preconditions.checkNotNull(name, "Field didn't have name.");
      this.formats = DateFormats.getFormatList(format);

      // the type system for elastic somewhat incomplete. In cases of the single "date" type can be mapped to TIME, DATE or TIMESTAMP. We need to check this here.
      if(type == Type.DATE){
        FormatterAndType[] types = DateFormats.getFormatterAndType(formats);
        Preconditions.checkArgument(types.length > 0);
        Type newType = types[0].type();

        for(int i = 1; i < types.length; i++){
          newType = maxTemporal(newType, types[i].type());
        }

        type = newType;
      }

      if(type == Type.HALF_FLOAT){
        type = Type.FLOAT;
      }

      // fields are not analyzed unless they are text excplicitly set to analyzed.
      if(indexing == null){
        this.indexing = type == Type.TEXT || type == Type.STRING ? Indexing.ANALYZED : Indexing.NOT_ANALYZED;
      }else {
        if (type == Type.TEXT && indexing == Indexing.NOT_ANALYZED) {
          // TEXT is analyzed as long as it is indexed
          this.indexing = Indexing.ANALYZED;
        } else {
          this.indexing = indexing;
        }
      }

      this.normalized = normalizer != null;

      this.type = type == null ? Type.OBJECT : type;

      // doc values are available for all fields except analyzed ones or where explicitly disabled.
      // https://www.elastic.co/guide/en/elasticsearch/reference/current/doc-values.html
      if(docValues == null){
        if(type == Type.TEXT || this.indexing == Indexing.ANALYZED){
          this.docValues = false;
        }else{
          this.docValues = true;
        }
      }else{
        this.docValues = docValues;
      }
      this.children = asList(children);

    }

    public void setTypeUnknown() {
      this.type = Type.UNKNOWN;
    }

    public ElasticField merge(ElasticMapping mapping, ElasticField field, String curr_mapping, String other_mapping, String curr_index, String other_index){
      if(equals(field)){
        return this;
      }

      if(!Objects.equal(name, field.name)) {
        this.type = Type.UNKNOWN;
        field.type = Type.UNKNOWN;
        logDataReadErrorHelper(field, curr_mapping, other_mapping, curr_index, other_index, "names", name, field.name);
        return null;
      }

      if(type != field.type){
        this.type = Type.UNKNOWN;
        field.type = Type.UNKNOWN;
        logDataReadErrorHelper(field, curr_mapping, other_mapping, curr_index, other_index, "types", type.toString(), field.type.toString());
        return null;
      }

      if(indexing != field.indexing){
        this.type = Type.UNKNOWN;
        field.type = Type.UNKNOWN;
        logDataReadErrorHelper(field, curr_mapping, other_mapping, curr_index, other_index, "indexing schemes", indexing.toString(), field.indexing.toString());
        return null;
      }

      if(!Objects.equal(formats, field.formats)){
        this.type = Type.UNKNOWN;
        field.type = Type.UNKNOWN;
        logDataReadErrorHelper(field, curr_mapping, other_mapping, curr_index, other_index, "date format schemes", formats.toString(), field.formats.toString());
        return null;
      }

      if(docValues != field.docValues){
        this.type = Type.UNKNOWN;
        field.type = Type.UNKNOWN;
        logDataReadErrorHelper(field, curr_mapping, other_mapping, curr_index, other_index, "doc values storage settings", String.valueOf(docValues), String.valueOf(field.docValues));
        return null;
      }

      // if either one is normalized, treat the merger as normalized
      boolean normalized = this.normalized || field.normalized;

      // we just have different fields. Let's merge them.
      return new ElasticField(name, type, indexing, normalized, formats, docValues, mergeFields(mapping, children, field.children, curr_mapping, other_mapping, curr_index, other_index));
    }

    public void logDataReadErrorHelper(ElasticField field, String curr_mapping, String other_mapping, String curr_index, String other_index, String diff, String first, String second) {
      logger.warn(String.format("Unable to merge two different definitions for Field: (%s) with Type: (%s) from (%s.%s) with Field: (%s) with Type: (%s) from (%s.%s) as they are different %s. The %s are %s and %s.", name, type, curr_index, curr_mapping, field.name, field.type, other_index, other_mapping, diff, diff, first, second));
    }

    public static List<ElasticField> mergeFields(ElasticMapping mapping, List<ElasticField> fieldsA, List<ElasticField> fieldsB, String mappingA, String mappingB, String indexA, String indexB){
      // There is field variation if the number of fieldA is different from that of fieldB.
      if (fieldsA.size() != fieldsB.size()) {
        mapping.setVariationDetected(true);
      }
      Map<String, ElasticField> fields = new LinkedHashMap<>();
      for(ElasticField f : fieldsA){
        fields.put(f.getName(), f);
      }
      // add new fields, add replacements to intermediate map.
      Map<String, ElasticField> replacements = new HashMap<>();
      for(ElasticField f : fieldsB){
        if(fields.containsKey(f.getName())){
          replacements.put(f.getName(), fields.get(f.getName()).merge(mapping, f, mappingA, mappingB, indexA, indexB));
        } else {
          // There is field variation if the field in fieldB is not found in fieldA.
          mapping.setVariationDetected(true);
          fields.put(f.getName(), f);
        }
      }

      // return all values, substituting replacements where they exist (but keeping the initial field ordering).

      List<ElasticField> output = new ArrayList<>();
      for(Map.Entry<String, ElasticField> e : fields.entrySet()){
        ElasticField replacement = replacements.get(e.getKey());
        if(replacement != null){
          output.add(replacement);
        } else {
          output.add(e.getValue());
        }
      }
      return output;
    }

    public Field toArrowField(){
      if(type.completeType != null){
        return type.completeType.toField(name);
      }

      switch(type){
      case NESTED:
        return CompleteType.struct(FluentIterable.from(children).transform(new Function<ElasticField, Field>(){
          @Override
          public Field apply(ElasticField input) {
            return input.toArrowField();
          }})).toField(name);
      case OBJECT:
        return CompleteType.struct(FluentIterable.from(children).transform(new Function<ElasticField, Field>(){
          @Override
          public Field apply(ElasticField input) {
            return input.toArrowField();
          }})).toField(name);
      default:
        throw new UnsupportedOperationException("Unable to handle field " + this.toString());
      }
    }


    public boolean hasDocValues(){
      return docValues;
    }

    @Override
    public Iterator<ElasticField> iterator() {
      return children.iterator();
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
    }

    public Indexing getIndexing() {
      return indexing;
    }

    public boolean isNormalized() {
      return normalized;
    }

    public List<String> getFormats() {
      return formats;
    }

    public ImmutableList<ElasticField> getChildren() {
      return children;
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof ElasticField)) {
        return false;
      }
      ElasticField castOther = (ElasticField) other;
      return Objects.equal(name, castOther.name) && Objects.equal(type, castOther.type)
          && Objects.equal(indexing, castOther.indexing)
          && Objects.equal(formats, castOther.formats) && Objects.equal(children, castOther.children);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, type.ordinal(), indexing.ordinal(), formats, ImmutableSet.copyOf(children));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("name", name).add("type", type).add("indexing", indexing)
          .add("formats", formats).add("children", children).toString();
    }

  }

  public static class ElasticIndex {

    private final String name;
    private final ImmutableList<ElasticMapping> mappings;
    private final ImmutableList<String> aliases;

    @JsonCreator
    public ElasticIndex(
        @JacksonInject(CurentNameInjectable.CURRENT_NAME) String name,
        @JsonProperty("aliases") List<String> aliases,
        @JsonProperty("mappings") Map<String, ElasticMapping> mappings) {
      this.name = name;
      this.aliases = aliases == null ? ImmutableList.<String>of() : ImmutableList.copyOf(aliases);
      this.mappings = asList(mappings);
    }

    public ElasticIndex(
        String name,
        ElasticMapping mapping) {
      this.name = name;
      this.aliases = ImmutableList.of();
      this.mappings = ImmutableList.of(mapping);
    }

    public String getName() {
      return name;
    }

    public ElasticIndex filterToType(String name){
      List<ElasticMapping> filtered = new ArrayList<>();
      for(ElasticMapping m : mappings){
        for(String namePiece : name.split(",")){
          if(m.getName().equals(namePiece)){
            filtered.add(m);
          }
        }
      }
      if(filtered.isEmpty()){
        return null;
      }
      return new ElasticIndex(name, ImmutableList.<String>of(), FluentIterable.from(filtered).uniqueIndex(new Function<ElasticMapping, String>(){

        @Override
        public String apply(ElasticMapping input) {
          return input.getName();
        }}));
    }

    public ImmutableList<ElasticMapping> getMappings() {
      return mappings;
    }

    public ElasticMapping getMergedMapping(){
      return new ElasticMappingSet(ImmutableMap.<String, ElasticIndex>of(this.name, this)).getMergedMapping();
    }

    public List<String> getAliases(){
      return aliases;
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof ElasticIndex)) {
        return false;
      }
      ElasticIndex castOther = (ElasticIndex) other;
      return Objects.equal(name, castOther.name) && Objects.equal(mappings, castOther.mappings)
          && Objects.equal(aliases, castOther.aliases);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, mappings, aliases);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("name", name).add("mappings", mappings).add("aliases", aliases)
          .toString();
    }


  }

  public static class ElasticMapping implements Iterable<ElasticField> {

    private final String name;
    private final ImmutableList<ElasticField> fields;
    private final boolean composite;
    private boolean variationDetected;

    private ElasticMapping(String name, List<ElasticField> fields, boolean merged, boolean variationDetected){
      this.name = name;
      this.fields = ImmutableList.copyOf(fields);
      this.composite = merged;
      this.variationDetected = variationDetected;
    }

    @JsonCreator
    public ElasticMapping(
        @JacksonInject(CurentNameInjectable.CURRENT_NAME) String name,
        @JsonProperty("properties") Map<String, ElasticField> fields) {
      super();
      this.name = name;
      this.fields = asList(fields);
      this.composite = false;
      this.variationDetected = false;
    }

    public boolean isVariationDetected() {
      return variationDetected;
    }

    public void setVariationDetected(boolean variationDetected) {
      this.variationDetected = variationDetected;
    }

    public String getName() {
      return name;
    }

    public ImmutableList<ElasticField> getFields() {
      return fields;
    }

    public ElasticMapping merge(ElasticMapping mapping, String curr_index, String other_index){
      String newName = name.equals(mapping.name) ? name : name + "," + mapping.name;
      List<ElasticField> newFields = ElasticField.mergeFields(mapping, fields, mapping.fields, name, mapping.name, curr_index, other_index);
      return new ElasticMapping(newName, newFields, true, mapping.variationDetected);
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof ElasticMapping)) {
        return false;
      }
      ElasticMapping castOther = (ElasticMapping) other;
      return Objects.equal(name, castOther.name) && Objects.equal(fields, castOther.fields);
    }

    @Override
    public Iterator<ElasticField> iterator() {
      return fields.iterator();
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, ImmutableSet.copyOf(fields));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("name", name).add("fields", fields).toString();
    }

    /**
     * Whether this mapping is a composite of multiple original mappings.
     * @return True if this was built of more than one mapping.
     */
    public boolean isComposite(){
      return composite;
    }
  }

  public static enum Indexing {
    @JsonProperty("true") NOT_ANALYZED(true, "not_analyzed", "true"),
    @JsonProperty("true") ANALYZED(true, "analyzed"),
    @JsonProperty("false") NOT_INDEXED(false, "no", "not_indexed", "false");

    public final boolean supportsAggregation;
    private final List<String> names;

    Indexing(boolean supportsAggregation, String... names){
      this.supportsAggregation = supportsAggregation;
      this.names = ImmutableList.copyOf(names);
    }

    private static final ImmutableMap<String, Indexing> TYPES;


    static {
      ImmutableMap.Builder<String, Indexing> builder = ImmutableMap.builder();
      for(Indexing i : Indexing.values()){
        for(String name : i.names){
          builder.put(name, i);
        }
      }
      TYPES = builder.build();
    }

    @JsonCreator
    public static Indexing getIndexingValue(String value) throws JsonMappingException {
      Indexing indexing = TYPES.get(value);
      if(indexing != null){
        return indexing;
      }

      throw new JsonMappingException(String.format("Unable to map Indexing property '%s' to known properties.", value));
    }

  }

  @JsonDeserialize(using=ElasticTypeDeserializer.class)
  public static enum Type {
    BYTE(CompleteType.INT),
    SHORT(CompleteType.INT),
    INTEGER(CompleteType.INT),
    LONG(CompleteType.BIGINT),
    HALF_FLOAT(CompleteType.FLOAT),
    FLOAT(CompleteType.FLOAT),
    DOUBLE(CompleteType.DOUBLE),
    SCALED_FLOAT(CompleteType.DOUBLE),
    BOOLEAN(CompleteType.BIT),
    BINARY(CompleteType.VARBINARY),
    STRING(CompleteType.VARCHAR),
    TEXT(CompleteType.VARCHAR),
    KEYWORD(CompleteType.VARCHAR),
    DATE(CompleteType.DATE),
    IP(CompleteType.VARCHAR),
    TIME(CompleteType.TIME),
    TIMESTAMP(CompleteType.TIMESTAMP),
    GEO_POINT(GEO_POINT_TYPE),
    GEO_SHAPE(GEO_SHAPE_TYPE),
    ATTACHMENT(CompleteType.VARBINARY),

    NESTED(null),
    OBJECT(null),

    UNKNOWN(CompleteType.VARBINARY);

    public final CompleteType completeType;

    Type(CompleteType completeType){
      this.completeType = completeType;
    }

  }

  public static class CurentNameInjectable extends InjectableValues {
    public static final String CURRENT_NAME = "CURRENT_NAME";

    public static InjectableValues INSTANCE = new CurentNameInjectable(new InjectableValues.Std());

    private final InjectableValues delegate;

    public CurentNameInjectable(InjectableValues delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    public Object findInjectableValue(Object valueId, DeserializationContext ctxt, BeanProperty forProperty,
        Object beanInstance) throws JsonMappingException {
      if(valueId != null && CURRENT_NAME.equals(valueId)){
        return ctxt.getParser().getParsingContext().getCurrentName();
      }
      return delegate.findInjectableValue(valueId, ctxt, forProperty, beanInstance);
    }

  }

  private static <T> ImmutableList<T> asList(Map<String, T> map){
    if(map == null){
      return ImmutableList.of();
    } else {
      return ImmutableList.copyOf(map.values());
    }
  }


  public static class ClusterMetadata {
    private final String clusterName;
    private final MetadataBlock metadata;

    @JsonCreator
    public ClusterMetadata(@JsonProperty("cluster_name") String clusterName, @JsonProperty("metadata") MetadataBlock metadata) {
      super();
      this.clusterName = clusterName;
      this.metadata = metadata;
    }

    public String getClusterName(){
      return clusterName;
    }

    public List<ElasticIndex> getIndices(){
      return metadata.mappingSet.getIndexes();
    }

  }

  public static class MetadataBlock {
    private final ElasticMappingSet mappingSet;

    @JsonCreator
    public MetadataBlock(@JsonProperty("indices") ElasticMappingSet mappingSet) {
      super();
      this.mappingSet = mappingSet;
    }

  }
}
