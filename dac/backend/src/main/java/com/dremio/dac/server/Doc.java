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
package com.dremio.dac.server;

//import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;

import java.io.File;
import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.apache.arrow.util.Preconditions;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;
import com.fasterxml.jackson.module.jsonSchema.types.ArraySchema;
import com.fasterxml.jackson.module.jsonSchema.types.ArraySchema.ArrayItems;
import com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema;
import com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema.AdditionalProperties;
import com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema.SchemaAdditionalProperties;
import com.fasterxml.jackson.module.jsonSchema.types.ReferenceSchema;
import com.google.common.base.Joiner;

/**
 * Generates doc for the REST API
 *
 */
public class Doc {

  private Set<? extends Class<?>> classes;

  static class ProviderClass {
    private final Class<?> c;

    public ProviderClass(Class<?> c) {
      super();
      this.c = c;
    }
    @Override
    public String toString() {
      return "# Provider: " + c + " " + Arrays.toString(c.getAnnotations());
    }
  }
  class Resources {
    private final Class<?> c;
    private final Path basePath;
    private final PathParam pathParam;
    private final List<Resource> children = new ArrayList<>();

    public Resources(Class<?> c) {
      super();
      this.c = c;
      this.basePath = c.getAnnotation(Path.class);
      this.pathParam = c.getAnnotation(PathParam.class);
      Method[] methods = c.getDeclaredMethods();
      for (Method method : methods) {
        HttpMethod httpMethod = getHttpMethod(method);
        if (httpMethod != null) {
          try{
            Resource resource = new Resource(basePath != null ? basePath.value() : pathParam != null ? "{" + pathParam.value() + "}" : "???", method);
            children.add(resource);
          }catch(Exception ex){
            System.err.println(String.format("[WARNING] Failure while attempting to generate doc for resource %s.", method.toGenericString()));
            ex.printStackTrace(System.err);
          }
        }
      }
      Collections.sort(children, new Comparator<Resource>() {
        @Override
        public int compare(Resource o1, Resource o2) {
          int pathComp;
          Path p1 = o1.methodPath;
          Path p2 = o2.methodPath;
          if (p1 == p2) {
            pathComp = 0;
          } else if (p1 == null) {
            pathComp = -1;
          } else if (p2 == null) {
            pathComp = 1;
          } else {
            pathComp = p1.value().compareTo(p2.value());
          }
          if (pathComp != 0) {
            return pathComp;
          } else {
            return o1.httpMethod.value().compareTo(o2.httpMethod.value());
          }
        }
      });
    }
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("## Resource defined by class " + c.getName() + "\n");
      for (Resource resource : children) {
        sb.append(resource.toString());
      }
      return sb.toString();
    }
  }

  class Resource {

    private final String pathPrefix;
    private final HttpMethod httpMethod;
    private final Path methodPath;
    private final List<Param> params = new ArrayList<>();
    private final List<Param> pathParams = new ArrayList<>();
    private final List<Param> formParams = new ArrayList<>();
    private final List<Param> headerParams = new ArrayList<>();
    private final DataType returnType;
    private final DataType body;

    public Resource(String parentPath, Method method) {
      this.pathPrefix = notNull(parentPath, "parentPath");
      this.httpMethod = notNull(getHttpMethod(method), "httpMethod");
      this.methodPath = method.getAnnotation(Path.class);
      this.returnType = getDataType(method.getGenericReturnType());
      Annotation[][] parameterAnnotations = method.getParameterAnnotations();
      Type[] paramTypes = method.getGenericParameterTypes();
      DataType bodyType = null;
      for (int i = 0; i < paramTypes.length; i++) {
        DataType type = getParamDataType(paramTypes[i]);
        Annotation[] paramAnnotations = parameterAnnotations[i];
        Param p = null;
        String defaultValue = null;
        for (Annotation paramAnnotation : paramAnnotations) {
          if (paramAnnotation instanceof QueryParam) {
            p = new Param(((QueryParam)paramAnnotation).value(), type);
            params.add(p);
          } else if (paramAnnotation instanceof PathParam) {
            p = new Param(((PathParam)paramAnnotation).value(), type);
            pathParams.add(p);
          } else if (paramAnnotation instanceof FormParam) {
            p = new Param(((FormParam)paramAnnotation).value(), type);
            formParams.add(p);
          } else if (paramAnnotation instanceof FormDataParam) {
            // TODO
            p = new Param(((FormDataParam)paramAnnotation).value(), type);
            formParams.add(p);
          } else if (paramAnnotation instanceof HeaderParam) {
            p = new Param(((HeaderParam)paramAnnotation).value(), type);
            headerParams.add(p);
          } else if (paramAnnotation instanceof DefaultValue) {
            defaultValue = ((DefaultValue)paramAnnotation).value();
          } else if (paramAnnotation instanceof Context) {
            // just an injected value. Not part of the api
            p = new Param(null, null);
          } else {
            System.err.println("[WARNING] Unknown param ann: " + paramAnnotation + " on " + i + "th param of " + method);
          }
        }
        if (p == null) {
          if (bodyType != null) {
            System.err.println("[WARNING] More than one body param on " + i + "th param of " + method);
          }
          if (defaultValue != null) {
            System.err.println("[WARNING] default value " + defaultValue + " for body on " + i + "th param of " + method);
          }
          bodyType = getDataType(paramTypes[i]);
        } else {
          p.setDefaultValue(defaultValue);
        }
      }
      this.body = bodyType;
    }

    @Override
    public String toString() {
      String paramsString = params.size() == 0 ? "" : "?" + Joiner.on("&").join(params);
      String pathSuffix = methodPath != null ? methodPath.value() : "";
      if (pathSuffix.length() > 0 && !pathSuffix.startsWith("/") && !pathPrefix.endsWith("/")) {
        pathSuffix = "/" + pathSuffix;
      }
      String fullPath = pathPrefix + pathSuffix;

      StringBuilder sb = new StringBuilder();
      sb.append("\n - ").append(httpMethod.value()).append(" ").append(fullPath).append(paramsString);
      if (pathParams.size() > 0) {
        sb.append(" (path params: ").append(Joiner.on(", ").join(pathParams)).append(")");
      }
      sb.append("   \n");
      if ("PUT".equals(httpMethod.value()) || "POST".equals(httpMethod.value()) || !headerParams.isEmpty() || !formParams.isEmpty()) {
        sb.append("   > `=>`");
        if (body != null) {
          sb.append(" ").append(body.toLinkedString());
        }
        sb.append("   \n");
        for (Param p : headerParams) {
          sb.append("   > ").append(p.name).append(": {").append(p.type).append("}   \n");
        }
        for (Param p : formParams) {
          sb.append("   > ").append(p.name).append("={").append(p.type).append("}   \n");
        }
      }
      sb.append("   > `<=` ").append(returnType.toLinkedString()).append("   \n");
//      sb.append("   >   \n");
      return sb.toString();
    }

  }

  static class Param {

    private final String name;
    private final DataType type;
    private String defaultValue;

    public Param(String name, DataType type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String toString() {
      return name + "={" + type + "}" + (defaultValue == null ? "" : defaultValue);
    }

    public void setDefaultValue(String defaultValue) {
      this.defaultValue = defaultValue;
    }

  }

  class DataType {
    private final Type type;

    public DataType(Type type) {
      super();
      this.type = type;
    }

    public String toLinkedString() {
      String typeName;
      if (type == String.class) {
        typeName = "String";
      } else if (type instanceof Class) {
        Class<?> c = (Class<?>)type;
        if (c.isPrimitive()) {
          typeName = c.getSimpleName();
        } else if (c.isArray()){
          typeName = new DataType(c.getComponentType()).toLinkedString() + "`[]`";
        } else {
          if (rawDataTypes.contains(type)) {
            String anchor = "class-" + c.getName().replaceAll("\\.", "").toLowerCase();
            typeName = "[" + c.getName() + "](#" + anchor + ")";
          } else {
            typeName = c.getName();
          }
        }

      } else if (type instanceof ParameterizedType) {
        ParameterizedType pt = (ParameterizedType)type;
        typeName = new DataType(pt.getRawType()).toLinkedString() + "`<`";
        Type[] actualTypeArguments = pt.getActualTypeArguments();
        boolean first = true;
        for (Type type : actualTypeArguments) {
          if (first) {
            first = false;
          } else {
            typeName += ", ";
          }
          typeName += new DataType(type).toLinkedString();
        }
        typeName += "`>`";
      } else {
        throw new UnsupportedOperationException(type.toString() + " " + type.getClass());
      }
      return typeName;
    }

    @Override
    public String toString() {
      String typeName;
      if (type == String.class) {
        typeName = "String";
      } else if (type instanceof Class) {
        Class<?> c = (Class<?>)type;
        if (c.isPrimitive()) {
          typeName = c.getSimpleName();
        } else if (c.isArray()){
          typeName = new DataType(c.getComponentType()).toString() + "[]";
        } else {
          typeName = c.getName();
        }
      } else if ( type instanceof ParameterizedType) {
        typeName = type.toString();
      } else {
        typeName = type.toString() + " " + type.getClass();
      }
      return typeName;
    }

    public boolean isBeanClass() {
      if (type instanceof Class) {
        Class<?> c = (Class<?>)type;
        return (c != String.class)
          && (c != Response.class)
          && !c.isPrimitive()
          && !c.isArray();
      }
      return false;
    }

    public List<Class<?>> getBeanClasses() {
      if (type == String.class || type == Response.class || type == Object.class) {
        return Collections.emptyList();
      } else if (type instanceof Class) {
        Class<?> c = (Class<?>)type;
        if (c.isPrimitive()) {
          return Collections.emptyList();
        } else if (c.isArray()){
          return new DataType(c.getComponentType()).getBeanClasses();
        } else {
          return Collections.<Class<?>>singletonList(c);
        }
      } else if (type instanceof ParameterizedType) {
        ParameterizedType pt = (ParameterizedType)type;
        Type[] actualTypeArguments = pt.getActualTypeArguments();
        List<Class<?>> all = new ArrayList<>();
        for (Type type : actualTypeArguments) {
          all.addAll(new DataType(type).getBeanClasses());
        }
        return all;
      } else {
        throw new UnsupportedOperationException();
      }
    }

  }

  public Doc(Collection<? extends Class<?>> classes) {
    this.classes = new HashSet<>(classes);
  }

  <T> T notNull(T o, String name) {
    if (o == null) {
      throw new NullPointerException(name + " is null");
    }
    return o;
  }

  HttpMethod getHttpMethod(Method method) {
    for (Annotation annotation : method.getAnnotations()) {
      HttpMethod httpMethod = getAnnotation(annotation, HttpMethod.class);
      if (httpMethod != null) {
        return httpMethod;
      }
    }
    return null;
  }


  private List<Resources> roots = new ArrayList<>();
  private List<ProviderClass> providers = new ArrayList<>();
  private List<Class<?>> others = new ArrayList<>();

  private static final Comparator<Type> typeComparator = new Comparator<Type>() {
    @Override
    public int compare(Type o1, Type o2) {
      return o1.toString().compareTo(o2.toString());
    }
  };

  private Map<Type, DataType> data = new TreeMap<>(typeComparator);
  private Map<Type, DataType> dataParams = new TreeMap<>(typeComparator);

  private Set<Class<?>> rawDataTypes = new TreeSet<>(typeComparator);

  DataType getParamDataType(Type type) {
    if (!dataParams.containsKey(type)) {
      dataParams.put(type, new DataType(type));
    }
    return dataParams.get(type);
  }

  DataType getDataType(Type type) {
    if (!data.containsKey(type)) {
      DataType dataType = new DataType(type);
      data.put(type, dataType);
      rawDataTypes.addAll(dataType.getBeanClasses());
    }
    return data.get(type);
  }

  public static void main(String[] args) throws Exception {
    PrintStream out = System.out;
    if (args.length > 0) {
      File f = new File(args[0]);
      if (!f.getParentFile().exists() || !f.getParentFile().isDirectory()) {
        System.err.println("[WARNING] can not create file: " + f.getAbsolutePath());
        return;
      }
      System.out.println("creating doc file at: " + f.getAbsolutePath());
      out = new PrintStream(f);
    }
    RestServerV2 restServerV2 = new RestServerV2(ClassPathScanner.fromPrescan(SabotConfig.create()));
    restServerV2.property(RestServerV2.FIRST_TIME_API_ENABLE, true);
    restServerV2.property(RestServerV2.TEST_API_ENABLE, true);
    restServerV2.property(RestServerV2.ERROR_STACKTRACE_ENABLE, false);
    Doc docv2 = new Doc(restServerV2.getClasses());
    docv2.generateDoc("V2", out);
  }

  void generateDoc(String linkPrefix, PrintStream out)
    throws JsonMappingException, JsonProcessingException {
    Set<Class<?>> classes = new TreeSet<>(new Comparator<Class<?>>() {
      @Override
      public int compare(Class<?> o1, Class<?> o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    classes.addAll(this.classes);
    for (Class<?> c : classes) {
      Provider provider = c.getAnnotation(Provider.class);
      if (provider != null) {
//      TODO
        ProviderClass providerClass = new ProviderClass(c);
        providers.add(providerClass);
      } else if (ContainerResponseFilter.class.isAssignableFrom(c)) {
        // TODO
        others.add(c);
      } else if (ContainerRequestFilter.class.isAssignableFrom(c)) {
        // TODO
        others.add(c);
      } else if (MessageBodyReader.class.isAssignableFrom(c)) {
        // TODO
        others.add(c);
      } else if (MessageBodyWriter.class.isAssignableFrom(c)) {
        // TODO
        others.add(c);
      } else if (Feature.class.isAssignableFrom(c)) {
        // TODO
        others.add(c);
      } else if (ExceptionMapper.class.isAssignableFrom(c)) {
        // TODO
        others.add(c);
      } else if (RolesAllowedDynamicFeature.class.isAssignableFrom(c)) {
        others.add(c);
      } else {
        Resources resources = new Resources(c);
        roots.add(resources);
      }
    }
//    System.err.println("instances");
//    Set<Object> instances = restServerV1.getInstances();
//    for (Object instance : instances) {
//      System.err.println(instance);
//    }
//    System.err.println("singletons");
//    Set<Object> singletons = restServerV1.getSingletons();
//    for (Object singleton : singletons) {
//      System.err.println(singleton);
//    }

    Collections.sort(roots, new Comparator<Resources>() {
      @Override
      public int compare(Resources o1, Resources o2) {
        if (o1.basePath == null && o2.basePath == null) {
          return 0;
        } else if (o1.basePath == null) {
          return -1;
        } else if (o2.basePath == null) {
          return 1;
        }
        return o1.basePath.value().compareTo(o2.basePath.value());
      }
    });

    out.println("# TOC");
    out.println(" - [Resources](#"+linkPrefix.toLowerCase()+"-resources)");
    out.println(" - [JobData](#"+linkPrefix.toLowerCase()+"-data)");
    out.println(" - [Others](#"+linkPrefix.toLowerCase()+"-others)");
    out.println();

    out.println("#"+linkPrefix+" Resources: ");
    for (Resources resources : roots) {
      out.println(resources);
      out.println();
    }
    out.println();

    ObjectMapper om = JSONUtil.prettyMapper();
    om.enable(SerializationFeature.INDENT_OUTPUT);
    out.println("#"+linkPrefix+" JobData");
    for (final Class<?> type : rawDataTypes) {
      out.println("## `" + type + "`");
      try {
        SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();
        om.acceptJsonFormatVisitor(om.constructType(type), visitor);
        JsonSchema schema = visitor.finalSchema();
        out.println("- Example:");
        out.println("```");
        out.println(example(schema));
        out.println("```");
// Schemas are inconsistent in the ordering of refs and example is easier to read
//        out.println("- Schema:");
//        out.println("```");
//        out.println(om.enable(ORDER_MAP_ENTRIES_BY_KEYS).writeValueAsString(schema));
//        out.println("```");
      } catch (RuntimeException e) {
        throw new RuntimeException("Error generating schema for " + type, e);
      }
      out.println();
    }
    out.println();

    out.println("#"+linkPrefix+" Others: ");
    for (Class<?> other : others) {
      out.println("## " + other);
    }
  }

  private void findRefs(JsonSchema schema, Map<String, JsonSchema> refs, Set<String> referenced) {
    addRef(schema, refs);
    if (schema instanceof ReferenceSchema) {
      referenced.add(schema.get$ref());
    } else if (schema.isArraySchema()) {
      ArraySchema as = schema.asArraySchema();
      if (as.getItems() != null) {
        if (as.getItems().isSingleItems()) {
          findRefs(as.getItems().asSingleItems().getSchema(), refs, referenced);
        } else if (as.getItems().isArrayItems()) {
          ArrayItems items = as.getItems().asArrayItems();
          for (JsonSchema item : items.getJsonSchemas()) {
            findRefs(item, refs, referenced);
          }
        } else {
          throw new UnsupportedOperationException(as.getItems().toString());
        }
      }
    } else if (schema.isObjectSchema()) {
      ObjectSchema os = schema.asObjectSchema();
      for (JsonSchema value : os.getProperties().values()) {
        findRefs(value, refs, referenced);
      }
    }
  }

  private String example(JsonSchema schema) {
    StringBuilder sb = new StringBuilder();
    Map<String, JsonSchema> refs = new HashMap<>();
    Set<String> referenced = new HashSet<>();
    findRefs(schema, refs, referenced);
    example(sb, 16384, "", schema, refs, new HashSet<String>(), referenced);
    return sb.toString();
  }

  private void shortId(StringBuilder sb, JsonSchema s) {
    if (s.getId() != null) {
      sb.append(" /** ").append(shortId(s.getId())).append(" **/");
    }
  }

  private String shortId(String s) {
    String[] split = s.split(":");
    return split[split.length - 1];
  }

  /**
   * generates a json example
   * @param sb the output
   * @param indent the current indent prefix
   * @param schema the schema to print an example for
   * @param refs for referenced schema (when used more than once)
   * @param followed to avoid infinite loops in recursive schema
   * @param referenced the types that are referenced and should have an annotation
   */
  private void example(StringBuilder sb, int maxlength, String indent, JsonSchema schema, Map<String, JsonSchema> refs, Set<String> followed, Set<String> referenced) {
    if (sb.length() > maxlength) {
      sb.append("example is too big, truncated...");
      return;
    }
    followed = new HashSet<>(followed);
    String id = schema.getId();
    if (id == null && schema instanceof ReferenceSchema) {
      id = schema.get$ref();
    }
    if (id != null && followed.contains(id)) {
      sb.append(refExample(id));
      return;
    }
    if (schema.isObjectSchema()) {
      followed.add(id);
      objectExample(sb, maxlength, indent, schema, refs, followed, referenced, id);
    } else if (schema.isArraySchema()) {
      arrayExample(sb, maxlength, indent, schema, refs, followed, referenced);
    } else if (schema instanceof ReferenceSchema) {
      if (refs.containsKey(schema.get$ref())) {
        example(sb, maxlength, indent, refs.get(schema.get$ref()), refs, followed, referenced);
      } else {
        sb.append(refExample(schema.get$ref()));
      }
    } else {
      sb.append(primitiveTypeExample(schema));
    }
  }

  private void objectExample(StringBuilder sb, int maxlength, String indent, JsonSchema schema,
      Map<String, JsonSchema> refs, Set<String> followed, Set<String> referenced, String id) {
    sb.append("{");
    if (referenced.contains(id)) {
      shortId(sb, schema);
    }
    ObjectSchema os = schema.asObjectSchema();
    if (os.getProperties().isEmpty()) {
      AdditionalProperties additionalProperties = os.getAdditionalProperties();
      if (additionalProperties instanceof SchemaAdditionalProperties) {
        sb.append("\n").append(indent).append("  ").append("abc").append(": ");
        example(sb, maxlength, indent + "  ", ((SchemaAdditionalProperties) additionalProperties).getJsonSchema(), refs, followed, referenced);
        sb.append(", ...");
      }
    }
    Map<String, JsonSchema> props = new TreeMap<>(os.getProperties());
    for (Entry<String, JsonSchema> entry : props.entrySet()) {
      sb.append("\n").append(indent).append("  ").append(entry.getKey()).append(": ");
      example(sb, maxlength, indent + "  ", entry.getValue(), refs, followed, referenced);
      sb.append(",");
    }
    sb.append("\n").append(indent).append("}");
  }

  private String refExample(String id) {
    return "(ref: " + shortId(id) + ")";
  }

  private void arrayExample(StringBuilder sb, int maxlength, String indent, JsonSchema schema,
      Map<String, JsonSchema> refs, Set<String> followed, Set<String> referenced) {
    sb.append("[\n").append(indent).append("  ");
    ArraySchema as = schema.asArraySchema();
    if (as.getItems() == null) {
      sb.append(" ... ]");
    } else if (as.getItems().isSingleItems()) {
      example(sb, maxlength, indent + "  ", as.getItems().asSingleItems().getSchema(), refs, followed, referenced);
      sb.append(",\n").append(indent).append("  ...\n").append(indent).append("]");
    } else if (as.getItems().isArrayItems()) {
      ArrayItems items = as.getItems().asArrayItems();
      for (JsonSchema item : items.getJsonSchemas()) {
        sb.append("\n").append(indent);
        example(sb, maxlength, indent + "  ", item, refs, followed, referenced);
        sb.append(",");
      }
      sb.append("]");
    } else {
      throw new UnsupportedOperationException(as.getItems().toString());
    }
  }

  private String primitiveTypeExample(JsonSchema schema) {
    switch (schema.getType()) {
    case ANY:
      return "any";
    case ARRAY:
      return "[ ... ]";
    case BOOLEAN:
     return "true | false";
    case INTEGER:
      return "1";
    case NULL:
      return "null";
    case NUMBER:
      return "1.0";
    case OBJECT:
      return "{ ... }";
    case STRING:
      Set<String> enums = schema.asStringSchema().getEnums();
      if (enums != null && !enums.isEmpty()) {
        List<String> quoted = new ArrayList<>();
        for (String en : enums) {
          quoted.add("\"" + en + "\"");
        }
        return Joiner.on(" | ").join(quoted);
      } else {
        return "\"abc\"";
      }
    default:
      return "? " + schema.getType();
    }
  }

  private void addRef(JsonSchema schema, Map<String, JsonSchema> refs) {
    if (schema.getId() != null && !refs.containsKey(schema.getId())) {
      refs.put(schema.getId(), schema);
    }
  }

  /**
   * Check if the provided annotation is annotated with annotationClass
   * @param <A>
   * @param annotation
   * @param annotationClass
   * @return
   */
  static <A extends Annotation> A getAnnotation(Annotation annotation, Class<A> annotationClass) {
    A[] result = annotation.annotationType().getAnnotationsByType(annotationClass);
    Preconditions.checkArgument(result.length < 2, "more than one annotation of type %s: %s",
        annotationClass.getSimpleName(), Arrays.toString(result));

    return result.length != 0 ? result[0] : null;
  }
}
