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
package com.dremio.service.functions.generator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.DremioEmptyScope;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.expr.fn.PrimaryFunctionRegistry;
import com.dremio.exec.planner.sql.DremioSqlConformance;
import com.dremio.exec.planner.sql.SqlFunctionImpl;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.service.functions.model.Function;
import com.dremio.service.functions.model.FunctionSignature;
import com.dremio.service.functions.model.Parameter;
import com.dremio.service.functions.model.ParameterKind;
import com.dremio.service.functions.model.ParameterType;
import com.dremio.service.functions.model.SqlTypeNameToParameterType;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Factory to create a function from a SqlFunction.
 */
public final class FunctionFactory {
  private static final FunctionImplementationRegistry FUNCTION_IMPLEMENTATION_REGISTRY = createFunctionImplementationRegistry();
  private static final MockDremioTable MOCK_DREMIO_TABLE = createMockTable();
  private static final DremioCatalogReader MOCK_CATALOG_READER = createCatalogReader(MOCK_DREMIO_TABLE);

  private final SqlValidatorImpl validator;
  private final SqlValidatorScope scope;

  private FunctionFactory(SqlValidatorImpl validator, SqlValidatorScope scope) {
    Preconditions.checkNotNull(validator);
    Preconditions.checkNotNull(scope);

    this.validator = validator;
    this.scope = scope;
  }

  public static FunctionFactory makeFunctionFactory(SqlOperatorTable sqlOperatorTable) {
    MockValidator validator = new MockValidator(
      sqlOperatorTable,
      MOCK_CATALOG_READER);

    MockScope scope = new MockScope(validator);

    return new FunctionFactory(validator, scope);
  }

  public Function fromSqlFunction(SqlFunction sqlFunction) {
    Preconditions.checkNotNull(sqlFunction);
    Optional<ImmutableList<FunctionSignature>> optionalFunctionSignatures = tryGetSignatures(sqlFunction);
    return Function.builder()
      .name(sqlFunction.getName().toUpperCase())
      .addAllSignatures(optionalFunctionSignatures.orElse(ImmutableList.of()))
      .build();
  }

  private Optional<ImmutableList<FunctionSignature>> tryGetSignatures(SqlFunction sqlFunction) {
    return firstSuccess(
      sqlFunction,
      this::tryGetSignaturesFromGetParamTypes,
      this::tryGetSignaturesFromGetAllowedSignatures,
      this::tryGetSignaturesFromFunctionRegistry,
      this::tryGetSignaturesFromCheckOperandTypes)
      .map(ImmutableList::copyOf);
  }

  private static Optional<ImmutableSet<FunctionSignature>> firstSuccess(
    SqlFunction sqlFunction,
    java.util.function.Function<SqlFunction, Optional<ImmutableSet<FunctionSignature>>> ... functions) {
    for (java.util.function.Function<SqlFunction, Optional<ImmutableSet<FunctionSignature>>> function : functions) {
      Optional<ImmutableSet<FunctionSignature>> optional = function.apply(sqlFunction);
      if (optional.isPresent() && !optional.get().isEmpty()) {
        return optional;
      }
    }

    return Optional.empty();
  }

  private Optional<ImmutableSet<FunctionSignature>> tryGetSignaturesFromGetParamTypes(SqlFunction sqlFunction) {
    try {
      List<RelDataType> paramTypes = sqlFunction.getParamTypes();
      if (paramTypes == null) {
        return Optional.empty();
      }

      ParameterType returnType = SqlTypeNameToParameterType.convert(
        sqlFunction
          .inferReturnType(JavaTypeFactoryImpl.INSTANCE, paramTypes)
          .getSqlTypeName());
      ImmutableList<Parameter> parameters = paramTypes
        .stream()
        .map(RelDataType::getSqlTypeName)
        .map(type -> Parameter
          .builder()
          .kind(ParameterKind.REGULAR)
          .type(SqlTypeNameToParameterType.convert(type))
          .name("<PARAMETER NAME>")
          .build())
        .collect(ImmutableList.toImmutableList());
      FunctionSignature functionSignature = FunctionSignature
        .builder()
        .returnType(returnType)
        .addAllParameters(parameters)
        .build();
      return Optional.of(ImmutableSet.of(functionSignature));
    } catch (Exception ex) {
      return Optional.empty();
    }
  }

  private Optional<ImmutableSet<FunctionSignature>> tryGetSignaturesFromGetAllowedSignatures(SqlFunction sqlFunction) {
    try {
      String allowedSignaturesString = sqlFunction.getAllowedSignatures();
      if (allowedSignaturesString.contains("Dremio") || allowedSignaturesString.contains("*")) {
        return Optional.empty();
      }

      String[] allowedSignatures = allowedSignaturesString.split("\\r?\\n");
      // FUNCTION_NAME(<PARAM1_TYPE> <PARAM2_TYPE>)\nFUNCTION_NAME(<PARAM1_TYPE> <PARAM2_TYPE> <PARAM3_TYPE>)
      ImmutableSet.Builder<FunctionSignature> functionSignatureBuilder = new ImmutableSet.Builder<>();
      for (String allowedSignature : allowedSignatures) {
        ImmutableList.Builder<SqlTypeName> operandTypesBuilder = new ImmutableList.Builder<>();

        int startIndex = 0;
        for (int i = 0; i < allowedSignature.length(); i++) {
          char character = allowedSignature.charAt(i);
          switch (character) {
          case '<':
            startIndex = i;
            break;

          case '>':
            String operandTypeString = allowedSignature.substring(startIndex + 1, i);
            SqlTypeName sqlTypeName = SqlTypeName.valueOf(operandTypeString);
            operandTypesBuilder.add(sqlTypeName);
            break;

          default:
            // Do Nothing
            break;
          }
        }

        ImmutableList<SqlTypeName> operandTypes = operandTypesBuilder.build();
        SqlNodeList sqlNodeList = SqlNodeLists.VALUES.get(operandTypes);
        if (sqlNodeList == null) {
          return Optional.empty();
        }

        SqlCall sqlCall = sqlFunction.createCall(sqlNodeList);
        ParameterType returnType = SqlTypeNameToParameterType.convert(
          sqlFunction
            .deriveType(
              validator,
              scope,
              sqlCall)
            .getSqlTypeName());

        ImmutableList<Parameter> parameters = operandTypes
          .stream()
          .map(type -> Parameter
            .builder()
            .kind(ParameterKind.REGULAR)
            .type(SqlTypeNameToParameterType.convert(type))
            .name("<PARAMETER NAME>")
            .build())
          .collect(ImmutableList.toImmutableList());

        FunctionSignature functionSignature = FunctionSignature.builder()
          .returnType(returnType)
          .addAllParameters(parameters)
          .build();
        functionSignatureBuilder.add(functionSignature);
      }

      return Optional.of(functionSignatureBuilder.build());
    } catch (Error | Exception e) {
      // Not every function implements this shortcut
      return Optional.empty();
    }
  }

  private Optional<ImmutableSet<FunctionSignature>> tryGetSignaturesFromFunctionRegistry(SqlFunction sqlFunction) {
    if (!(sqlFunction instanceof SqlFunctionImpl)) {
      return Optional.empty();
    }

    SqlFunctionImpl sqlFunctionImpl = (SqlFunctionImpl) sqlFunction;
    PrimaryFunctionRegistry primaryFunctionRegistry;
    switch (sqlFunctionImpl.getSource()) {
    case JAVA:
      primaryFunctionRegistry = FUNCTION_IMPLEMENTATION_REGISTRY.getJavaFunctionRegistry();
      break;

    case GANDIVA:
      primaryFunctionRegistry = FUNCTION_IMPLEMENTATION_REGISTRY.getGandivaFunctionRegistry();
      break;

    default:
      return Optional.empty();
    }

    List<AbstractFunctionHolder> abstractFunctionHolders = primaryFunctionRegistry.getMethods(sqlFunction.getName());
    if (abstractFunctionHolders == null) {
      return Optional.empty();
    }

    Set<FunctionSignature> signatures = new HashSet<>();
    for (AbstractFunctionHolder abstractFunctionHolder : abstractFunctionHolders) {
      try {
        ParameterType returnType = SqlTypeNameToParameterType.convert(
          TypeInferenceUtils
            .getCalciteTypeFromMinorType(
              getReturnType(abstractFunctionHolder).toMinorType()));
        if (returnType == ParameterType.ANY) {
          continue;
        }

        ImmutableList<Parameter> parameters = IntStream
          .range(0, abstractFunctionHolder.getParamCount())
          .mapToObj(abstractFunctionHolder::getParamType)
          .map(CompleteType::toMinorType)
          .map(TypeInferenceUtils::getCalciteTypeFromMinorType)
          .map(SqlTypeNameToParameterType::convert)
          .map(parameterType -> Parameter
            .builder()
            .kind(ParameterKind.REGULAR)
            .type(parameterType)
            .name("<PARAMETER NAME>")
            .build())
          .collect(ImmutableList.toImmutableList());

        if (parameters.stream().anyMatch(parameter -> parameter.getType() == ParameterType.ANY)) {
          continue;
        }

        FunctionSignature functionSignature = FunctionSignature.builder()
          .returnType(returnType)
          .addAllParameters(parameters)
          .build();

        signatures.add(functionSignature);
      } catch (Exception ex) {
        continue;
      }
    }

    return Optional.of(ImmutableSet.copyOf(signatures));
  }

  private static CompleteType getReturnType(AbstractFunctionHolder abstractFunctionHolder) {
    if (!(abstractFunctionHolder instanceof BaseFunctionHolder)) {
      return abstractFunctionHolder.getReturnType(ImmutableList.of());
    }

    BaseFunctionHolder baseFunctionHolder = (BaseFunctionHolder) abstractFunctionHolder;
    return baseFunctionHolder.getReturnValue().getType();
  }

  private Optional<ImmutableSet<FunctionSignature>> tryGetSignaturesFromCheckOperandTypes(SqlFunction sqlFunction) {
    SqlOperandCountRange sqlOperandCountRange;
    try {
      sqlOperandCountRange = sqlFunction.getOperandCountRange();
    } catch (Exception ex) {
      return Optional.empty();
    }

    boolean hasVarArg = (sqlOperandCountRange.getMax() < 0) || (sqlOperandCountRange.getMax() == Integer.MAX_VALUE);
    int maxOperands =  hasVarArg ? sqlOperandCountRange.getMin() + 1: sqlOperandCountRange.getMax();
    if (maxOperands > ParameterCombinations.MAX_OPERANDS) {
      // It's going to take too long, so just give up
      return Optional.empty();
    }

    int minOperands = sqlOperandCountRange.getMin();
    Set<FunctionSignature> functionSignatures = new HashSet<>();
    for (int operandCount = minOperands; operandCount <= maxOperands; operandCount++) {
      ImmutableSet<ImmutableList<SqlTypeName>> parameterTypeCombinations = ParameterCombinations.get(operandCount);
      for (ImmutableList<SqlTypeName> parameterTypes : parameterTypeCombinations) {
        SqlNodeList sqlNodeList = SqlNodeLists.VALUES.get(parameterTypes);
        assert sqlNodeList != null;
        SqlCall sqlCall;
        try {
          sqlCall = sqlFunction.createCall(sqlNodeList);
        } catch (Error | Exception e) {
          // Some functions like SqlTrimFunction don't implement createCall
          continue;
        }

        SqlCallBinding sqlCallBinding = new SqlCallBinding(
          validator,
          scope,
          sqlCall);

        boolean validOperandTypes;
        try {
          validOperandTypes = sqlFunction.checkOperandTypes(sqlCallBinding, false);
        } catch (Error | Exception e) {
          // For whatever reason some functions don't know how to handle MAP types
          validOperandTypes = false;
        }

        if (validOperandTypes) {
          ParameterType returnType;
          try {
            returnType = SqlTypeNameToParameterType.convert(
              sqlFunction
                .deriveType(
                  validator,
                  scope,
                  sqlCall)
                .getSqlTypeName());
          } catch (Error | Exception e) {
            // For whatever reason some functions always return true for checkOperandTypes, but throw an exception for deriveType
            continue;
          }

          List<Parameter> parameters = new ArrayList<>();
          for (int i = 0; i < parameterTypes.size(); i++) {
            SqlTypeName sqlTypeName = parameterTypes.get(i);
            ParameterKind kind = hasVarArg && i == (parameterTypes.size() - 1) ? ParameterKind.VARARG : ParameterKind.REGULAR;
            ParameterType type = SqlTypeNameToParameterType.convert(sqlTypeName);
            Parameter parameter = Parameter
              .builder()
              .kind(kind)
              .type(type)
              .name("<PARAMETER NAME>")
              .build();
            parameters.add(parameter);
          }

          FunctionSignature functionSignature = FunctionSignature.builder()
            .returnType(returnType)
            .addAllParameters(parameters)
            .build();

          functionSignatures.add(functionSignature);
        }
      }
    }

    return Optional.of(ImmutableSet.copyOf(functionSignatures));
  }

  private static  FunctionImplementationRegistry createFunctionImplementationRegistry() {
    SabotConfig sabotConfig = SabotConfig.create();
    ScanResult scanResult = ClassPathScanner.fromPrescan(sabotConfig);
    return FunctionImplementationRegistry.create(
      sabotConfig,
      scanResult);
  }


  private static MockDremioTable createMockTable() {
    RelDataTypeFactory.FieldInfoBuilder builder = JavaTypeFactoryImpl.INSTANCE.builder();

    for (int index = 0; index < ColumnAndNode.ALL_VALUES.size(); index++) {
      ColumnAndNode columnAndNode = ColumnAndNode.ALL_VALUES.get(index);
      SqlTypeName sqlTypeName = columnAndNode.getColumn().getType();
      RelDataType relDataType = JavaTypeFactoryImpl.INSTANCE.createSqlType(sqlTypeName);
      relDataType = JavaTypeFactoryImpl.INSTANCE.createTypeWithNullability(relDataType, true);
      RelDataTypeField field = new RelDataTypeFieldImpl(
        columnAndNode.getColumn().getName(),
        index,
        relDataType);

      builder.add(field);
    }

    return MockDremioTable.create(
      new NamespaceKey(ImmutableList.of("MOCK_TABLE")),
      builder.build());
  }

  private static DremioCatalogReader createCatalogReader(MockDremioTable dremioTable) {
    MockCatalog mockCatalog = new MockCatalog(
      JavaTypeFactoryImpl.INSTANCE,
      ImmutableList.of(dremioTable));
    DremioCatalogReader mockCatalogReader = new DremioCatalogReader(
      mockCatalog,
      JavaTypeFactoryImpl.INSTANCE);

    return mockCatalogReader;
  }

  public static final class ColumnAndNode {
    // BOOLEAN
    public static final ColumnAndNode BOOLEAN = create(SqlTypeName.BOOLEAN);
    // BINARY
    public static final ColumnAndNode BINARY = create(SqlTypeName.BINARY);
    public static final ColumnAndNode VARBINARY = create(SqlTypeName.VARBINARY);
    // NUMERIC
    public static final ColumnAndNode FLOAT = create(SqlTypeName.FLOAT);
    public static final ColumnAndNode DECIMAL = create(SqlTypeName.DECIMAL);
    public static final ColumnAndNode DOUBLE = create(SqlTypeName.DOUBLE);
    public static final ColumnAndNode INTEGER = create(SqlTypeName.INTEGER);
    public static final ColumnAndNode BIGINT = create(SqlTypeName.BIGINT);
    // STRING
    public static final ColumnAndNode CHAR = create(SqlTypeName.CHAR);
    public static final ColumnAndNode VARCHAR = create(SqlTypeName.VARCHAR);
    // DATE AND TIME
    public static final ColumnAndNode DATE = create(SqlTypeName.DATE);
    public static final ColumnAndNode TIME = create(SqlTypeName.TIME);
    public static final ColumnAndNode TIMESTAMP = create(SqlTypeName.TIMESTAMP);
    public static final ColumnAndNode INTERVAL_DAY_SECOND = create(SqlTypeName.INTERVAL_DAY_SECOND);
    public static final ColumnAndNode INTERVAL_YEAR_MONTH = create(SqlTypeName.INTERVAL_YEAR_MONTH);
    // LIST
    public static final ColumnAndNode ARRAY = create(SqlTypeName.ARRAY);
    // STRUCT
    public static final ColumnAndNode MAP = create(SqlTypeName.MAP);

    public static final ImmutableList<ColumnAndNode> ALL_VALUES = ImmutableList.of(
      BOOLEAN,
      BINARY, VARBINARY,
      FLOAT, DECIMAL, DOUBLE, INTEGER, BIGINT,
      CHAR, VARCHAR,
      DATE, TIME, TIMESTAMP/*, INTERVAL_DAY_SECOND, INTERVAL_YEAR_MONTH,
      ARRAY,
      MAP*/);
    public static final ImmutableMap<SqlTypeName, SqlNode> TYPE_TO_NODE = createTypeToNode();

    private final Column column;
    private final SqlNode node;

    private ColumnAndNode(Column column, SqlNode node) {
      this.column = column;
      this.node = node;
    }

    public Column getColumn() {
      return column;
    }

    public SqlNode getNode() {
      return node;
    }

    private static ColumnAndNode create(SqlTypeName sqlTypeName) {
      String columnName = sqlTypeName.toString() + "_COLUMN";
      Column column = Column.typedColumn(columnName, sqlTypeName);
      SqlNode node = new SqlIdentifier(ImmutableList.of("MOCK_TABLE", columnName), SqlParserPos.ZERO);

      return new ColumnAndNode(column, node);
    }

    private static ImmutableMap<SqlTypeName, SqlNode> createTypeToNode() {
      ImmutableMap.Builder<SqlTypeName, SqlNode> builder = new ImmutableMap.Builder<>();
      for (ColumnAndNode columnAndNode : ALL_VALUES) {
        builder.put(columnAndNode.getColumn().getType(), columnAndNode.node);
      }

      return builder.build();
    }
  }

  private static final class ParameterCombinations {
    public static final ImmutableSet<SqlTypeName> POSSIBLE_TYPES = ColumnAndNode
      .ALL_VALUES
      .stream()
      .map(columnAndNode -> columnAndNode.getColumn().getType())
      .collect(ImmutableSet.toImmutableSet());
    public static final ImmutableSet<ImmutableList<SqlTypeName>> LENGTH0 = ImmutableSet.of(ImmutableList.of());
    public static final ImmutableSet<ImmutableList<SqlTypeName>> LENGTH1 = crossProduct(POSSIBLE_TYPES);
    public static final ImmutableSet<ImmutableList<SqlTypeName>> LENGTH2 = crossProduct(POSSIBLE_TYPES, POSSIBLE_TYPES);
    public static final ImmutableSet<ImmutableList<SqlTypeName>> LENGTH3 = crossProduct(POSSIBLE_TYPES, POSSIBLE_TYPES, POSSIBLE_TYPES);
    public static final Integer MAX_OPERANDS = LENGTH3.stream().findFirst().get().size();

    private static ImmutableSet<ImmutableList<SqlTypeName>> crossProduct(Set<? extends SqlTypeName>... sets) {
      return Sets.cartesianProduct(sets)
        .stream()
        .map(ImmutableList::copyOf)
        .collect(ImmutableSet.toImmutableSet());
    }

    public static ImmutableSet<ImmutableList<SqlTypeName>> get(int length) {
      switch (length) {
      case 0:
        return LENGTH0;

      case 1:
        return LENGTH1;

      case 2:
        return LENGTH2;

      case 3:
        return LENGTH3;

      default:
        throw new UnsupportedOperationException("not a supported length");
      }
    }
  }

  private static final class SqlNodeLists {
    public static final ImmutableMap<ImmutableList<SqlTypeName>, SqlNodeList> VALUES = createMap();

    private static ImmutableMap<ImmutableList<SqlTypeName>, SqlNodeList> createMap() {
      ImmutableMap.Builder<ImmutableList<SqlTypeName>, SqlNodeList> builder = new ImmutableMap.Builder<>();
      for (int length = 0; length <= ParameterCombinations.MAX_OPERANDS; length++) {
        ImmutableSet<ImmutableList<SqlTypeName>> combinations = ParameterCombinations.get(length);
        for (ImmutableList<SqlTypeName> combination : combinations) {
          SqlNodeList list = createList(combination);
          builder.put(combination, list);
        }
      }

      return builder.build();
    }

    private static SqlNodeList createList(ImmutableList<SqlTypeName> sqlTypeNames) {
      List<SqlNode> sqlNodes = new ArrayList<>();
      for (SqlTypeName sqlTypeName : sqlTypeNames) {
        SqlNode sqlNode = convertTypeToNode(sqlTypeName);
        sqlNodes.add(sqlNode);
      }

      return new SqlNodeList(sqlNodes, SqlParserPos.ZERO);
    }

    private static SqlNode convertTypeToNode(SqlTypeName sqlTypeName) {
      return ColumnAndNode
        .TYPE_TO_NODE
        .get(sqlTypeName);
    }
  }

  private static final class MockValidator extends com.dremio.exec.planner.sql.SqlValidatorImpl {
    protected MockValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader) {
      super(
        new com.dremio.exec.planner.sql.SqlValidatorImpl.FlattenOpCounter(),
        opTab,
        catalogReader,
        JavaTypeFactoryImpl.INSTANCE,
        DremioSqlConformance.INSTANCE,
        null);
    }

    @Override
    public Config config() {
      return super.config().withTypeCoercionEnabled(false);
    }
  }

  private static final class MockScope extends DremioEmptyScope {
    public MockScope(com.dremio.exec.planner.sql.SqlValidatorImpl validator) {
      super(validator);
    }

    @Override
    public RelDataType resolveColumn(String name, SqlNode ctx) {
      SqlIdentifier identifier = (SqlIdentifier) ctx;
      DremioCatalogReader dremioCatalogReader = (DremioCatalogReader) this.validator.getCatalogReader();
      RelDataType tableSchema = dremioCatalogReader
        .getTableSchema(identifier.names.subList(0, identifier.names.size() - 1))
        .get();

      return tableSchema;
    }
  }

  /**
   * Column datatype.
   */
  private static final class Column {
    private final String name;
    private final SqlTypeName type;

    private Column(String name, SqlTypeName type) {
      Preconditions.checkNotNull(name);
      Preconditions.checkNotNull(type);

      this.name = name;
      this.type = type;
    }

    public static Column typedColumn(String name, SqlTypeName type) {
      return new Column(name, type);
    }

    public static Column anyTypeColumn(String name) {
      return new Column(name, SqlTypeName.ANY);
    }

    public String getName() {
      return name;
    }

    public SqlTypeName getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Column column = (Column) o;
      return name.equals(column.name) && type == column.type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }

    @Override
    public String toString() {
      return "Column{" +
        "name='" + name + '\'' +
        ", type=" + type +
        '}';
    }
  }
}
