/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Class for JsonTableColumn definition.
 */
public abstract class SqlJsonTableColumn extends SqlCall {

  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

  protected final SqlIdentifier name;

  protected Map<String, RelDataType> resolvedFields;

  protected SqlJsonTableColumn(SqlParserPos pos, SqlIdentifier name) {
    super(pos);
    this.name = name;
  }

  public abstract Map<String, RelDataType> resolveFields(
      RelDataTypeFactory typeFactory,
      SqlValidator validator);

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public static class SqlJsonTableColumnClause extends SqlJsonTableColumn {

    private final List<SqlJsonTableColumn> columns;

    public SqlJsonTableColumnClause(SqlParserPos pos, List<SqlNode> columns) {
      super(pos, new SqlIdentifier("ColumnClause", pos));
      this.columns = columns.stream()
          .map(SqlJsonTableColumn.class::cast).collect(Collectors.toList());
    }

    @Override
    public List<SqlNode> getOperandList() {
      return this.columns.stream().map(SqlNode.class::cast).collect(Collectors.toList());
    }

    @Override
    public Map<String, RelDataType> resolveFields(RelDataTypeFactory typeFactory,
        SqlValidator validator) {
      resolvedFields = columns.stream().map(column -> column.resolveFields(
              typeFactory, validator).entrySet())
          .flatMap(Collection::stream)
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
              (x, y) -> {
                throw new UnsupportedOperationException(
                    "there must container duplicated column names: " + x);
              },
              LinkedHashMap::new));
      return resolvedFields;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.keyword("COLUMNS");
      final SqlWriter.Frame columnsFrame = writer.startList("(", ")");
      for (SqlNode column : columns) {
        writer.sep(",", false);
        column.unparse(writer, 0, 0);
      }
      writer.endList(columnsFrame);
    }
  }

  public static class SqlJsonTableOrdinalityColumn extends SqlJsonTableColumn {

    public SqlJsonTableOrdinalityColumn(SqlParserPos pos, SqlIdentifier name) {
      super(pos, name);
    }

    @Override
    public Map<String, RelDataType> resolveFields(
        RelDataTypeFactory typeFactory,
        SqlValidator validator) {
      resolvedFields = Collections.singletonMap(
          name.getSimple(), typeFactory.createSqlType(SqlTypeName.INTEGER));
      return resolvedFields;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return Collections.singletonList(name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      name.unparse(writer, 0, 0);
      writer.keyword("FOR ORDINALITY");
    }
  }

  public static class SqlJsonTableRegularColumn extends SqlJsonTableColumn {

    private final List<SqlNode> columnInfos;

    public SqlJsonTableRegularColumn(SqlParserPos pos, List<SqlNode> columnInfos) {
      super(pos, (SqlIdentifier) columnInfos.get(0));

      assert columnInfos.size() >= 2;
      final List<SqlNode> newColumnInfos = new ArrayList<>();
      newColumnInfos.add(columnInfos.get(0));
      newColumnInfos.add(columnInfos.get(1));
      List<SqlNode> leftColumnInfos = Util.skip(columnInfos, 2);
      SqlNode pathSpec = SqlLiteral.createCharString(
          SqlJsonTableColumn.buildJsonPathSpec(
              ((SqlIdentifier) columnInfos.get(0)).getSimple()),
          pos);
      ;
      SqlNode columnEmptyBehavior =
          SqlJsonTableColumnEmptyOrErrorBehavior.NULL.symbol(pos);
      SqlNode columnErrorBehavior =
          SqlJsonTableColumnEmptyOrErrorBehavior.NULL.symbol(pos);
      SqlNode defaultErrorValue = null;
      SqlNode defaultEmptyValue = null;

      if (leftColumnInfos.size() > 0) {
        SqlNode columnInfo = leftColumnInfos.get(0);
        if (columnInfo instanceof SqlIdentifier) {
          pathSpec = columnInfo;
        }
        for (int i = 0; i < leftColumnInfos.size(); i++) {
          int defaultSymbolIdx = i - 2;
          SqlLiteral columnInfoVal = (SqlLiteral) leftColumnInfos.get(i);
          if (columnInfoVal.getValue() == SqlJsonEmptyOrError.EMPTY) {
            if (defaultSymbolIdx >= 0 &&
                leftColumnInfos.get(defaultSymbolIdx) instanceof SqlLiteral) {
              SqlLiteral tmpLiteral = (SqlLiteral) leftColumnInfos.get(defaultSymbolIdx);
              if (tmpLiteral.getValue() == SqlJsonTableColumnEmptyOrErrorBehavior.DEFAULT) {
                columnEmptyBehavior = tmpLiteral;
                defaultEmptyValue = leftColumnInfos.get(i - 1);
              } else {
                columnEmptyBehavior = leftColumnInfos.get(i - 1);
              }
            }
          } else if (columnInfoVal.getValue() == SqlJsonEmptyOrError.ERROR) {
            if (defaultSymbolIdx >= 0 &&
                leftColumnInfos.get(defaultSymbolIdx) instanceof SqlLiteral) {
              SqlLiteral tmpLiteral = (SqlLiteral) leftColumnInfos.get(defaultSymbolIdx);
              if (tmpLiteral.getValue() == SqlJsonTableColumnEmptyOrErrorBehavior.DEFAULT) {
                columnErrorBehavior = tmpLiteral;
                defaultErrorValue = leftColumnInfos.get(i - 1);
              } else {
                columnErrorBehavior = leftColumnInfos.get(i - 1);
              }
            }
          }
        }
      }
      newColumnInfos.add(pathSpec);
      newColumnInfos.add(columnEmptyBehavior);
      if (defaultEmptyValue != null) {
        newColumnInfos.add(defaultEmptyValue);
      }
      newColumnInfos.add(columnErrorBehavior);
      if (defaultErrorValue != null) {
        newColumnInfos.add(defaultErrorValue);
      }
      this.columnInfos = newColumnInfos;

    }

    @Override
    public List<SqlNode> getOperandList() {
      return this.columnInfos;
    }

    @Override
    public Map<String, RelDataType> resolveFields(
        RelDataTypeFactory typeFactory,
        SqlValidator validator) {
      SqlDataTypeSpec dataTypeSpec = (SqlDataTypeSpec) columnInfos.get(1);
      RelDataType dataType = dataTypeSpec.deriveType(validator);
      this.resolvedFields = Collections.singletonMap(name.getSimple(), dataType);
      return resolvedFields;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      name.unparse(writer, 0, 0);
      int idx = 1;
      columnInfos.get(idx++).unparse(writer, 0, 0);
      writer.keyword("PATH");
      columnInfos.get(idx++).unparse(writer, 0, 0);
      if (((SqlLiteral) columnInfos.get(idx)).getValue() ==
          SqlJsonTableColumnEmptyOrErrorBehavior.DEFAULT) {
        columnInfos.get(idx++).unparse(writer, 0, 0);
      }
      columnInfos.get(idx++).unparse(writer, 0, 0);
      writer.keyword("ON EMPTY");
      if (((SqlLiteral) columnInfos.get(idx)).getValue() ==
          SqlJsonTableColumnEmptyOrErrorBehavior.DEFAULT) {
        columnInfos.get(idx++).unparse(writer, 0, 0);
      }
      columnInfos.get(idx).unparse(writer, 0, 0);
      writer.keyword("ON ERROR");
    }
  }

  public static class SqlJsonTableFormattedColumn extends SqlJsonTableColumn {
    private final List<SqlNode> columnInfos;

    public SqlJsonTableFormattedColumn(SqlParserPos pos, List<SqlNode> columnInfos) {
      super(pos, (SqlIdentifier) columnInfos.get(0));
      assert columnInfos.size() >= 2;

      final List<SqlNode> newColumnInfos = new ArrayList<>();
      newColumnInfos.add(columnInfos.get(0));
      columnInfos.add(columnInfos.get(1));
      SqlNode pathSpec = SqlLiteral.createCharString(
          SqlJsonTableColumn.buildJsonPathSpec(
              ((SqlIdentifier) columnInfos.get(0)).getSimple()),
          pos);
      ;
      ;
      SqlNode wrapperBehavior =
          SqlJsonTableFormattedColumnWrapperBehavior.WITHOUT_ARRAY.symbol(pos);
      SqlNode emptyBehavior = SqlJsonTableFormattedColumnEmptyOrErrorBehavior.NULL.symbol(pos);
      SqlNode errorBehavior = SqlJsonTableFormattedColumnEmptyOrErrorBehavior.NULL.symbol(pos);

      List<SqlNode> leftColumnInfos = Util.skip(columnInfos, 2);
      if (leftColumnInfos.size() > 0) {
        SqlNode columnInfo = leftColumnInfos.get(0);
        if (columnInfo instanceof SqlIdentifier) {
          pathSpec = columnInfo;
        }

        for (int i = 0; i < leftColumnInfos.size(); i++) {
          if (leftColumnInfos.get(i) instanceof SqlLiteral) {
            SqlLiteral columnInfoLiteral = (SqlLiteral) leftColumnInfos.get(i);
            if (columnInfoLiteral.getValue() instanceof
                SqlJsonTableFormattedColumnWrapperBehavior) {
              wrapperBehavior = columnInfoLiteral;
            } else if (columnInfoLiteral.getValue() instanceof SqlJsonEmptyOrError) {
              if (columnInfoLiteral.getValue() == SqlJsonEmptyOrError.EMPTY) {
                emptyBehavior = leftColumnInfos.get(i - 1);
              } else if (columnInfoLiteral.getValue() == SqlJsonEmptyOrError.ERROR) {
                emptyBehavior = leftColumnInfos.get(i - 1);
              }
            }
          }
        }
      }
      newColumnInfos.add(pathSpec);
      newColumnInfos.add(wrapperBehavior);
      newColumnInfos.add(emptyBehavior);
      newColumnInfos.add(errorBehavior);
      this.columnInfos = newColumnInfos;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return columnInfos;
    }

    @Override
    public Map<String, RelDataType> resolveFields(
        RelDataTypeFactory typeFactory,
        SqlValidator validator) {
      SqlDataTypeSpec dataTypeSpec = (SqlDataTypeSpec) columnInfos.get(1);
      RelDataType dataType = dataTypeSpec.deriveType(validator);
      resolvedFields = Collections.singletonMap(name.getSimple(), dataType);
      return resolvedFields;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      name.unparse(writer, 0, 0);
      columnInfos.get(1).unparse(writer, 0, 0);
      writer.keyword("FORMAT JSON");
      SqlJsonTableFormattedColumnWrapperBehavior wrapperBehavior =
          (SqlJsonTableFormattedColumnWrapperBehavior) ((SqlLiteral) columnInfos.get(2)).getValue();
      unparseWrapperBehavior(writer, wrapperBehavior);
      writer.keyword("WRAPPER");
      SqlJsonTableFormattedColumnEmptyOrErrorBehavior emptyBehavior =
          (SqlJsonTableFormattedColumnEmptyOrErrorBehavior)
              ((SqlLiteral) columnInfos.get(3)).getValue();
      unparseEmptyOrErrorBehavior(writer, emptyBehavior);
      writer.keyword("ON EMPTY");
      SqlJsonTableFormattedColumnEmptyOrErrorBehavior errorBehavior =
          (SqlJsonTableFormattedColumnEmptyOrErrorBehavior)
              ((SqlLiteral) columnInfos.get(4)).getValue();
      unparseEmptyOrErrorBehavior(writer, errorBehavior);
      writer.keyword("ON ERROR");
    }

    private void unparseWrapperBehavior(SqlWriter writer,
        SqlJsonTableFormattedColumnWrapperBehavior wrapperBehavior) {
      switch (wrapperBehavior) {
      case WITHOUT_ARRAY:
        writer.keyword("WITHOUT ARRAY");
        break;
      case WITH_CONDITIONAL_ARRAY:
        writer.keyword("WITH CONDITIONAL ARRAY");
        break;
      case WITH_UNCONDITIONAL_ARRAY:
        writer.keyword("WITH UNCONDITIONAL ARRAY");
        break;
      default:
        throw new IllegalStateException("unreachable code");
      }
    }

    private void unparseEmptyOrErrorBehavior(SqlWriter writer,
        SqlJsonTableFormattedColumnEmptyOrErrorBehavior emptyOrErrorBehavior) {
      switch (emptyOrErrorBehavior) {
      case NULL:
        writer.keyword("NULL");
        break;
      case ERROR:
        writer.keyword("ERROR");
        break;
      case EMPTY_ARRAY:
        writer.keyword("EMPTY ARRAY");
        break;
      case EMPTY_OBJECT:
        writer.keyword("EMPTY OBJECT");
        break;
      default:
        throw new IllegalStateException("unreachable code");
      }
    }
  }

  public static class SqlJsonTableNestedColumn extends SqlJsonTableColumn {
    private final List<SqlNode> columnInfos;

    public SqlJsonTableNestedColumn(SqlParserPos pos, List<SqlNode> columnInfo) {
      super(pos, new SqlIdentifier("NESTED", pos));
      this.columnInfos = columnInfo;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return columnInfos;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.keyword("NESTED PATH");
      int currentIdx = 0;
      columnInfos.get(0).unparse(writer, 0, 0);
      currentIdx++;
      if (currentIdx < columnInfos.size()) {
        if (columnInfos.get(currentIdx) instanceof SqlIdentifier) {
          writer.keyword("AS");
          columnInfos.get(currentIdx).unparse(writer, 0, 0);
          currentIdx++;
        }
        columnInfos.get(currentIdx).unparse(writer, 0, 0);
      }
    }

    @Override
    public Map<String, RelDataType> resolveFields(
        RelDataTypeFactory typeFactory,
        SqlValidator validator) {
      SqlJsonTableColumnClause columnClause;
      if (columnInfos.get(1) instanceof SqlJsonTableColumnClause) {
        columnClause = (SqlJsonTableColumnClause) columnInfos.get(1);
      } else {
        columnClause = (SqlJsonTableColumnClause) columnInfos.get(2);
      }
      resolvedFields = columnClause.resolveFields(typeFactory, validator);
      return resolvedFields;
    }
  }

  private static String buildJsonPathSpec(String member) {
    return "strict $." + member;
  }

}
