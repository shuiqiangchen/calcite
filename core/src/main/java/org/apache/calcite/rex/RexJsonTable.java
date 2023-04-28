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

package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlJsonTableColumnEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlOperator;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class RexJsonTable extends RexCall{

  private final JsonTableInfo tableInfo;

  public RexJsonTable(
      RelDataType type,
      SqlOperator operator,
      List<? extends RexNode> operands,
      String pathSpec,
      List<JsonTableColumn> columns,
      Object planClause,
      Object errorBehavior) {
    super(type, operator, operands);
    this.tableInfo = new JsonTableInfo(pathSpec, columns, planClause, errorBehavior);
  }

  public JsonTableInfo getTableInfo() {
    return tableInfo;
  }

  public static class JsonTableInfo implements Serializable {
    private final String pathSpec;
    private final List<JsonTableColumn> columns;
    private final Object planClause;
    private final Object errorBehavior;

    public JsonTableInfo(
        String pathSpec,
        List<JsonTableColumn> columns,
        Object planClause,
        Object errorBehavior) {
      this.pathSpec = pathSpec;
      this.columns = columns;
      this.planClause = planClause;
      this.errorBehavior = errorBehavior;
    }

    public String getPathSpec() {
      return pathSpec;
    }

    public List<JsonTableColumn> getColumns() {
      return columns;
    }

    public Object getPlanClause() {
      return planClause;
    }

    public Object getErrorBehavior() {
      return errorBehavior;
    }
  }

  public static class JsonTableColumn implements Serializable{
    private final String columnName;

    public JsonTableColumn(String columnName) {
      this.columnName = columnName;
    }

    public String getColumnName() {
      return columnName;
    }
  }

  public static class JsonTableOrdinalityColumn extends JsonTableColumn {
    public JsonTableOrdinalityColumn(String columnName) {
      super(columnName);
    }
  }

  public static class JsonTableRegularColumn extends JsonTableColumn {

    private final String pathSpec;
    private final SqlJsonTableColumnEmptyOrErrorBehavior emptyBehavior;
    private final Object defaultEmptyValue;
    private final SqlJsonTableColumnEmptyOrErrorBehavior errorBehavior;
    private final Object defaultErrorValue;

    public JsonTableRegularColumn(
        String columnName,
        String pathSpec,
        SqlJsonTableColumnEmptyOrErrorBehavior emptyBehavior,
        Object defaultEmptyValue,
        SqlJsonTableColumnEmptyOrErrorBehavior errorBehavior,
        Object defaultErrorValue) {
      super(columnName);
      this.pathSpec = pathSpec;
      this.emptyBehavior = emptyBehavior;
      this.defaultEmptyValue = defaultEmptyValue;
      this.errorBehavior = errorBehavior;
      this.defaultErrorValue = defaultErrorValue;
    }

    public String getPathSpec() {
      return pathSpec;
    }
    public SqlJsonTableColumnEmptyOrErrorBehavior getEmptyBehavior() {
      return emptyBehavior;
    }

    public Object getDefaultEmptyValue() {
      return defaultEmptyValue;
    }

    public SqlJsonTableColumnEmptyOrErrorBehavior getErrorBehavior() {
      return errorBehavior;
    }

    public Object getDefaultErrorValue() {
      return defaultErrorValue;
    }
  }

  public static class JsonTableNestedColumn extends JsonTableColumn {

    private final String pathName;
    private final String pathSpec;
    private final List<JsonTableColumn> columns;

    private int expandedColumnNum;

    public JsonTableNestedColumn(
        String columnName,
        String pathSpec,
        String pathName,
        List<JsonTableColumn> columns
        ) {
      super(columnName);
      this.pathSpec = pathSpec;
      this.pathName = pathName;
      this.columns = columns;
      this.expandedColumnNum = columns.stream()
          .mapToInt(column -> column instanceof JsonTableNestedColumn ?
              ((JsonTableNestedColumn) column).getExpandedColumnNum() : 1)
          .sum();
    }

    public String getPathSpec() {
      return pathSpec;
    }

    public String getPathName() {
      return pathName;
    }

    public List<JsonTableColumn> getColumns() {
      return columns;
    }

    public int getExpandedColumnNum() {
      return expandedColumnNum;
    }
  }

  public static class JsonTablePlan implements Serializable{
    private final Object defaultParentChild;
    private final Object defaultSibling;
    private final Map<String, Object> specificPlans;

    public JsonTablePlan(Object defaultParentChild, Object defaultSibling,
        Map<String, Object> specificPlans) {
      this.defaultParentChild = defaultParentChild;
      this.defaultSibling = defaultSibling;
      this.specificPlans = specificPlans;
    }
  }

}
