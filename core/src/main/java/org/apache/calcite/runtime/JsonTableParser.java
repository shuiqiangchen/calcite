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

package org.apache.calcite.runtime;

import org.apache.calcite.rex.RexJsonTable;
import org.apache.calcite.sql.SqlJsonTablePlanSemantics;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.runtime.JsonFunctions.jsonApiCommonSyntax;
import static org.apache.calcite.runtime.JsonFunctions.jsonValue;

public class JsonTableParser {

  private final RexJsonTable.JsonTableInfo jsonTableInfo;

  public JsonTableParser(RexJsonTable.JsonTableInfo jsonTableInfo) {
    this.jsonTableInfo = jsonTableInfo;
  }

  public List<Object[]> parse() {
    // get the parsed table
    Map<Object, List<Object>> parsedTable = new LinkedHashMap<>();
    List<Object[]> returned = new ArrayList<>();

    int tableSize = parsedTable.values().iterator().next().size();
    int columnSize = parsedTable.keySet().size();
    IntStream.of(tableSize).forEach( i -> {
      int currentCol = 0;
      Object[] row = new Object[columnSize];
      for (Object key : parsedTable.keySet()) {
        row[currentCol++] = parsedTable.get(key).get(i);
      }
      returned.add(row);
        });
    return returned;
  }

  private Map<Object, List<Object>> parse(
      JsonFunctions.JsonValueContext valueContext, // Current value context with value
      RexJsonTable.JsonTableColumn column) {


    return null;
  }

  private Object parseRegularColumn(
      JsonFunctions.JsonValueContext valueContext,
      RexJsonTable.JsonTableRegularColumn regularColumn) {
    return jsonValue(
        valueContext,
        regularColumn.getPathSpec(),
        SqlJsonValueEmptyOrErrorBehavior.valueOf(regularColumn.getEmptyBehavior().name()),
        regularColumn.getDefaultEmptyValue(),
        SqlJsonValueEmptyOrErrorBehavior.valueOf(regularColumn.getErrorBehavior().name()),
        regularColumn.getDefaultErrorValue()
    );
  }

  private Map<Object, List<Object>> parseNestedColumn(
      JsonFunctions.JsonValueContext valueContext,
      RexJsonTable.JsonTableNestedColumn nestedColumn
  ) {
    JsonFunctions.JsonPathContext pathContext =
        jsonApiCommonSyntax(valueContext, nestedColumn.getPathSpec());

    // fill up the ordinal column after all physical columns are resolved
    Map<Object, Integer> ordinalColumnIndex = new LinkedHashMap<>();

    Map[] parsedColumns = new Map[nestedColumn.getColumns().size()];

    // parse columns sequentially
    int index = 0;
    for (RexJsonTable.JsonTableColumn column : nestedColumn.getColumns()) {
      if (column instanceof RexJsonTable.JsonTableOrdinalityColumn) {
        ordinalColumnIndex.put(column, index);
      } else if (column instanceof RexJsonTable.JsonTableRegularColumn) {
        Object result = parseRegularColumn(
            JsonFunctions.JsonValueContext.withJavaObj(pathContext.obj),
            (RexJsonTable.JsonTableRegularColumn) column
        );
        parsedColumns[index] = Collections.singletonMap(column, Collections.singletonList(result));
      } else if (column instanceof RexJsonTable.JsonTableNestedColumn) {
        Map<Object, List<Object>> nestedTable =
            parseNestedColumn(JsonFunctions.JsonValueContext.withJavaObj(pathContext.obj),
                (RexJsonTable.JsonTableNestedColumn) column);
        parsedColumns[index] = nestedTable;
      }
      index++;
    }

    // expand nested columns with plan



  return null;


  }

  private List<Object> parseOrdinalColumn(
      int length) {
    return IntStream.range(1, length + 1).boxed().collect(Collectors.toList());
  }

  private Map<Object, List<Object>> parseSiblingUnionCross(
      SqlJsonTablePlanSemantics semantic,
      Map<Object, List<Object>> left,
      Map<Object, List<Object>> right) {
    int leftSize = left.values().iterator().next().size();
    int rightSize = right.values().iterator().next().size();
    Map<Object, List<Object>> parsedResult = new LinkedHashMap<>();
    if (semantic.equals(SqlJsonTablePlanSemantics.CROSS)) {
      // fill left table
      for (Map.Entry<Object, List<Object>> column : left.entrySet()) {
        Object key = column.getKey();
        List<Object> values = column.getValue();
        List<Object> newValues = new ArrayList<>();
        for (Object value : values) {
          IntStream.of(rightSize).forEach(i -> newValues.add(value));
        }
        parsedResult.put(key, newValues);
      }
      // fill right table
      for (Map.Entry<Object, List<Object>> column : right.entrySet()) {
        Object key = column.getKey();
        List<Object> values = column.getValue();
        List<Object> newValues = new ArrayList<>();
        IntStream.of(leftSize).forEach(i -> values.addAll(values));
        parsedResult.put(key, newValues);
      }
    } else {
      // fill left table with null
      for (Map.Entry<Object, List<Object>> column : left.entrySet()) {
        Object key = column.getKey();
        List<Object> values = column.getValue();
        List<Object> newValues = new ArrayList<>(values);
        IntStream.of(rightSize).forEach(i -> newValues.add(null));
        parsedResult.put(key, newValues);
      }
      // append null to left table
      for (Map.Entry<Object, List<Object>> column : right.entrySet()) {
        Object key = column.getKey();
        List<Object> values = column.getValue();
        List<Object> newValues = new ArrayList<>(values);
        IntStream.of(leftSize).forEach(i -> newValues.add(null));
        newValues.add(values);
        parsedResult.put(key, newValues);
      }
    }
    return parsedResult;
  }

  private Map<Object, List<Object>> parseParentChildInnerOuter(
      SqlJsonTablePlanSemantics semantic,
      Map<Object, List<Object>> left,
      Map<Object, List<Object>> right
  ) {
    // assert that left size must be larger than 1
    int rightSize = right.values().iterator().next().size();
    Map<Object, List<Object>> parsedResult = new LinkedHashMap<>();
    if (semantic.equals(SqlJsonTablePlanSemantics.INNER)) {
      if (rightSize == 0) {
        for (Map.Entry<Object, List<Object>> column : left.entrySet()) {
          Object key = column.getKey();
          parsedResult.put(key, Collections.emptyList());
        }
        for (Map.Entry<Object, List<Object>> column : right.entrySet()) {
          Object key = column.getKey();
          parsedResult.put(key, Collections.emptyList());
        }
      } else {
        for (Map.Entry<Object, List<Object>> column : left.entrySet()) {
          Object key = column.getKey();
          List<Object> values = column.getValue();
          List<Object> newValues = new ArrayList<>();
          for (Object value : values) {
            IntStream.of(rightSize).forEach(i -> newValues.add(value));
          }
          parsedResult.put(key, newValues);
          parsedResult.putAll(right);
        }
      }
    } else { // outter join
      if (rightSize == 0) {
        parsedResult.putAll(left);
        parsedResult.putAll(right);
      } else {
        for (Map.Entry<Object, List<Object>> column : left.entrySet()) {
          Object key = column.getKey();
          List<Object> values = column.getValue();
          List<Object> newValues = new ArrayList<>();
          for (Object value : values) {
            IntStream.of(rightSize).forEach(i -> newValues.add(value));
          }
          parsedResult.put(key, newValues);
          parsedResult.putAll(right);
        }
      }
    }
    return parsedResult;
  }

}
