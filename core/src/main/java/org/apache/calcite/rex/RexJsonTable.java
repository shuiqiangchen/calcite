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

import java.util.List;

public class RexJsonTable extends RexCall{

  protected RexJsonTable(
      RelDataType type,
      SqlOperator operator,
      List<? extends RexNode> operands) {
    super(type, operator, operands);
  }

  public static class JsonTableColumn {
    private final String columnName;

    public JsonTableColumn(String columnName) {
      this.columnName = columnName;
    }
  }

  public static class JsonTableOrdinalityColumn extends JsonTableColumn {
    public JsonTableOrdinalityColumn(String columnName) {
      super(columnName);
    }
  }

  public static class JsonTableRegularColumn extends JsonTableColumn {

    public JsonTableRegularColumn(
        String columnName,
        String pathSpec,
        SqlJsonTableColumnEmptyOrErrorBehavior emptyBehavior,
        SqlJsonTableColumnEmptyOrErrorBehavior errorBehavior) {
      super(columnName);
    }
  }
}
