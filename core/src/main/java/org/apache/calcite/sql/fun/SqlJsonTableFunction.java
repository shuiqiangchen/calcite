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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;

import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * SqlJsonTableFunction.
 */
public class SqlJsonTableFunction extends SqlFunction implements SqlTableFunction {
  public SqlJsonTableFunction() {
    super(
        "JSON_TABLE",
        SqlKind.JSON_TABLE,
        null,
        null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER,
            SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.SYSTEM);
  }



  @Override
  public SqlReturnTypeInference getRowTypeInference() {
    return SqlJsonTableFunction::getTableSchema;
  }

  @Override
  public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos,
      @Nullable SqlNode... operands) {
    operands[1] = SqlLiteral.createCharString(((SqlIdentifier)operands[1]).getSimple(), operands[1].getParserPosition());
    if (operands[3] == null) {
      SqlNode[] plans = new SqlNode[2];
      plans[0] = SqlLiteral.createSymbol(SqlJsonTablePlanSemantics.INNER, pos);
      plans[1] = SqlLiteral.createSymbol(SqlJsonTablePlanSemantics.CROSS, pos);
      operands[3] = new SqlJsonTablePlanBase.SqlJsonTableDefaultPlan(Arrays.asList(plans), pos);
    }

    if (operands[4] == null){
      operands[4] = SqlLiteral.createSymbol(SqlJsonTableErrorBehavior.EMPTY, pos);
    }
    return super.createCall(functionQualifier, pos, operands);
  }

  private static RelDataType getTableSchema(SqlOperatorBinding opBinding) {
    SqlCallBinding callBinding = (SqlCallBinding) opBinding;
    Map<String, RelDataType> columnInfos =
        ((SqlJsonTableColumn) callBinding.operand(2))
            .resolveFields(opBinding.getTypeFactory(), ((SqlCallBinding) opBinding).getValidator());
    RelDataTypeFactory.Builder builder = callBinding.getTypeFactory().builder();
    columnInfos.forEach(builder::add);
    return builder.build();
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    // Unparse api common syntax
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(",", true);
    call.operand(1).unparse(writer, 0, 0);

    // Unparse columns
    call.operand(2).unparse(writer, 0, 0);

    // Unparse plan
    call.operand(3).unparse(writer, 0, 0);
//    SqlNodeList plans = call.operand(3);
//    if (plans.size() == 2) {
//      writer.keyword("PLAN DEFAULT");
//    } else {
//      writer.keyword("PLAN");
//    }
//    unparsePlanClause(writer, plans);

    // Unparse error behavior
    call.operand(4).unparse(writer, 0, 0);
    writer.keyword("ON ERROR");


    writer.endFunCall(frame);
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    return getTableSchema(new SqlCallBinding(validator, scope, call));
  }

  private void unparsePlanClause(SqlWriter writer, SqlNodeList plans) {

    final SqlWriter.Frame planFrame = writer.startList("(", ")");
    if (plans.size() == 1) {
      plans.get(0).unparse(writer, 0, 0);
    }
    if (plans.size() == 2) {
      plans.get(0).unparse(writer, 0, 0);
      writer.sep(",", true);
      plans.get(1).unparse(writer, 0, 0);
    } else if (plans.size() >= 3){
      plans.get(1).unparse(writer, 0, 0);
      plans.get(0).unparse(writer, 0, 0);
      for (int i = 2; i < plans.size(); i++) {
        if (plans.get(2) instanceof SqlNodeList) {
          unparsePlanClause(writer, (SqlNodeList) plans.get(2));
        } else {
          plans.get(2).unparse(writer, 0, 0);
        }
      }
    }
    writer.endList(planFrame);
  }
}
