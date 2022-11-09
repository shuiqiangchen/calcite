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

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public abstract class SqlJsonTablePlanBase extends SqlCall{

  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("JSON_TABLE_PLAN", SqlKind.JSON_TABLE_PLAN);

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  protected SqlJsonTablePlanBase(SqlParserPos pos) {
    super(pos);
  }

  public static class SqlJsonTableDefaultPlan extends SqlJsonTablePlanBase {

    private final List<SqlNode> planChoices;

    public SqlJsonTableDefaultPlan(List<SqlNode> planChoices, SqlParserPos pos) {
      super(pos);
      this.planChoices = planChoices;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return planChoices;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.keyword("PLAN DEFAULT");
      assert planChoices.size() == 2;
      final SqlWriter.Frame planFrame = writer.startList("(", ")");
      planChoices.get(0).unparse(writer, 0, 0);
      writer.sep(",", true);
      planChoices.get(1).unparse(writer, 0, 0);
      writer.endList(planFrame);
    }
  }

  public static class SqlJsonTableSpecificPlan extends SqlJsonTablePlanBase {

    private final SqlNode jsonTablePlan;

    public SqlJsonTableSpecificPlan(SqlNode jsonTablePlan, SqlParserPos pos) {
      super(pos);
      this.jsonTablePlan = jsonTablePlan;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return Collections.singletonList(jsonTablePlan);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      writer.keyword("PLAN");
      writer.keyword("(");
      jsonTablePlan.unparse(writer, 0, 0);
      writer.keyword(")");
    }
  }

  public static class SqlJsonTablePlan extends SqlJsonTablePlanBase {
    private final SqlNode jsonTablePlan;
    public SqlJsonTablePlan(SqlNode jsonTablePlan, SqlParserPos pos) {
      super(pos);
      this.jsonTablePlan = jsonTablePlan;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return Collections.singletonList(jsonTablePlan);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      jsonTablePlan.unparse(writer, 0, 0);
    }
  }

  public static class SqlJsonTablePlanParentChild extends SqlJsonTablePlanBase {

    private final List<SqlNode> plans;

    public SqlJsonTablePlanParentChild(List<SqlNode> plans, SqlParserPos pos) {
      super(pos);
      this.plans = plans;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return plans;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      plans.get(1).unparse(writer, 0, 0);
      plans.get(0).unparse(writer, 0, 0);
      plans.get(2).unparse(writer, 0, 0);
    }
  }

  public static class SqlJsonTablePlanSibling extends SqlJsonTablePlanBase {

    private final List<SqlNode> plans;

    public SqlJsonTablePlanSibling(List<SqlNode> plans, SqlParserPos pos) {
      super(pos);
      this.plans = plans;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return plans;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      plans.get(1).unparse(writer, 0, 0);
      plans.get(0).unparse(writer, 0, 0);
      for (int i = 2; i < plans.size(); i++) {
        if (i > 2) {
          plans.get(0).unparse(writer, 0, 0);
        }
        plans.get(i).unparse(writer, 0, 0);
      }
    }
  }

  public static class SqlJsonTablePlanPrimary extends SqlJsonTablePlanBase {
    private final SqlNode plan;

    public SqlJsonTablePlanPrimary(SqlNode plan, SqlParserPos pos) {
      super(pos);
      this.plan = plan;
    }

    @Override
    public List<SqlNode> getOperandList() {
      return Collections.singletonList(plan);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      if (plan instanceof SqlIdentifier) {
        plan.unparse(writer, 0, 0);
      } else {
        writer.keyword("(");
        plan.unparse(writer, 0, 0);
        writer.keyword(")");
      }
    }
  }
}
