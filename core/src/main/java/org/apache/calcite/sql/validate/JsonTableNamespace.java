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

package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import org.apache.calcite.sql.fun.SqlJsonTableFunction;

import org.checkerframework.checker.nullness.qual.Nullable;

public class JsonTableNamespace extends AbstractNamespace{

  private final SqlCall jsonTable;

  private final SqlValidatorScope scope;

  /**
   * Creates an AbstractNamespace.
   *
   * @param validator     Validator
   * @param enclosingNode Enclosing node
   */
  JsonTableNamespace(
      SqlValidatorImpl validator,
      SqlCall jsonTable,
      SqlValidatorScope scope,
      @Nullable SqlNode enclosingNode) {
    super(validator, enclosingNode);
    assert jsonTable.getOperator() instanceof SqlJsonTableFunction;
    this.jsonTable = jsonTable;
    this.scope = scope;
  }

  @Override
  protected RelDataType validateImpl(RelDataType targetRowType) {
    validator.validateIdentifier(jsonTable.operand(0), scope);
    RelDataType type =
        jsonTable.getOperator().deriveType(validator, scope, jsonTable);
    return type;
  }

  @Override
  public @Nullable SqlNode getNode() {
    return jsonTable;
  }
}
