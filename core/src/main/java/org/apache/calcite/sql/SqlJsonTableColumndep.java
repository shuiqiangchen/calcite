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

//
//
//package org.apache.calcite.sql;
//
//import com.google.common.collect.ImmutableList;
//
//import org.apache.calcite.sql.parser.SqlParserPos;
//import org.apache.calcite.sql.util.SqlVisitor;
//import org.apache.calcite.sql.validate.SqlValidator;
//import org.apache.calcite.sql.validate.SqlValidatorScope;
//import org.apache.calcite.util.Litmus;
//
//import org.checkerframework.checker.nullness.qual.Nullable;
//
//import java.util.List;
//
//public class SqlJsonTableColumn {
//
//  public static class SqlJsonTableOrdinalityColumn extends SqlNode {
//
//    private final SqlIdentifier name;
//
//    protected SqlJsonTableOrdinalityColumn(SqlParserPos pos, SqlIdentifier name) {
//      super(pos);
//      this.name = name;
//    }
//
//    @Override
//    public SqlNode clone(SqlParserPos pos) {
//      return new SqlJsonTableOrdinalityColumn(pos, name);
//    }
//
//    @Override
//    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
//      //todo
//    }
//
//    @Override
//    public void validate(SqlValidator validator, SqlValidatorScope scope) {
//
//    }
//
//    @Override
//    public <R> R accept(SqlVisitor<R> visitor) {
//      return visitor.visit(name);
//    }
//
//    @Override
//    public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
//      return false;
//    }
//  }
//
//  public static class SqlJsonTableRegularColumn extends SqlNode {
//
//    private final SqlNodeList columnInfo;
//
//    protected SqlJsonTableRegularColumn(
//        SqlParserPos pos,
//        SqlNodeList columnInfo
////        SqlIdentifier name,
////        SqlDataTypeSpec dataType,
////        SqlNode jsonPath,
////        SqlJsonTableColumnEmptyOrErrorBehavior emptyBehavior,
////        Object defaultValueOnEmpty,
////        SqlJsonTableColumnEmptyOrErrorBehavior errorBehavior,
////        Object defaultValueOnError
//        ) {
//      super(pos);
//      this.columnInfo = columnInfo;
//    }
//
//    @Override
//    public SqlNode clone(SqlParserPos pos) {
//      return null;
//    }
//
//    @Override
//    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
//
//    }
//
//    @Override
//    public void validate(SqlValidator validator, SqlValidatorScope scope) {
//
//    }
//
//    @Override
//    public <R> R accept(SqlVisitor<R> visitor) {
//      return null;
//    }
//
//    @Override
//    public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
//      return false;
//    }
//  }
//
//  public static class SqlJsonTableFormattedColumn extends SqlJsonTableColumn {
//
//    protected SqlJsonTableFormattedColumn(
//        SqlParserPos pos,
//        SqlIdentifier name,
//        SqlDataTypeSpec dataType,
//        SqlJsonEncoding jsonEncoding,
//        SqlNode jsonPath,
//        SqlJsonTableFormattedColumnWrapperBehavior wrapperBehavior,
//        SqlJsonTableFormattedColumnEmptyOrErrorBehavior errorOrEmptyBehavior
//
//        ) {
//      super(pos, name);
//    }
//
//    @Override
//    public List<SqlNode> getOperandList() {
//      return null;
//    }
//
//    @Override
//    protected void unparseColumn(SqlWriter writer, int leftPrec, int rightPrec) {
//
//    }
//  }
//
//  public static class SqlJsonTableNestedColumn extends SqlJsonTableColumn {
//
//    protected SqlJsonTableNestedColumn(
//        SqlParserPos pos,
//        SqlNode jsonPath,
//        SqlIdentifier nestedPathName,
//        List<SqlJsonTableColumn> columns
//        ) {
//      super(pos);
//    }
//
//    @Override
//    public List<SqlNode> getOperandList() {
//      return null;
//    }
//
//    @Override
//    protected void unparseColumn(SqlWriter writer, int leftPrec, int rightPrec) {
//
//    }
//  }
//
//}
