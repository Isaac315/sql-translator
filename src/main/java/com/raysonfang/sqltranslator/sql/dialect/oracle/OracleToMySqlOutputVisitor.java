package com.raysonfang.sqltranslator.sql.dialect.oracle;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.expr.OracleSysdateExpr;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.*;
import com.alibaba.druid.sql.dialect.oracle.visitor.OracleOutputVisitor;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.util.FnvHash;
import com.alibaba.druid.util.JdbcConstants;
import com.raysonfang.sqltranslator.sql.dialect.mysql.util.MySqlUtil;
import com.raysonfang.sqltranslator.sql.dialect.oracle.function.OracleToMySqlFunctionTransform;
import com.raysonfang.sqltranslator.sql.dialect.oracle.util.OracleSQLDataTypeTransformUtil;
import com.raysonfang.sqltranslator.util.MapCacheUtil;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;

/**
 * oracle的sql 转换成 mysql的sql语法
 *
 * @author rayson.fang
 * @date 2021/02/05 12:11
 **/
public class OracleToMySqlOutputVisitor extends OracleOutputVisitor {
    
    private final OracleToMySqlFunctionTransform functionTransform = new OracleToMySqlFunctionTransform();
    // 目标数据库类型
    private final DbType distDbType = DbType.mysql;
    
    public OracleToMySqlOutputVisitor(Appendable appender, boolean printPostSemi){
        super(appender, printPostSemi);
    }
    
    public OracleToMySqlOutputVisitor(Appendable appender){
        super(appender);
    }
    
    @Override
    public boolean visit(OracleSelectQueryBlock x) {
        boolean parentIsSelectStatment = false;
        {
            if (x.getParent() instanceof SQLSelect) {
                SQLSelect select = (SQLSelect) x.getParent();
                if (select.getParent() instanceof SQLSelectStatement || select.getParent() instanceof SQLSubqueryTableSource) {
                    parentIsSelectStatment = true;
                }
            }
        }
        
        if (!parentIsSelectStatment) {
            return super.visit(x);
        }
        
        Optional.ofNullable(x.getSelectList()).orElse(new ArrayList<>()).stream().forEach(c->{
            functionTransform.transformFunctionExpr(c.getExpr());
        });
        
        if (x.getWhere() instanceof SQLBinaryOpExpr //
                && x.getFrom() instanceof SQLSubqueryTableSource //
        ) {
            int rownum;
            SQLVariantRefExpr rownumRef=null;
            String ident;
            SQLBinaryOpExpr where = (SQLBinaryOpExpr) x.getWhere();
            if (where.getRight() instanceof SQLIntegerExpr && where.getLeft() instanceof SQLIdentifierExpr) {
                rownum = ((SQLIntegerExpr) where.getRight()).getNumber().intValue();
                ident = ((SQLIdentifierExpr) where.getLeft()).getName();
            } else if(where.getRight() instanceof SQLVariantRefExpr && where.getLeft() instanceof SQLIdentifierExpr) {
                rownum = 0;
                rownumRef = (SQLVariantRefExpr) where.getRight();
                ident = ((SQLIdentifierExpr) where.getLeft()).getName();
            } else {
                return super.visit(x);
            }
            SQLSelect select = ((SQLSubqueryTableSource) x.getFrom()).getSelect();
            SQLSelectQueryBlock queryBlock = null;
            SQLSelect subSelect = null;
            SQLBinaryOpExpr subWhere = null;
            boolean isSubQueryRowNumMapping = false;
            
            if (select.getQuery() instanceof SQLSelectQueryBlock) {
                queryBlock = (SQLSelectQueryBlock) select.getQuery();
                if (queryBlock.getWhere() instanceof SQLBinaryOpExpr) {
                    subWhere = (SQLBinaryOpExpr) queryBlock.getWhere();
                }
                
                for (SQLSelectItem selectItem : queryBlock.getSelectList()) {
                    if (isRowNumber(selectItem.getExpr())) {
                        if (where.getLeft() instanceof SQLIdentifierExpr
                                && ((SQLIdentifierExpr) where.getLeft()).getName().equals(selectItem.getAlias())) {
                            isSubQueryRowNumMapping = true;
                        }
                    }
                }
                
                SQLTableSource subTableSource = queryBlock.getFrom();
                if (subTableSource instanceof SQLSubqueryTableSource) {
                    subSelect = ((SQLSubqueryTableSource) subTableSource).getSelect();
                }
            }
            
            if ("ROWNUM".equalsIgnoreCase(ident)) {
                SQLBinaryOperator op = where.getOperator();
                Integer limit = null;
                if (op == SQLBinaryOperator.LessThanOrEqual) {
                    //limit1 = rownumRef.toString();
                    limit = rownum;
                } else if (op == SQLBinaryOperator.LessThan) {
                    //limit1 = rownumRef.toString() + "-1";
                    limit = rownum - 1;
                }
                
                if (limit != null) {
                    select.accept(this);
                    println();
                    print0(ucase ? "LIMIT " : "limit ");
                    print(rownumRef != null ? rownumRef.getName() : String.valueOf(limit));
                    return false;
                }
            } else if (isSubQueryRowNumMapping) {
                SQLBinaryOperator op = where.getOperator();
                SQLBinaryOperator subOp = subWhere.getOperator();
                
                if (isRowNumber(subWhere.getLeft()) //
                        && subWhere.getRight() instanceof SQLIntegerExpr) {
                    
                    int subRownum = ((SQLIntegerExpr) subWhere.getRight()).getNumber().intValue();
                    
                    Integer offset = null;
                    if (op == SQLBinaryOperator.GreaterThanOrEqual) {
                        offset = rownum + 1;
                    } else if (op == SQLBinaryOperator.GreaterThan) {
                        offset = rownum;
                    }
                    
                    if (offset != null) {
                        Integer limit = null;
                        if (subOp == SQLBinaryOperator.LessThanOrEqual) {
                            limit = subRownum - offset;
                        } else if (subOp == SQLBinaryOperator.LessThan) {
                            limit = subRownum - 1 - offset;
                        }
                        
                        if (limit != null) {
                            subSelect.accept(this);
                            println();
                            print0(ucase ? "LIMIT " : "limit ");
                            print("?");
                            print0(", ");
                            print("?");
                            return false;
                        }
                    }
                }else if(isRowNumber(subWhere.getLeft()) //
                        && subWhere.getRight() instanceof SQLVariantRefExpr) {
                    subSelect.accept(this);
                    println();
                    print0(ucase ? "LIMIT " : "limit ");
                    print(rownumRef.getName());
                    print0(", ");
                    print(((SQLVariantRefExpr) subWhere.getRight()).getName());
                    return false;
                }
            }
        }
        
        return super.visit(x);
    }
    
    static boolean isRowNumber(SQLExpr expr) {
        if (expr instanceof SQLIdentifierExpr) {
            return ((SQLIdentifierExpr) expr)
                    .hashCode64() == FnvHash.Constants.ROWNUM;
        }
        
        return false;
    }
    
    @Override
    public boolean visit(SQLDropTableStatement x) {
        x.setDbType(DbType.mysql);
        x.setIfExists(true);
        return super.visit(x);
    }
    
    @Override
    public boolean visit(SQLBlockStatement x) {
        for(SQLStatement statement : x.getStatementList()) {
            if (statement instanceof SQLIfStatement) {
                for (SQLStatement s : ((SQLIfStatement) (statement)).getStatements()) {
                    if (s instanceof OracleExecuteImmediateStatement) {
                        String sql = ((OracleExecuteImmediateStatement) s)
                                .getDynamicSql().toString().toLowerCase();
                        if (sql.contains("drop table")) {
                            println();
                            print0(sql.replace("drop table", "drop table if exists ").replaceAll("'",""));
                            return false;
                        }
                    }
                }
            }
        }
        return super.visit(x);
    }
    
    @Override
    public boolean visit(SQLAlterTableStatement x) {
        for (SQLAlterTableItem item : x.getItems()) {
            if(item instanceof SQLAlterTableRename) {
                ((SQLAlterTableStatement) item.getParent()).setDbType(distDbType);
                String rename = "";
                if(x.isAfterSemi()) {
                    rename = x.toString().substring(0, x.toString().length() - 1);
                } else {
                    rename = x.toString();
                }
                println();
                print0(rename);
                return false;
            }
        }
        
        return super.visit(x);
    }
    
    @Override
    public boolean visit(OracleInsertStatement x) {
        /*print(";");*/
        x.getTableSource().setExpr(x.getTableSource().getName().toString().replaceAll("\"",""));
        x.getColumns().forEach(c->{
            ((SQLIdentifierExpr)c).setName(((SQLIdentifierExpr)c).getName().replaceAll("\"", ""));
            if(MySqlUtil.containsKeyWords(((SQLIdentifierExpr)c).getName())) {
                ((SQLIdentifierExpr)c).setName("`"+((SQLIdentifierExpr)c).getName()+"`");
            }
        });
        Optional.ofNullable(x.getValuesList()).orElse(new ArrayList<>()).forEach(e -> {
            Optional.ofNullable(e.getValues()).orElse(new ArrayList<>()).forEach(c -> {
                functionTransform.transformFunctionExpr(c);
            });
        });
        println();
        return super.visit(x);
    }
    
    @Override
    public boolean visit(OracleUpdateStatement x) {
        println();
        Optional.ofNullable(x.getItems()).orElse(new ArrayList<>()).forEach(expr -> {
            functionTransform.transformFunctionExpr(expr.getValue());
        });
        SQLExpr where = x.getWhere();
        if (!ObjectUtils.isEmpty(where)) {
            Optional.ofNullable(where.getChildren()).orElse(new ArrayList<>()).forEach(e -> {
                functionTransform.sqlWhereList(e);
            });
        }
        /*print(";");*/
        return super.visit(x);
    }
    
    @Override
    public boolean visit(OracleCreateTableStatement x) {
        MySqlCreateTableStatement mySqlCreateTableStatement = new MySqlCreateTableStatement();
        mySqlCreateTableStatement.setTableSource(x.getTableSource());
        String tableName = x.getTableSource().getName().getSimpleName();
        for(SQLTableElement sqlTableElement : x.getTableElementList()){
            if( sqlTableElement instanceof SQLColumnDefinition) {
                SQLColumnDefinition sqlColumnDefinition = ((SQLColumnDefinition)sqlTableElement);
                String columnName = sqlColumnDefinition.getName().getSimpleName().replaceAll("\"", "");
                if(MySqlUtil.containsKeyWords(columnName)){
                    sqlColumnDefinition.setName("`"+columnName+"`");
                }
                sqlColumnDefinition.setDataType(OracleSQLDataTypeTransformUtil.transformOracleToMySql(SQLParserUtils.createExprParser(sqlColumnDefinition.getDataType().toString(), DbType.oracle).parseDataType()));
                if(sqlColumnDefinition.getDefaultExpr() != null) {
                    SQLExpr expr = sqlColumnDefinition.getDefaultExpr();
                    if(expr instanceof SQLMethodInvokeExpr) {
                        SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) expr;
                        final long nameHashCode64 = sqlMethodInvokeExpr.methodNameHashCode64();
                        if (nameHashCode64 == FnvHash.Constants.SYS_GUID || nameHashCode64 == FnvHash.Constants.TO_CHAR) {
                            sqlColumnDefinition.setDefaultExpr(null);
                        }else {
                            functionTransform.methodInvoke((SQLMethodInvokeExpr) sqlColumnDefinition.getDefaultExpr());
                        }
                        
                    }else if(expr instanceof SQLIdentifierExpr) {
                        SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) expr;
                        final long nameHashCode64 = sqlIdentifierExpr.nameHashCode64();
                        if(nameHashCode64 == FnvHash.Constants.SYSTIMESTAMP) {
                            SQLIdentifierExpr xx = sqlIdentifierExpr.clone();
                            xx.setName("CURRENT_TIMESTAMP");
                            xx.setParent(sqlIdentifierExpr.getParent());
                            sqlColumnDefinition.setDefaultExpr(xx);
                            if(sqlColumnDefinition.getColumnName().contains("UPDATE_TIME")
                                    || sqlColumnDefinition.getColumnName().contains("MDFY_TIME")) {
                                sqlColumnDefinition.setOnUpdate(xx);
                            }
                        }
                    }else if(expr instanceof OracleSysdateExpr) {
                        SQLIdentifierExpr xx = new SQLIdentifierExpr("CURRENT_TIMESTAMP");
                        xx.setParent(expr.getParent());
                        sqlColumnDefinition.setDefaultExpr(xx);
                        if(sqlColumnDefinition.getColumnName().contains("UPDATE_TIME")
                                || sqlColumnDefinition.getColumnName().contains("MDFY_TIME")) {
                            sqlColumnDefinition.setOnUpdate(xx);
                        }
                    }
                }
                sqlColumnDefinition.setDbType(distDbType);
                MapCacheUtil.getInstance().addCacheData(tableName.toUpperCase() + ":" + columnName.toUpperCase(), sqlColumnDefinition.toString().replaceAll(sqlColumnDefinition.getColumnName(), ""));
                mySqlCreateTableStatement.getTableElementList().add(sqlColumnDefinition);
            }else if(sqlTableElement instanceof OraclePrimaryKey){
                MySqlPrimaryKey mySqlPrimaryKey = new MySqlPrimaryKey();
                ((OraclePrimaryKey) sqlTableElement).cloneTo(mySqlPrimaryKey);
                mySqlCreateTableStatement.getTableElementList().add(mySqlPrimaryKey);
            }else if(sqlTableElement instanceof OracleUnique) {
                MySqlUnique mySqlUnique = new MySqlUnique();
                ((OracleUnique) sqlTableElement).cloneTo(mySqlUnique);
                mySqlCreateTableStatement.getTableElementList().add(mySqlUnique);
            }
        }
        if(Objects.nonNull(x.getSelect())){
            x.setParent(mySqlCreateTableStatement);
            mySqlCreateTableStatement.setSelect(x.getSelect());
        }
        println();
        print(mySqlCreateTableStatement.toString());
        /*print(";");*/
        return false;
    }
    
    @Override
    public boolean visit(SQLColumnDefinition x) {
        if(x.getParent() instanceof SQLAlterTableAddColumn || x.getParent() instanceof OracleAlterTableModify) {
            String columnName = x.getName().getSimpleName().replaceAll("\"", "");
            if(MySqlUtil.containsKeyWords(columnName)){
                x.setName("`"+columnName+"`");
            }
            x.setDataType(OracleSQLDataTypeTransformUtil.transformOracleToMySql(SQLParserUtils.createExprParser(x.getDataType().toString(), DbType.oracle).parseDataType()));
            if(x.getDefaultExpr() != null) {
                SQLExpr expr = x.getDefaultExpr();
                if(expr instanceof SQLMethodInvokeExpr) {
                    SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) expr;
                    final long nameHashCode64 = sqlMethodInvokeExpr.methodNameHashCode64();
                    if (nameHashCode64 == FnvHash.Constants.SYS_GUID || nameHashCode64 == FnvHash.Constants.TO_CHAR) {
                        x.setDefaultExpr(null);
                    }else {
                        functionTransform.methodInvoke((SQLMethodInvokeExpr) x.getDefaultExpr());
                    }
                    
                }else if(expr instanceof SQLIdentifierExpr) {
                    SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) expr;
                    final long nameHashCode64 = sqlIdentifierExpr.nameHashCode64();
                    if(nameHashCode64 == FnvHash.Constants.SYSTIMESTAMP) {
                        SQLIdentifierExpr xx = sqlIdentifierExpr.clone();
                        xx.setName("CURRENT_TIMESTAMP");
                        xx.setParent(sqlIdentifierExpr.getParent());
                        x.setDefaultExpr(xx);
                        if(x.getColumnName().contains("UPDATE_TIME")
                                || x.getColumnName().contains("MDFY_TIME")) {
                            x.setOnUpdate(xx);
                        }
                    }
                }else if(expr instanceof OracleSysdateExpr) {
                    SQLIdentifierExpr xx = new SQLIdentifierExpr("CURRENT_TIMESTAMP");
                    xx.setParent(expr.getParent());
                    x.setDefaultExpr(xx);
                    if(x.getColumnName().contains("UPDATE_TIME")
                            || x.getColumnName().contains("MDFY_TIME")) {
                        x.setOnUpdate(xx);
                    }
                }
            }
            x.setDbType(distDbType);
            if((x.getParent().getParent()) instanceof SQLAlterTableStatement ) {
                String tableName = ((SQLAlterTableStatement)(x.getParent().getParent())).getTableName();
                MapCacheUtil.getInstance().addCacheData(tableName.toUpperCase() + ":" + columnName.toUpperCase(), x.toString().replaceAll(x.getColumnName(), ""));
            }
        }
        return super.visit(x);
    }
    
    @Override
    public boolean visit(OracleDeleteStatement x) {
        println();
        SQLExpr where = x.getWhere();
        if (!ObjectUtils.isEmpty(where)) {
            if(where instanceof SQLExistsExpr) {
                SQLSelect sqlSelect = ((SQLExistsExpr) where).getSubQuery();
                SQLSelectQueryBlock oracleSelectQueryBlock = sqlSelect.getQueryBlock();
                SQLTableSource oracleSelectTableReference = oracleSelectQueryBlock.getFrom();
                
                MySqlDeleteStatement mySqlDeleteStatement = new MySqlDeleteStatement();
                SQLExprTableSource sqlExprTableSource = new SQLExprTableSource();
                sqlExprTableSource.setExpr(x.getAlias());
                sqlExprTableSource.setParent(mySqlDeleteStatement);
                
                mySqlDeleteStatement.setTableSource(sqlExprTableSource);
                
                SQLJoinTableSource sqlJoinTableSource1 = new SQLJoinTableSource();
                SQLExprTableSource leftTable = new SQLExprTableSource();
                leftTable.setExpr(x.getTableName());
                leftTable.setParent(sqlJoinTableSource1);
                leftTable.setAlias(x.getAlias());
                sqlJoinTableSource1.setLeft(leftTable);
                sqlJoinTableSource1.setJoinType(SQLJoinTableSource.JoinType.COMMA);
                SQLExprTableSource rightTable = new SQLExprTableSource();
                rightTable.setAlias(oracleSelectTableReference.getAlias());
                rightTable.setExpr(((OracleSelectTableReference)oracleSelectTableReference).getName());
                rightTable.setParent(sqlJoinTableSource1);
                sqlJoinTableSource1.setRight(rightTable);
                sqlJoinTableSource1.setParent(mySqlDeleteStatement);
                mySqlDeleteStatement.setUsing(sqlJoinTableSource1);
                if(oracleSelectQueryBlock.getWhere() instanceof SQLBinaryOpExpr) {
                    SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) oracleSelectQueryBlock.getWhere();
                    if(sqlBinaryOpExpr.getOperator() == SQLBinaryOperator.Equality) {
                        if(sqlBinaryOpExpr.getLeft() instanceof SQLBinaryOpExpr) {
                            if (((SQLBinaryOpExpr)sqlBinaryOpExpr.getLeft()).getOperator() == SQLBinaryOperator.Concat) {
                                String str = sqlBinaryOpExpr.getLeft().toString();
                                SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr(String.format("concat(%s)", str.replaceAll("\\|\\|", ",")));
                                sqlBinaryOpExpr.setLeft(sqlIdentifierExpr);
                                sqlIdentifierExpr.setParent(sqlBinaryOpExpr);
                            }
                        }
                        if(sqlBinaryOpExpr.getRight() instanceof SQLBinaryOpExpr) {
                            if (((SQLBinaryOpExpr)sqlBinaryOpExpr.getRight()).getOperator() == SQLBinaryOperator.Concat) {
                                String str = sqlBinaryOpExpr.getRight().toString();
                                SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr(String.format("concat(%s)", str.replaceAll("\\|\\|", ",")));
                                sqlBinaryOpExpr.setRight(sqlIdentifierExpr);
                                sqlIdentifierExpr.setParent(sqlBinaryOpExpr);
                            }
                        }
                        
                    }
                    mySqlDeleteStatement.setWhere(sqlBinaryOpExpr);
                }
                mySqlDeleteStatement.setDbType(distDbType);
                println();
                print(mySqlDeleteStatement.toString());
                return false;
            }
            Optional.ofNullable(where.getChildren()).orElse(new ArrayList<>()).forEach(expr -> {
                functionTransform.sqlWhereList(expr);
            });
        }
        /*print(";");*/
        return super.visit(x);
    }
    
    @Override
    public boolean visit(OracleCreateIndexStatement x) {
        if(Token.UNIQUE.name.equals(x.getType())) {
            SQLCreateIndexStatement sqlCreateIndexStatement = new SQLCreateIndexStatement();
            sqlCreateIndexStatement.setTable(x.getTable());
            sqlCreateIndexStatement.setName(x.getName());
            x.getItems().stream().forEach(item->sqlCreateIndexStatement.addItem(item));
            sqlCreateIndexStatement.setDbType(distDbType);
            println();
            print(sqlCreateIndexStatement.toString());
            /* print(";");*/
            return false;
        }
        return super.visit(x);
    }
    
    @Override
    public boolean visit(SQLCreateSequenceStatement x) {
        
        return false;
    }
    
    @Override
    public boolean visit(SQLCommentStatement x) {
        if(SQLCommentStatement.Type.TABLE.name().equals(x.getType().name())){
            String tableName = x.getOn().getName().getSimpleName();
            SQLAlterTableStatement sqlAlterStatement = new SQLAlterTableStatement();
            SQLExprTableSource sqlExprTableSource = new SQLExprTableSource();
            sqlExprTableSource.setExpr(tableName);
            sqlAlterStatement.setTableSource(sqlExprTableSource);
            sqlAlterStatement.setDbType(distDbType);
            MySqlAlterTableOption sqlAlterTableOption = new MySqlAlterTableOption();
            sqlAlterTableOption.setName("COMMENT");
            sqlAlterTableOption.setValue(x.getComment());
            sqlAlterTableOption.setParent(sqlAlterStatement);
            sqlAlterStatement.addItem(sqlAlterTableOption);
            println();
            print(sqlAlterStatement.toString());
            /*print(";");*/
            return false;
        }else if(SQLCommentStatement.Type.COLUMN.name().equals(x.getType().name())) {
            String columnName = x.getOn().getName().getSimpleName().replaceAll("\"", "");
            
            String tableName = ((SQLPropertyExpr)x.getOn().getExpr()).getOwner().toString();
            SQLColumnDefinition sqlColumnDefinition = new SQLColumnDefinition();
            sqlColumnDefinition.setDbType(JdbcConstants.MYSQL);
            if(MySqlUtil.containsKeyWords(columnName)){
                sqlColumnDefinition.setName("`"+columnName+"`");
            }else{
                sqlColumnDefinition.setName(columnName);
            }
            sqlColumnDefinition.setDataType(new SQLCharacterDataType((String)MapCacheUtil.getInstance().getCacheData(tableName.toUpperCase()+":"+columnName.toUpperCase())));
            sqlColumnDefinition.setComment(x.getComment());
            MySqlAlterTableModifyColumn mySqlAlterTableModifyColumn = new MySqlAlterTableModifyColumn();
            mySqlAlterTableModifyColumn.setNewColumnDefinition(sqlColumnDefinition);
            
            SQLAlterTableStatement sqlAlterTableStatement = new SQLAlterTableStatement(distDbType);
            SQLExprTableSource sqlExprTableSource = new SQLExprTableSource();
            sqlExprTableSource.setExpr(tableName);
            sqlAlterTableStatement.setTableSource(sqlExprTableSource);
            sqlAlterTableStatement.addItem(mySqlAlterTableModifyColumn);
            println();
            print(sqlAlterTableStatement.toString());
            return false;
        }
        return super.visit(x);
    }
    
    @Override
    public boolean visit(SQLCommitStatement x) {
        println();
        return super.visit(x);
    }
    
    @Override
    public boolean visit(SQLWhileStatement x){
        x.setAfterSemi(true);
        return super.visit(x);
    }
    
    @Override
    public boolean visit(SQLCreateTriggerStatement x) {
        x.setDbType(distDbType);
        SQLBlockStatement body = (SQLBlockStatement)x.getBody();
        body.setDbType(distDbType);
        for(SQLStatement sqlStatement : body.getStatementList()){
            if (sqlStatement instanceof SQLIfStatement) {
                ((SQLIfStatement) sqlStatement).setDbType(distDbType);
            }
        }
        super.visit(x);
        println(SQLUtils.toMySqlString(x));
        return false;
    }
    
    @Override
    public boolean visit(SQLVariantRefExpr x){
        if(":new".equalsIgnoreCase(x.getName()) || ":old".equalsIgnoreCase(x.getName())) {
            x.setName(x.getName().replace(":", "").toLowerCase());
        }
        return super.visit(x);
    }
    
    @Override
    public boolean visit(SQLSelectOrderByItem x) {
        if(Objects.nonNull(x.getNullsOrderType())) { // 处理nulls last || nulls first
            Integer first = 0, last = 0;
            if(SQLSelectOrderByItem.NullsOrderType.NullsFirst.name().equals(x.getNullsOrderType().name())) {
                first = 1;
            } else {
                last = 1;
            }
            SQLMethodInvokeExpr sqlMethodInvokeExpr = new SQLMethodInvokeExpr();
            sqlMethodInvokeExpr.setMethodName("IF");
            SQLMethodInvokeExpr sqlMethodInvokeExpr1 = new SQLMethodInvokeExpr();
            sqlMethodInvokeExpr1.setMethodName("ISNULL");
            sqlMethodInvokeExpr1.addArgument(x.getExpr());
            sqlMethodInvokeExpr1.setParent(sqlMethodInvokeExpr);
            sqlMethodInvokeExpr.addArgument(sqlMethodInvokeExpr1);
            SQLIntegerExpr sqlIntegerExpr = new SQLIntegerExpr();
            sqlIntegerExpr.setNumber(first);
            sqlIntegerExpr.setParent(sqlMethodInvokeExpr);
            SQLIntegerExpr sqlIntegerExpr1 = new SQLIntegerExpr();
            sqlIntegerExpr1.setNumber(last);
            sqlIntegerExpr1.setParent(sqlMethodInvokeExpr);
            sqlMethodInvokeExpr.addArgument(sqlIntegerExpr);
            sqlMethodInvokeExpr.addArgument(sqlIntegerExpr1);
            
            x.setNullsOrderType(null);
            x.setExpr(sqlMethodInvokeExpr);
        }
        return super.visit(x);
    }
    
    @Override
    public boolean visit(SQLMethodInvokeExpr x) {
        functionTransform.methodInvoke(x);
        return super.visit(x);
    }
    
    @Override
    public boolean visit(SQLBinaryOpExpr x) {
        x.setDbType(distDbType);
        if(x.getOperator() == SQLBinaryOperator.Equality) {
            if(x.getLeft() instanceof SQLBinaryOpExpr) {
                if (((SQLBinaryOpExpr)x.getLeft()).getOperator() == SQLBinaryOperator.Concat) {
                    String str = x.getLeft().toString();
                    SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr(String.format("concat(%s)", str.replaceAll("\\|\\|", ",")));
                    print(sqlIdentifierExpr.toString());
                    return false;
                }
            }
            if(x.getRight() instanceof SQLBinaryOpExpr) {
                if (((SQLBinaryOpExpr)x.getRight()).getOperator() == SQLBinaryOperator.Concat) {
                    String str = x.getRight().toString();
                    SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr(String.format("concat(%s)", str.replaceAll("\\|\\|", ",")));
                    print(sqlIdentifierExpr.toString());
                    return false;
                }
            }
            
        }
        return super.visit(x);
    }
}
