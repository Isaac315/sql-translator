package com.raysonfang.sqltranslator.sql.dialect.oracle.function;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.util.StringUtils;
import com.raysonfang.sqltranslator.sql.MethodInvoke;

/**
* oracle需要转换的函数
* @author rayson.fang
* @date 2021/02/05 12:44
*/
public interface OracleFunction extends MethodInvoke {
    
    /**
    * 转换函数
    * @author rayson.fang
    * @date 2021/02/05 13:23
    * @param sqlExpr
    * @return void
    */
    void transformFunctionExpr(SQLExpr sqlExpr);
    
    /**
    * 转换 oracle 字符串拼接 'a' || 'b'
    * @author rayson.fang
    * @date 2021/02/05 12:46
    * @param sqlObject
    * @return java.lang.String
    */
    String concat(SQLObject sqlObject);
    
    /**
    * 转换 listagg
    * @author rayson.fang
    * @date 2021/02/05 13:07
    * @param expr
    * @return void
    */
    void listagg(SQLMethodInvokeExpr expr);
    
    /**
    * 转换 sys_guid
    * @author rayson.fang
    * @date 2021/02/05 13:23
     * @param expr
    * @return void
    */
    void sys_guid(SQLMethodInvokeExpr expr);
    
    /**
    * 转换 to_char
    * @author rayson.fang
    * @date 2021/02/05 13:24
    * @param expr
    * @return void
    */
    void to_char(SQLMethodInvokeExpr expr);
    
    /**
    * 转换 to_date
    * @author rayson.fang
    * @date 2021/02/05 13:25
    * @param expr
    * @return void
    */
    void to_date(SQLMethodInvokeExpr expr);
    
    /**
    * 转换 to_timestamp
    * @author rayson.fang
    * @date 2021/02/05 13:26
    * @param expr
    * @return void
    */
    void to_timestamp(SQLMethodInvokeExpr expr);
    
    /**
    * 转换 to_number
    * @author rayson.fang
    * @date 2021/02/05 13:26
    * @param expr
    * @return void
    */
    void to_number(SQLMethodInvokeExpr expr);
    
    /**
     * 转换 trunc
     * @author rayson.fang
     * @date 2021/02/05 13:26
     * @param expr
     * @return void
     */
    void trunc(SQLMethodInvokeExpr expr);
    
    /**
    * 转换 nvl
    * @author rayson.fang
    * @date 2021/02/05 13:27
    * @param expr
    * @return void
    */
    void nvl(SQLMethodInvokeExpr expr);
    
    /**
    * 转换 nvl2
    * @author rayson.fang
    * @date 2021/02/05 13:28
    * @param expr
    * @return void
    */
    void nvl2(SQLMethodInvokeExpr expr);
    
    /**
     * 转换 length
     * @author rayson.fang
     * @date 2021/02/05 13:28
     * @param expr
     * @return void
     */
    void length(SQLMethodInvokeExpr expr);
    
    /**
     * 转换 instr
     * @author rayson.fang
     * @date 2021/02/05 13:28
     * @param expr
     * @return void
     */
    void instr(SQLMethodInvokeExpr expr);
    
    /**
     * 转换 substr
     * @author rayson.fang
     * @date 2021/02/05 13:28
     * @param expr
     * @return void
     */
    void substr(SQLMethodInvokeExpr expr);
    
    /**
     * 转换 add_months
     * @author rayson.fang
     * @date 2021/02/05 13:28
     * @param expr
     * @return void
     */
    void add_months(SQLMethodInvokeExpr expr);
    
    /**
     * 转换 hextoraw
     * @author rayson.fang
     * @date 2021/02/05 13:28
     * @param expr
     * @return void
     */
    void hextoraw(SQLMethodInvokeExpr expr);
    
    /**
     * 转换 decode
     * @author rayson.fang
     * @date 2021/02/05 13:28
     * @param expr
     * @return void
     */
    void decode(SQLMethodInvokeExpr expr);
    
    /**
    * 日期 格式化转换
    * @author rayson.fang
    * @date 2021/02/05 13:30
    * @param format
    * @return java.lang.String
    */
    String formatDate(String format);
    
    /**
    * 关键字 格式化转换
    * @author rayson.fang
    * @date 2021/02/05 13:31
    * @param format
    * @return java.lang.String
    */
    String formatKeyField(String format);
    
    /**
    * 处理别名
    * @author rayson.fang
    * @date 2021/02/05 13:50
    * @param sqlExpr
    * @return void
    */
    void alias(SQLExpr sqlExpr);
    
    /**
     * 整体替换当前类型
     * @author rayson.fang
     * @date 2021/02/05 13:37
     * @param name
     * @param expr
     * @return void
     */
    default void identifierExpr(String name, SQLObject expr) {
        SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
        sqlIdentifierExpr.setName(name);
        sqlIdentifierExpr.setParent(expr.getParent());
        SQLObject parent = expr.getParent();
        if (parent instanceof SQLSelectItem) {
            ((SQLSelectItem) parent).setExpr(sqlIdentifierExpr);
        } else if (parent instanceof SQLBinaryOpExpr) {
            ((SQLBinaryOpExpr) parent).setRight(sqlIdentifierExpr);
        }
    }
    
    /**
    *
    * @author rayson.fang
    * @date 2021/02/05 14:23
    * @param sqlObject
    * @return void
    */
    default void sqlWhereList(SQLObject sqlObject) {
        if (sqlObject instanceof SQLBinaryOpExpr) {
            SQLExpr left = ((SQLBinaryOpExpr) sqlObject).getLeft();
            SQLExpr right = ((SQLBinaryOpExpr) sqlObject).getRight();
            transformFunctionExpr(left);
            transformFunctionExpr(right);
            sqlWhereList(left);
            sqlWhereList(right);
        } else if (sqlObject instanceof SQLMethodInvokeExpr) {
            methodInvoke((SQLMethodInvokeExpr) sqlObject);
        } else if (sqlObject instanceof SQLCharExpr) {
            String text = ((SQLCharExpr) sqlObject).getText();
            if (StringUtils.isEmpty(text)) {
                identifierExpr("''", sqlObject);
            }
        }
    }
}
