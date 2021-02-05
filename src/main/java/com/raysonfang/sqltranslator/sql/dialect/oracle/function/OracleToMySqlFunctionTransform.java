package com.raysonfang.sqltranslator.sql.dialect.oracle.function;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.oracle.ast.expr.OracleSysdateExpr;
import com.alibaba.druid.util.StringUtils;
import com.raysonfang.sqltranslator.sql.dialect.mysql.util.MySqlUtil;
import com.raysonfang.sqltranslator.sql.dialect.oracle.util.OracleSQLDataTypeTransformUtil;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * oracle函数 转换成 mysql函数
 *
 * @author rayson.fang
 * @date 2021/02/05 12:38
 **/
public class OracleToMySqlFunctionTransform implements OracleFunction {
    
    @Override
    public void transformFunctionExpr(SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLMethodInvokeExpr) { //函数
            forEachMethod(sqlExpr);
            alias(sqlExpr);
        } else if (sqlExpr instanceof SQLIdentifierExpr) { //字段
            String name = ((SQLIdentifierExpr) sqlExpr).getName().toUpperCase();
            alias(sqlExpr);
            if ("SYSTIMESTAMP".equalsIgnoreCase(name)) {
                ((SQLIdentifierExpr) sqlExpr).setName("CURRENT_TIMESTAMP");
            } else {
                ((SQLIdentifierExpr) sqlExpr).setName(formatKeyField(name));
            }
        } else if (sqlExpr instanceof SQLPropertyExpr) {  //参数
            String name = ((SQLPropertyExpr) sqlExpr).getName();
            alias(sqlExpr);
            ((SQLPropertyExpr) sqlExpr).setName(formatKeyField(name.toUpperCase()));
        } else if (sqlExpr instanceof SQLCharExpr) {  // 字符
            alias(sqlExpr);
        } else if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOperator operator = ((SQLBinaryOpExpr) sqlExpr).getOperator();
            Optional.ofNullable(sqlExpr.getChildren()).orElse(new ArrayList<>()).forEach(e -> {
                if (e instanceof SQLMethodInvokeExpr) { //函数
                    // forEachMethod((SQLMethodInvokeExpr)e);
                    methodInvoke((SQLMethodInvokeExpr) e);
                }
            });
            
            if (SQLBinaryOperator.Concat == operator) {
                String str = sqlExpr.toString();
                SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr(String.format("concat(%s)", str.replaceAll("\\|\\|", ",")));
                sqlIdentifierExpr.setParent(sqlExpr.getParent());
                SQLObject parent = sqlExpr.getParent();
                if (parent instanceof SQLSelectItem) {
                    ((SQLSelectItem) parent).setExpr(sqlIdentifierExpr);
                }
            } else if (sqlExpr instanceof SQLAggregateExpr) {
                methodInvoke((SQLMethodInvokeExpr) sqlExpr);
            }
            
            alias(sqlExpr);
        
        } else if (sqlExpr instanceof SQLCastExpr) {
            SQLDataType dataType = ((SQLCastExpr) sqlExpr).getDataType();
            if (!ObjectUtils.isEmpty(dataType) && "timestamp".equalsIgnoreCase(dataType.getName())) {
                dataType.setName("datetime");
            }
        }
    }
    
    @Override
    public String concat(SQLObject sqlObject) {
        StringBuilder sb = new StringBuilder();
        if (sqlObject instanceof SQLBinaryOpExpr) {
            SQLExpr left = ((SQLBinaryOpExpr) sqlObject).getLeft();
            sb.append(concat(left));
            SQLExpr right = ((SQLBinaryOpExpr) sqlObject).getRight();
            sb.append(concat(right));
        }
        if (sqlObject instanceof SQLIdentifierExpr) {
            String name = ((SQLIdentifierExpr) sqlObject).getName();
            sb.append(name);
            sb.append(" ");
        } else if (sqlObject instanceof SQLCharExpr) {
            String name = ((SQLCharExpr) sqlObject).getText();
            sb.append(" '");
            sb.append(name);
            sb.append("' ");
        }
        return sb.toString();
    }
    
    @Override
    public void listagg(SQLMethodInvokeExpr expr) {
        if (expr instanceof SQLAggregateExpr) {
            String methodName = expr.getMethodName();
            if ("listagg".equalsIgnoreCase(methodName)) {
                List children = expr.getChildren();
                StringBuffer sb = new StringBuffer();
                StringBuffer field = new StringBuffer();
                StringBuffer fieldVal = new StringBuffer();
                StringBuffer orderBy = new StringBuffer();
                children.forEach(e -> {
                    if (e instanceof SQLPropertyExpr) {
                        field.append(((SQLPropertyExpr) e).getName());
                    } else if (e instanceof SQLCharExpr) {
                        fieldVal.append(" separator '" + ((SQLCharExpr) e).getText());
                        fieldVal.append("'");
                    } else if (e instanceof SQLOrderBy) {
                        ((SQLOrderBy) e).getItems().forEach(c -> {
                            orderBy.append(c.getExpr().toString());
                        });
                    }
                
                });
                sb.append("group_concat(");
                sb.append(field);
                sb.append(" ");
                if (orderBy.toString().length() > 0) {
                    sb.append(" order by ");
                    sb.append(orderBy);
                }
            
                sb.append(" ");
                sb.append(fieldVal);
                sb.append(")");
                identifierExpr(sb.toString(), expr);
            }
        }
    }
    
    @Override
    public void sys_guid(SQLMethodInvokeExpr expr) {
        identifierExpr("md5(uuid())", expr);
    }
    
    @Override
    public void to_char(SQLMethodInvokeExpr expr) {
        expr.setMethodName("date_format");
        SQLIdentifierExpr sqlCharExpr = null;
        List<SQLObject> children = expr.getChildren();
        for (int i = 0, listSize = children.size(); i < listSize; i++) {
            SQLObject sqlObj = children.get(i);
            if (sqlObj instanceof SQLCharExpr) {
                String textStr = ((SQLCharExpr) sqlObj).getText();
                if (textStr.indexOf("99") < 0) {
                    ((SQLCharExpr) sqlObj).setText(formatDate(textStr));
                } else {
                
                    sqlCharExpr = new SQLIdentifierExpr(String.valueOf(getRound(textStr)));
                    sqlCharExpr.setParent(sqlObj.getParent());
                    children.set(i, sqlCharExpr);
                    expr.setMethodName("ROUND");
                }
            } else {
                String val = "";
                int length = 0;
                if (sqlObj instanceof SQLIntegerExpr) {
                    val = ((SQLIntegerExpr) sqlObj).getValue().toString();
                    length = val.length();
                } else if (sqlObj instanceof SQLNumberExpr) {
                    val = ((SQLNumberExpr) sqlObj).getNumber().toString();
                    length = val.length();
                    if ((i + 1) <= listSize) {
                        SQLObject sqlObjNext = children.get(i + 1);
                        if (sqlObjNext instanceof SQLCharExpr) {
                            continue;
                        }
                    }
                
                } else if (sqlObj instanceof SQLIdentifierExpr) {
                    if (sqlObj.getParent() instanceof SQLMethodInvokeExpr) {
                        if (((SQLMethodInvokeExpr) sqlObj.getParent()).getArguments().size() > 1) {
                            continue;
                        }
                    }
                    val = ((SQLIdentifierExpr) sqlObj).getName();
                    length = 11;
                } else if (sqlObj instanceof SQLVariantRefExpr) {
                    if ((((SQLVariantRefExpr) sqlObj).getChildren().size()) > 1) {
                        continue;
                    }
                    val = ((SQLVariantRefExpr) sqlObj).getName();
                    length = 11;
                }else if (sqlObj instanceof OracleSysdateExpr) {
                    children.set(i, new SQLIdentifierExpr("now()"));
                    continue;
                } else {
                    continue;
                }
                sqlCharExpr = new SQLIdentifierExpr(String.format("%s AS CHAR(%d)", val, length));
                sqlCharExpr.setParent(sqlObj.getParent());
                children.set(i, sqlCharExpr);
                expr.setMethodName("cast");
            }
        }
    }
    
    @Override
    public void to_date(SQLMethodInvokeExpr expr) {
        expr.setMethodName("str_to_date");
        List<SQLObject> children = expr.getChildren();
        for (int i = 0, listSize = children.size(); i < listSize; i++) {
            SQLObject sqlObj = children.get(i);
            if (sqlObj instanceof SQLCharExpr) {
                String textStr = ((SQLCharExpr) sqlObj).getText();
                ((SQLCharExpr) sqlObj).setText(formatDate(textStr));
            }
        }
    }
    
    @Override
    public void to_timestamp(SQLMethodInvokeExpr expr) {
        expr.setMethodName("str_to_date");
        List<SQLObject> children = expr.getChildren();
        for (int i = 0, listSize = children.size(); i < listSize; i++) {
            SQLObject sqlObj = children.get(i);
            if (sqlObj instanceof SQLCharExpr) {
                String textStr = ((SQLCharExpr) sqlObj).getText();
                ((SQLCharExpr) sqlObj).setText(formatDate(textStr));
            }
        }
    }
    
    @Override
    public void to_number(SQLMethodInvokeExpr expr) {
        expr.setMethodName("cast");
        List<SQLObject> children = expr.getChildren();
        SQLIdentifierExpr sqlCharExpr = null;
        for (int i = 0, listSize = children.size(); i < listSize; i++) {
            SQLObject sqlObj = children.get(i);
            if (sqlObj instanceof SQLCharExpr) {
                String textStr = ((SQLCharExpr) sqlObj).getText();
                sqlCharExpr = new SQLIdentifierExpr(String.format("'%s' AS SIGNED", textStr));
                sqlCharExpr.setParent(sqlObj.getParent());
                children.set(i, sqlCharExpr);
            } else if (sqlObj instanceof SQLIdentifierExpr) {
                String name = ((SQLIdentifierExpr) sqlObj).getName();
                ((SQLIdentifierExpr) sqlObj).setName(String.format("%s AS SIGNED", name));
            } else if (sqlObj instanceof SQLVariantRefExpr) {
                String name = ((SQLVariantRefExpr) sqlObj).getName();
                ((SQLVariantRefExpr) sqlObj).setName(String.format("%s AS SIGNED", name));
            }
        }
    }
    
    @Override
    public void trunc(SQLMethodInvokeExpr expr) {
        List<SQLObject> children = expr.getChildren();
        SQLIdentifierExpr sqlIdentifierExpr = null;
        expr.setMethodName("truncate");
        for (int i = 0, listSize = children.size(); i < listSize; i++) {
            SQLObject sqlObj = children.get(i);
            if (sqlObj instanceof OracleSysdateExpr) {
                sqlIdentifierExpr = new SQLIdentifierExpr();
                sqlIdentifierExpr.setName("now(), '%Y-%m-%d'");
                sqlIdentifierExpr.setParent(sqlObj.getParent());
                children.set(i, sqlIdentifierExpr);
                expr.setMethodName("date_format");
            } else if (sqlObj instanceof SQLIdentifierExpr) {
                String name = ((SQLIdentifierExpr) sqlObj).getName();
                if (name.toLowerCase().indexOf("time") > 0) { // 如果是一个字段 必须包含time
                    sqlIdentifierExpr = new SQLIdentifierExpr();
                    sqlIdentifierExpr.setName(name + ", '%Y-%m-%d'");
                    sqlIdentifierExpr.setParent(sqlObj.getParent());
                    children.set(i, sqlIdentifierExpr);
                    expr.setMethodName("date_format");
                }
                // 其他占卜做处理
            } else if (sqlObj instanceof SQLNumberExpr) {
                Number number = ((SQLNumberExpr) sqlObj).getNumber();
                sqlIdentifierExpr = new SQLIdentifierExpr();
                if (number.intValue() > 0) {
                    String num = "0";
                    List<SQLExpr> arguments = ((SQLMethodInvokeExpr) sqlObj.getParent()).getArguments();
                    if (arguments != null && arguments.size() == 2) {
                        if (arguments.get(1) instanceof SQLIntegerExpr) {
                            num = ((SQLIntegerExpr) arguments.get(1)).getNumber().toString();
                        }
                    }
                    sqlIdentifierExpr.setName(String.format("%s, %s", number.toString(), num));
                    expr.setMethodName("truncate");
                } else {
                    sqlIdentifierExpr.setName(String.format("%s as SIGNED", number.toString()));
                    expr.setMethodName("cast");
                }
                sqlIdentifierExpr.setParent(sqlObj.getParent());
                children.set(i, sqlIdentifierExpr);
            } else if (sqlObj instanceof SQLCharExpr) {
                String text = ((SQLCharExpr) sqlObj).getText();
                ((SQLCharExpr) sqlObj).setText(formatDate(text));
                expr.setMethodName("date_format");
            } else if (sqlObj instanceof SQLIntegerExpr) {
                children.remove(i);
            }
        }
    }
    
    @Override
    public void nvl(SQLMethodInvokeExpr expr) {
        expr.setMethodName("ifnull");
    }
    
    @Override
    public void nvl2(SQLMethodInvokeExpr expr) {
        expr.setMethodName("if");
    }
    
    @Override
    public void length(SQLMethodInvokeExpr expr) {
        expr.setMethodName("char_length");
    }
    
    @Override
    public void instr(SQLMethodInvokeExpr expr) {
        List<SQLObject> children = expr.getChildren();
        expr.setMethodName("locate");
        if (children.size() >= 2) {
            SQLObject sqlObject = children.get(0);
            children.set(0, children.get(1));
            children.set(1, sqlObject);
        }
    }
    
    @Override
    public void substr(SQLMethodInvokeExpr expr) {
        List<SQLObject> children = expr.getChildren();
        expr.setMethodName("substr");
        if (children.size() >= 2) {
            SQLObject sqlObject = children.get(1);
            if (sqlObject instanceof SQLNumberExpr) {
                int val = ((SQLNumberExpr) sqlObject).getNumber().intValue();
                if (val <= 0) {
                    ((SQLNumberExpr) sqlObject).setNumber(1);
                }
            } else if (sqlObject instanceof SQLIntegerExpr) {
                int val = ((SQLIntegerExpr) sqlObject).getNumber().intValue();
                if (val <= 0) {
                    ((SQLIntegerExpr) sqlObject).setNumber(1);
                }
            }
        }
    }
    
    @Override
    public void add_months(SQLMethodInvokeExpr expr) {
        List<SQLObject> children = expr.getChildren();
        expr.setMethodName("date_add");
        SQLIdentifierExpr sqlCharExpr = null;
        for (int i = 0, listSize = children.size(); i < listSize; i++) {
            SQLObject sqlObj = children.get(i);
            if (sqlObj instanceof SQLIntegerExpr) {
                String num = ((SQLIntegerExpr) sqlObj).getNumber().toString();
                sqlCharExpr = new SQLIdentifierExpr();
                sqlCharExpr.setName(String.format("interval %s month", num));
                sqlCharExpr.setParent(sqlCharExpr);
                children.set(i, sqlCharExpr);
            }
        }
    }
    
    @Override
    public void hextoraw(SQLMethodInvokeExpr expr) {
        expr.setMethodName("UNHEX");
    }
    
    @Override
    public void decode(SQLMethodInvokeExpr expr) {
        SQLExpr expr1 = OracleSQLDataTypeTransformUtil.transformDecode(expr);
        if (expr.getParent() instanceof SQLSelectItem) {
            ((SQLSelectItem) expr.getParent()).setExpr(expr1);
        } else if (expr.getParent() instanceof SQLBinaryOpExpr) {
            ((SQLBinaryOpExpr) expr.getParent()).setRight(expr1);
        }
    }
    
    @Override
    public String formatDate(String format) {
        if(format.toUpperCase().startsWith("S")) {
            format = format.substring(1);
        }
        return format.toUpperCase()
                .replaceAll("YYYY", "%Y")
                .replaceAll("MM", "%m")
                .replaceAll("DD", "%d")
                .replaceAll("HH24", "%H")
                .replaceAll("MI", "%i")
                .replaceAll("SS", "%s")
                .replaceAll(".FF", ".%f")
                .replaceAll(":FF", ".%f");
    }
    
    @Override
    public String formatKeyField(String format) {
        if (MySqlUtil.containsKeyWords(format)) {
            return String.format("`%s`", format.toUpperCase());
        }
        return format;
    }
    
    @Override
    public void alias(SQLExpr sqlExpr) {
        if (sqlExpr.getParent() instanceof SQLSelectItem) {//取别名 as
            String alias = ((SQLSelectItem) sqlExpr.getParent()).getAlias();
            if (!StringUtils.isEmpty(alias)) {
                if (alias.lastIndexOf("\"") <= 0) {
                    ((SQLSelectItem) sqlExpr.getParent()).setAlias(formatKeyField(alias.toUpperCase()));
                }
            }
        }
    }
    
    /**
     * 获取四舍五入小数点多少位
     * to_char(999995.6, '9999999999990.999999')
     * @author rayson.fang
     * @date 2021/02/05 13:36
     * @param str
     * @return java.lang.Integer
     */
    private Integer getRound(String str) {
        int round = 0;
        if (str.indexOf(".") > 0) {
            round = str.split("\\.")[1].length();
        }
        return round;
    }
}
