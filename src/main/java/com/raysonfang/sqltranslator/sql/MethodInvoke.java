package com.raysonfang.sqltranslator.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Optional;

/**
* sql方法反射实现
* @author rayson.fang
* @date 2021/02/05 12:16
*/
public interface MethodInvoke {
    
    Logger log = LoggerFactory.getLogger(MethodInvoke.class);
    
    /**
     * 方法转换
     * @param expr
     */
    default void methodInvoke(SQLMethodInvokeExpr expr){
        if (ObjectUtils.isEmpty(expr)) {
            return;
        }
        Method method = ReflectionUtils.findMethod(this.getClass(), expr.getMethodName().toLowerCase(), SQLMethodInvokeExpr.class);
        if (null != method) {
            try {
                method.invoke(this, new Object[]{expr});
            } catch (Exception e) {
                log.error("method invoke error", e);
                e.printStackTrace();
            }
        }
    }
    
    /**
     * 处理嵌套函数
     *
     * @param sqlExpr
     */
    default void forEachMethod(SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLMethodInvokeExpr) {
            methodInvoke((SQLMethodInvokeExpr) sqlExpr);
        }
        Optional.ofNullable(sqlExpr.getChildren()).orElse(new ArrayList<>()).forEach(e -> {
            if (e instanceof SQLMethodInvokeExpr) {
                forEachMethod((SQLMethodInvokeExpr) e);
            }
        });
    }
}
