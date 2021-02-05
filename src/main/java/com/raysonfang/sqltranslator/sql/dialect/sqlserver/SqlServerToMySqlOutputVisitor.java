package com.raysonfang.sqltranslator.sql.dialect.sqlserver;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.dialect.sqlserver.visitor.SQLServerOutputVisitor;

/**
 * sqlserver  转 mysql
 *
 * @author fanglei
 * @date 2021/02/05 17:55
 **/
public class SqlServerToMySqlOutputVisitor extends SQLServerOutputVisitor {
    
    
    // 目标数据库类型
    private final DbType distDbType = DbType.mysql;
    
    public SqlServerToMySqlOutputVisitor(Appendable appender) {
        super(appender);
    }
    
    public SqlServerToMySqlOutputVisitor(Appendable appender, boolean parameterized) {
        super(appender, parameterized);
    }
}
