package com.raysonfang.sqltranslator.sql;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.raysonfang.sqltranslator.sql.dialect.oracle.OracleToMySqlOutputVisitor;
import junit.framework.TestCase;

import java.util.List;

/**
 * oracle转mysql测试类
 *
 * @author rayson.fang
 * @date 2021/02/05 15:15
 **/
public class OracleToMysqlTest extends TestCase {
    
    public void test1() {
        String sql = "CREATE TABLE T_DICT (\n" +
                "  ID VARCHAR2(32) DEFAULT sys_guid()  NOT NULL ,\n" +
                "  CLASS_ID VARCHAR2(20) ,\n" +
                "  CLASS_NAME VARCHAR2(100) ,\n" +
                "  CLASS_ENAME VARCHAR2(2000) ,\n" +
                "  KEY_ID VARCHAR2(20) ,\n" +
                "  KEY_NAME VARCHAR2(500) ,\n" +
                "  KEY_ENAME VARCHAR2(2000) ,\n" +
                "  OR_VALID_D VARCHAR2(1) DEFAULT 0 ,\n" +
                "  MEMO VARCHAR2(2000) ,\n" +
                "  TIME_MARK TIMESTAMP(6) ,\n" +
                "  STA VARCHAR2(10) DEFAULT 1 ,\n" +
                "  KEY_SEQ NUMBER(20) ,\n" +
                "  CREATE_PRSN VARCHAR2(64) ,\n" +
                "  CREATE_TIME TIMESTAMP(6) DEFAULT systimestamp ,\n" +
                "  UPDATE_PRSN VARCHAR2(64) ,\n" +
                "  UPDATE_TIME TIMESTAMP(6) DEFAULT systimestamp ,\n" +
                "  FDELETE_ID VARCHAR2(1) DEFAULT 0  NOT NULL ,\n" +
                "  constraint PK_T_DICT primary key(ID)\n" +
                ");";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.oracle);
        StringBuilder out = new StringBuilder();
        OracleToMySqlOutputVisitor visitor = new OracleToMySqlOutputVisitor(out, false);
        for(SQLStatement sqlStatement : stmtList) {
            sqlStatement.accept(visitor);
        }
        System.out.println(out.toString());
    }
    
    public void test2_select() {
        String sql = "SELECT '='||TRIM(' HELLO ')||'=' FROM DUAL;";
        sql +="select sys_guid(),sysdate,'oracle'  from dual;";
        sql += "select key,value,to_char(sysdate, 'yyyyMMdd'),to_char(#{num})--测试\n" +
                "       ,to_char(UPDATE_TIME, 'yyyyMMdd'),to_char(12312),to_char(state),to_char(#{num})  val,to_char(1123.26723,'99999999.99'),to_char( to_number( '4' ) + 1, '00' )\n" +
                "       from SYS_TEST where id = #{id}";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.oracle);
        StringBuilder out = new StringBuilder();
        OracleToMySqlOutputVisitor visitor = new OracleToMySqlOutputVisitor(out, false);
        for(SQLStatement sqlStatement : stmtList) {
            sqlStatement.accept(visitor);
        }
        System.out.println(out.toString());
    }
    
    public void test3_listagg() {
        String sql = "SELECT listagg(t2.id,',')within group (order by t2.id)\n" +
                "        from T_BASE_USERDEPT t1\n" +
                "          left join T_BASE_DEPT t2\n" +
                "          on t1.fdept_id = t2.id\n" +
                "        where t1.fuser_id = #{id}";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.oracle);
        StringBuilder out = new StringBuilder();
        OracleToMySqlOutputVisitor visitor = new OracleToMySqlOutputVisitor(out, false);
        for(SQLStatement sqlStatement : stmtList) {
            sqlStatement.accept(visitor);
        }
        System.out.println(out.toString());
    }
}
