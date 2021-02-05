package com.raysonfang.sqltranslator.sql.dialect.mysql.util;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
import org.springframework.util.StringUtils;

/**
 * mysql 相关工具类
 *
 * @author fanglei
 * @date 2021/02/05 14:12
 **/
public class MySqlUtil {
    
    public static boolean containsKeyWords(String name) {
        if (StringUtils.isEmpty(name)) {
            return false;
        }
        return MySqlLexer.DEFAULT_MYSQL_KEYWORDS.getKeywords().containsKey(name.toUpperCase());
    }
}
