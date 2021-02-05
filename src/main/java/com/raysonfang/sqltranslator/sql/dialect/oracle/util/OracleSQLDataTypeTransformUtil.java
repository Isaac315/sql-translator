package com.raysonfang.sqltranslator.sql.dialect.oracle.util;

import com.alibaba.druid.sql.SQLTransformUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.util.FnvHash;

import java.util.List;

/**
 * SQL转换工具类
 * @author rayson.fang
 * @date 2021-2-5 12:34:19
 */
public class OracleSQLDataTypeTransformUtil extends SQLTransformUtils {
    
    public static SQLDataType transformOracleToMySql(SQLDataType x) {
        final String name = x.getName();
        final long nameHash = x.nameHashCode64();
        if (name == null) {
            return x;
        }
        List<SQLExpr> argumentns = x.getArguments();
        SQLDataType dataType;
        if (nameHash == FnvHash.Constants.UROWID) {
            int len = 4000;
            if (argumentns.size() == 1) {
                SQLExpr arg0 = argumentns.get(0);
                if (arg0 instanceof SQLIntegerExpr) {
                    len = ((SQLIntegerExpr) arg0).getNumber().intValue();
                }
            }
            dataType = new SQLDataTypeImpl("varchar", len);
        } else if (nameHash == FnvHash.Constants.ROWID) {
            dataType = new SQLDataTypeImpl("char", 10);

        } else if (nameHash == FnvHash.Constants.BOOLEAN) {
            dataType = new SQLDataTypeImpl("tinyint");

        } else if (nameHash == FnvHash.Constants.INTEGER) {
            dataType = new SQLDataTypeImpl("int");

        } else if (nameHash == FnvHash.Constants.FLOAT
                || nameHash == FnvHash.Constants.BINARY_FLOAT) {
            dataType = new SQLDataTypeImpl("float");

        } else if (nameHash == FnvHash.Constants.REAL
                || nameHash == FnvHash.Constants.BINARY_DOUBLE
                || nameHash == FnvHash.Constants.DOUBLE_PRECISION) {
            dataType = new SQLDataTypeImpl("double");

        } else if (nameHash == FnvHash.Constants.NUMBER) {
            if (argumentns.size() == 0) {
                dataType = new SQLDataTypeImpl("decimal", 38);
            } else {
                SQLExpr arg0 = argumentns.get(0);

                int precision, scale = 0;
                if (arg0 instanceof SQLAllColumnExpr) {
                    precision = 9;
                } else {
                    precision = ((SQLIntegerExpr) arg0).getNumber().intValue();
                }

                if (argumentns.size() > 1) {
                    scale = ((SQLIntegerExpr) argumentns.get(1)).getNumber().intValue();
                }

                if (scale > precision) {
                    if (arg0 instanceof SQLAllColumnExpr) {
                        precision = 19;
                        if (scale > precision) {
                            precision = scale;
                        }
                    } else {
                        precision = scale;
                    }
                }

                if (scale == 0) {
                    if (precision < 3) {
                        dataType = new SQLDataTypeImpl("tinyint");
                    } else if (precision < 5) {
                        dataType = new SQLDataTypeImpl("smallint");
                    } else if (precision < 9) {
                        dataType = new SQLDataTypeImpl("int");
                    } else if (precision <= 20) {
                        dataType = new SQLDataTypeImpl("bigint");
                    } else {
                        dataType = new SQLDataTypeImpl("decimal", precision);
                    }
                } else {
                    dataType = new SQLDataTypeImpl("decimal", precision, scale);
                }
            }

        } else if (nameHash == FnvHash.Constants.DEC
                || nameHash == FnvHash.Constants.DECIMAL) {

            dataType = x.clone();
            dataType.setName("decimal");

            int precision = 0;
            if (argumentns.size() > 0) {
                precision = ((SQLIntegerExpr) argumentns.get(0)).getNumber().intValue();
            }

            int scale = 0;
            if (argumentns.size() > 1) {
                scale = ((SQLIntegerExpr) argumentns.get(1)).getNumber().intValue();
                if (precision < scale) {
                    ((SQLIntegerExpr) dataType.getArguments().get(1)).setNumber(precision);
                }
            }
        } else if (nameHash == FnvHash.Constants.RAW) {
            int len;

            if (argumentns.size() == 0) {
                len = -1;
            } else if (argumentns.size() == 1) {
                SQLExpr arg0 = argumentns.get(0);
                if (arg0 instanceof SQLNumericLiteralExpr) {
                    len = ((SQLNumericLiteralExpr) arg0).getNumber().intValue();
                } else {
                    throw new UnsupportedOperationException(SQLUtils.toOracleString(x));
                }
            } else {
                throw new UnsupportedOperationException(SQLUtils.toOracleString(x));
            }

            if (len == -1) {
                dataType = new SQLDataTypeImpl("binary");
            } else if (len <= 255) {
                dataType = new SQLDataTypeImpl("binary", len);
            } else {
                dataType = new SQLDataTypeImpl("varbinary", len);
            }
        } else if (nameHash == FnvHash.Constants.CHAR
                || nameHash == FnvHash.Constants.CHARACTER) {
            if (argumentns.size() == 1) {
                SQLExpr arg0 = argumentns.get(0);

                int len;
                if (arg0 instanceof SQLNumericLiteralExpr) {
                    len = ((SQLNumericLiteralExpr) arg0).getNumber().intValue();
                } else {
                    throw new UnsupportedOperationException(SQLUtils.toOracleString(x));
                }

                if (len <= 255) {
                    dataType = new SQLCharacterDataType("char", len);
                } else {
                    dataType = new SQLCharacterDataType("varchar", len);
                }
            } else if (argumentns.size() == 0) {
                dataType = new SQLCharacterDataType("char");
            } else {
                throw new UnsupportedOperationException(SQLUtils.toOracleString(x));
            }

        } else if (nameHash == FnvHash.Constants.NCHAR) {
            if (argumentns.size() == 1) {
                SQLExpr arg0 = argumentns.get(0);

                int len;
                if (arg0 instanceof SQLNumericLiteralExpr) {
                    len = ((SQLNumericLiteralExpr) arg0).getNumber().intValue();
                } else {
                    throw new UnsupportedOperationException(SQLUtils.toOracleString(x));
                }

                if (len <= 255) {
                    dataType = new SQLCharacterDataType("nchar", len);
                } else {
                    dataType = new SQLCharacterDataType("nvarchar", len);
                }
            } else if (argumentns.size() == 0) {
                dataType = new SQLCharacterDataType("nchar");
            } else {
                throw new UnsupportedOperationException(SQLUtils.toOracleString(x));
            }

        } else if (nameHash == FnvHash.Constants.VARCHAR2) {
            if (argumentns.size() > 0) {
                int len;
                SQLExpr arg0 = argumentns.get(0);
                if (arg0 instanceof SQLNumericLiteralExpr) {
                    len = ((SQLNumericLiteralExpr) arg0).getNumber().intValue();
                } else {
                    throw new UnsupportedOperationException(SQLUtils.toOracleString(x));
                }
                if(len >= 4000) {
                    dataType = new SQLCharacterDataType("text");
                }else {
                    dataType = new SQLCharacterDataType("varchar", len);
                }
            } else {
                dataType = new SQLCharacterDataType("varchar");
            }

        } else if (nameHash == FnvHash.Constants.NVARCHAR2) {
            if (argumentns.size() > 0) {
                int len;
                SQLExpr arg0 = argumentns.get(0);
                if (arg0 instanceof SQLNumericLiteralExpr) {
                    len = ((SQLNumericLiteralExpr) arg0).getNumber().intValue();
                } else {
                    throw new UnsupportedOperationException(SQLUtils.toOracleString(x));
                }
                dataType = new SQLCharacterDataType("nvarchar", len);
            } else {
                dataType = new SQLCharacterDataType("nvarchar");
            }

        } else if (nameHash == FnvHash.Constants.BFILE) {
            dataType = new SQLCharacterDataType("varchar", 255);

        } else if (nameHash == FnvHash.Constants.DATE
                || nameHash == FnvHash.Constants.TIMESTAMP) {
            int len = -1;
            if (argumentns.size() > 0) {
                SQLExpr arg0 = argumentns.get(0);
                if (arg0 instanceof SQLNumericLiteralExpr) {
                    len = ((SQLNumericLiteralExpr) arg0).getNumber().intValue();
                } else {
                    throw new UnsupportedOperationException(SQLUtils.toOracleString(x));
                }
            }

            /*if (len >= 0) {
                if (len > 6) {
                    len = 6;
                }
                dataType = new SQLDataTypeImpl("datetime", len);
            } else {*/
                dataType = new SQLDataTypeImpl("datetime");
           /* }*/
        } else if (nameHash == FnvHash.Constants.BLOB
                || nameHash == FnvHash.Constants.LONG_RAW) {
            argumentns.clear();
            dataType = new SQLDataTypeImpl("LONGBLOB");

        } else if (nameHash == FnvHash.Constants.CLOB
                || nameHash == FnvHash.Constants.NCLOB
                || nameHash == FnvHash.Constants.LONG
                || nameHash == FnvHash.Constants.XMLTYPE) {
            argumentns.clear();
            dataType = new SQLCharacterDataType("LONGTEXT");
        } else {
            dataType = x;
        }

        if (dataType != x) {
            dataType.setParent(x.getParent());
        }

        return dataType;
    }
}
