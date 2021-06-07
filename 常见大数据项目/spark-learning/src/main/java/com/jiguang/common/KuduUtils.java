package com.jiguang.common;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Update;
import org.apache.kudu.client.Upsert;

import com.alibaba.fastjson.JSONObject;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-07
 */
public class KuduUtils {

    private static final ThreadLocal<KuduSession> threadLocal = new ThreadLocal();

    public static KuduTable table(String name) throws KuduException {
        return Kudu.INSTANCE.table(name);
    }

    public static Insert emptyInsert(String table) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        return ktable.newInsert();
    }

    public static Update emptyUpdate(String table) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        return ktable.newUpdate();
    }

    public static Upsert emptyUpsert(String table) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        return ktable.newUpsert();
    }

    /**
     * Only columns which are part of the key can be set
     *
     * @param table
     * @return
     */
    public static Delete emptyDelete(String table) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        return ktable.newDelete();
    }

    public static Upsert createUpsert(String table, JSONObject data) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        Upsert upsert = ktable.newUpsert();
        PartialRow row = upsert.getRow();
        Schema schema = ktable.getSchema();
        for (String colName : data.keySet()) {
            ColumnSchema colSchema = schema.getColumn(colName);
            fillRow(row, colSchema, data);
        }
        return upsert;
    }

    public static Insert createInsert(String table, JSONObject data) throws KuduException {
        KuduTable ktable = Kudu.INSTANCE.table(table);
        Insert insert = ktable.newInsert();
        PartialRow row = insert.getRow();
        Schema schema = ktable.getSchema();
        for (String colName : data.keySet()) {
            ColumnSchema colSchema = schema.getColumn(colName.toLowerCase());
            fillRow(row, colSchema, data);
        }
        return insert;
    }

    public static void insert(String table, JSONObject data) throws KuduException {
        Insert insert = createInsert(table, data);
        KuduSession session = getSession();
        session.apply(insert);
        session.flush();
        closeSession();
    }

    public static void upsert(String table, JSONObject data) throws KuduException {
        Upsert upsert = createUpsert(table, data);
        KuduSession session = getSession();
        session.apply(upsert);
        session.flush();
        closeSession();
    }



    private static void fillRow(PartialRow row, ColumnSchema colSchema, JSONObject data) {
        String name = colSchema.getName();
        if (data.get(name) == null) {
            return;
        }

        Type type = colSchema.getType();
        switch (type) {
            case STRING:
                row.addString(name, data.getString(name));
                break;
            case INT64:
            case UNIXTIME_MICROS:
                row.addLong(name, data.getLongValue(name));
                break;
            case DOUBLE:
                row.addDouble(name, data.getDoubleValue(name));
                break;
            case INT32:
                row.addInt(name, data.getIntValue(name));
                break;
            case INT16:
                row.addShort(name, data.getShortValue(name));
                break;
            case INT8:
                row.addByte(name, data.getByteValue(name));
                break;
            case BOOL:
                row.addBoolean(name, data.getBooleanValue(name));
                break;
            case BINARY:
                row.addBinary(name, data.getBytes(name));
                break;
            case FLOAT:
                row.addFloat(name, data.getFloatValue(name));
                break;
            default:
                break;
        }
    }

    public static KuduSession getSession() throws KuduException {
        KuduSession session = threadLocal.get();
        if (session == null) {
            session = Kudu.INSTANCE.newAsyncSession();
            threadLocal.set(session);
        }
        return session;
    }

    public static KuduSession getAsyncSession() throws KuduException {
        KuduSession session = threadLocal.get();
        if (session == null) {
            session = Kudu.INSTANCE.newAsyncSession();
            threadLocal.set(session);
        }
        return session;
    }

    public static void closeSession() {
        KuduSession session = threadLocal.get();
        threadLocal.set(null);
        Kudu.INSTANCE.closeSession(session);
    }

}
