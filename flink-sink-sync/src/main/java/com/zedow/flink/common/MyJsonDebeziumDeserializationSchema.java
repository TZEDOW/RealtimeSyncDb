package com.zedow.flink.common;

import com.alibaba.fastjson.JSONObject;
import com.zedow.flink.utils.FieldNameUtil;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;

import java.time.LocalDate;
import java.util.List;

import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import org.apache.flink.table.data.TimestampData;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author: zhujiangtao
 * Date: 2022-09-08
 * Desc: 自定义序列器
 *  1. 解决时间时区，以及显示为NULL的问题;
 *  2. 简化binlog,构造JSONString类型的Source流;
 *  3. 对敏感信息（身份证+手机号）进行脱敏处理
 */

public class MyJsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private String serverTimeZone;
    private String maskKeywordList;
    private String unmaskingTable;

    public MyJsonDebeziumDeserializationSchema(String serverTimeZone,String maskKeywordList,String unmaskingTable) {
        this.serverTimeZone = serverTimeZone;
        this.maskKeywordList = maskKeywordList;
        this.unmaskingTable = unmaskingTable;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();

        // 解析binlog值
        Struct value = (Struct) sourceRecord.value();
        Struct source = value.getStruct("source");
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");

        // 从binlog source中获取mysql db&table等信息
        JSONObject sourceJson = new JSONObject();
        if (source != null) {

            Schema schema = source.schema();
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                sourceJson.put(field.name(),source.get(field));
            }
        }
        result.put("source",sourceJson);

        // 获取binlog中的操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op",operation.code());

        // 获取binlog before信息
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            // 获取列信息
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                beforeJson.put(field.name(),before.get(field));
            }
        }
        result.put("before",beforeJson);

        // 获取binlog after信息
        JSONObject afterJson = new JSONObject();
        if (after != null) {

            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();

            // 对after中的时间字段值、手机号、身份证号等进行自定义转换
            for (Field field : fieldList) {

                /*field样例展示:
                System.out.println("field.toString() >>> " + field.toString());
                System.out.println("field.name() >>> " + field.name());
                System.out.println("field.schema().type() >>> " + field.schema().type());
                System.out.println("field.schema().type().getName() >>> " + field.schema().type().getName());
                System.out.println("field.schema().name() >>> " + field.schema().name());*/

                /*time样例展示:
                "datetime3_c": 1595008822123
                "date_c": 18460
                "timestamp_c": "2020-07-17T18:00:22Z"
                "time_c": 64822000000
                "datetime6_c": 1595008822123456*/

                if (after.get(field) == null)
                {
                    afterJson.put(field.name(), after.get(field));
                } else if ("int64".equals(field.schema().type().getName()) && "io.debezium.time.Timestamp".equals(field.schema().name())) // "field": "datetime3_c"
                {
                    Object convert = convertToTimestamp(ZoneId.of(serverTimeZone)).convert(after.get(field), field.schema());
                    afterJson.put(field.name(), convert.toString());
                } else if ("int32".equals(field.schema().type().getName()) && "io.debezium.time.Date".equals(field.schema().name())) // "field": "date_c"
                {
                    Object convert = convertToDate().convert(after.get(field), field.schema());
                    /*Object convert = convertToTime().convert(after.get(field), field.schema()); //同样适用*/
                    afterJson.put(field.name(), convert.toString());
                } else if ("string".equals(field.schema().type().getName()) && "io.debezium.time.ZonedTimestamp".equals(field.schema().name())) // "default": "1970-01-01T00:00:00Z", "field": "timestamp_c"
                {
                    Object convert = convertToLocalTimeZoneTimestamp(ZoneId.of(serverTimeZone)).convert(after.get(field), field.schema());
                    afterJson.put(field.name(), convert.toString());
                } else if ("int64".equals(field.schema().type().getName()) && "io.debezium.time.MicroTime".equals(field.schema().name())) // "field": "time_c"
                {
                    Object convert = convertToTime().convert(after.get(field), field.schema());
                    System.out.println("convertToTime" + convert.toString());

                } else if ("int64".equals(field.schema().type().getName()) && "io.debezium.time.MicroTimestamp".equals(field.schema().name())) // "field": "datetime6_c"
                {
                    Object convert = convertToTimestamp(ZoneId.of(serverTimeZone)).convert(after.get(field), field.schema());
                    afterJson.put(field.name(), convert.toString());
                // 加密手机号和身份证信息
                } else {
                    assert source != null;
                    if (unmaskingTable.contains(source.get("table").toString())
                        && "string".equals(field.schema().type().getName())
                        && FieldNameUtil.check(field.name(),maskKeywordList)
                    ) {
                        // 这些表不对手机号、电话号脱敏，只对身份证号脱敏
                        Object convert = getMaskingStr(2).convert(after.get(field), field.schema());
                        afterJson.put(field.name(), convert.toString());
                    } else if ("string".equals(field.schema().type().getName())
                                && FieldNameUtil.check(field.name(),maskKeywordList))
                    {
                        Object convert = getMaskingStr(1).convert(after.get(field), field.schema());
                        afterJson.put(field.name(), convert.toString());
                    } else {
                        afterJson.put(field.name(), after.get(field));
                    }
                }
            }
        }
        result.put("after",afterJson);

        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }


    //自定义时间字段转换方法

    // Flink将Date(2022-09-08)转换成了EpochDay,在这里进行还原,使Sink端正常展示
    private static DeserializationRuntimeConverter convertToDate() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {

                return LocalDate.ofEpochDay((Long.valueOf((Integer)dbzObj)));
            }
        };
    }

    // Flink将Date(2022-09-08)转换成了EpochDay,我在这里进行还原,使Sink端正常展示
    private static DeserializationRuntimeConverter convertToTime() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case MicroTime.SCHEMA_NAME:
                            return LocalDate.ofEpochDay((long) dbzObj / 1000);
                        case NanoTime.SCHEMA_NAME:
                            return LocalDate.ofEpochDay((long) dbzObj / 1000_000);
                    }
                } else if (dbzObj instanceof Integer) {
                    return LocalDate.ofEpochDay(Long.valueOf((Integer)dbzObj));
                }
                throw new IllegalArgumentException(
                        "Unable to convert to Time from unexpected value '"
                                + dbzObj
                                + "' of type "
                                + dbzObj.getClass().getName());
            }
        };
    }

    // Flink将Datetime(2022-09-08 17:33:33)转换成了Long,我在这里进行还原,使Sink端正常展示
    private static DeserializationRuntimeConverter convertToTimestamp(ZoneId serverTimeZone) {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case Timestamp.SCHEMA_NAME:
                            return TimestampData.fromEpochMillis((Long) dbzObj);
                        case MicroTimestamp.SCHEMA_NAME:
                            long micro = (long) dbzObj;
                            return TimestampData.fromEpochMillis(
                                    micro / 1000, (int) (micro % 1000 * 1000));
                        case NanoTimestamp.SCHEMA_NAME:
                            long nano = (long) dbzObj;
                            return TimestampData.fromEpochMillis(
                                    nano / 1000_000, (int) (nano % 1000_000));
                    }
                }
                LocalDateTime localDateTime =
                        TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
                return TimestampData.fromLocalDateTime(localDateTime);
            }
        };
    }

    // Flink将Timestamp(2022-09-08 17:33:33)转换成了UTC,我在这里进行还原为中国时区,使Sink端正常展示
    private static DeserializationRuntimeConverter convertToLocalTimeZoneTimestamp(
            ZoneId serverTimeZone) {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof String) {
                    String str = (String) dbzObj;
                    Instant instant = Instant.parse(str);
                    return TimestampData.fromLocalDateTime(
                            LocalDateTime.ofInstant(instant, serverTimeZone));
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + dbzObj
                                + "' of type "
                                + dbzObj.getClass().getName());
            }
        };
    }

    // 对敏感信息（手机号电话号身份证）进行脱敏处理
    private static DeserializationRuntimeConverter getMaskingStr(Integer mode) {
        return new DeserializationRuntimeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                String str = (String) dbzObj;
                String unmaskingStr1 = str.replace(" ", "").replace("-", "");
                String unmaskingStr2 = str.trim();

                // 身份证
                String regExp1 = "^[1-9]\\d{5}(18|19|20|(3\\d))\\d{2}((0[1-9])|(1[0-2]))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$";
                Pattern p1 = Pattern.compile(regExp1);
                Matcher m1 = p1.matcher(unmaskingStr1);

                // 手机号
                String regExp2 = "^((\\+86)|(86))?[1][3456789][0-9]{9}$";
                Pattern p2 = Pattern.compile(regExp2);
                Matcher m2 = p2.matcher(unmaskingStr1);

                // 电话号
                String regExp4 = "^0\\d{2,3}[-]?\\d{7,8}|0\\d{2,3}\\s?\\d{7,8}$";
                Pattern p4 = Pattern.compile(regExp4);
                Matcher m4 = p4.matcher(unmaskingStr2);

                if (mode == 2) { // 如果脱敏模式为2，只对身份证脱敏
                    if (m1.matches()) {
                        return unmaskingStr1.replaceAll("(\\d{12})\\w{6}", "$1******");
                    } else return str;
                }
                // 其他脱敏模式下，对手机号电话号身份证三者脱敏
                if (m1.matches()) {
                    return unmaskingStr1.replaceAll("(\\d{12})\\w{6}", "$1******");
                } else if (m2.matches()) {
                    String regExp3 = "^((\\+?86)?|(86)?)";
                    Pattern p3 = Pattern.compile(regExp3);
                    Matcher m3 = p3.matcher(unmaskingStr1);
                    StringBuffer sb = new StringBuffer();
                    while (m3.find()) {
                        m3.appendReplacement(sb, "");
                    }
                    m3.appendTail(sb);
                    return sb.toString().replaceAll("(\\d{3})\\d{4}(\\d{4})", "$1****$2");
                } else if (m4.matches()) {
                    return unmaskingStr2.replace(unmaskingStr2.substring(5,8),"****");
                } else return str;
            }
        };
    }
}
