package com.zedow.flink.utils;

/**
 * Author: zhujiangtao
 * Date: 2022-10-08
 * Desc: 列名与关键字核对的工具类
 */
public class FieldNameUtil {

    public static boolean check(String fieldName,String paras) {
        String[] para = paras.split(",");
        int[] tags = new int[para.length];
        for (int i = 0; i < para.length; i++) {
            if (FieldNameUtil.containsIgnoreCase(fieldName,para[i])){
                tags[i] = 1;
            } else {
                tags[i] = 0;
            }
        }

        int result = 0;
        for (int t:tags) {
            result = result + t;
        }
        return result > 0;
    }

    public static String checkResult (String str, String paras) {
        StringBuilder keyWord = new StringBuilder();
        String[] para = paras.split(",");

        for (String s : para) {
            if (containsIgnoreCase(str, s)) {
                keyWord.append(s).append("/");
            }
        }
        return keyWord.toString();
    }

    public static boolean containsIgnoreCase(String str, String searchStr)     {
        if(str == null || searchStr == null) return false;
        final int length = searchStr.length();
        if (length == 0)
            return true;
        for (int i = str.length() - length; i >= 0; i--) {
            if (str.regionMatches(true, i,searchStr,0,length))
                return true;
        }
        return false;
    }

/*    public static void main(String[] args) {
        String mysqlDDL = "CREATE TABLE `orders_test` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',\n" +
                "  `order_id` int(11) NOT NULL DEFAULT '0' COMMENT '订单Id',\n" +
                "  `occur_date` date DEFAULT NULL COMMENT '发生日期',\n" +
                "  `payed_date` datetime DEFAULT NULL COMMENT '支付时间',\n" +
                "  `actly_payed` decimal(18,2) DEFAULT NULL COMMENT '实际支付金额',\n" +
                "  `remark` varchar(255) DEFAULT NULL COMMENT '备注',\n" +
                "  `update_timestamp` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新时间戳',\n" +
                "  `mobile_number` varchar(100) DEFAULT NULL,\n" +
                "  `mobile_number1` varchar(100) DEFAULT NULL,\n" +
                "  `id_card` varchar(100) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`) USING BTREE\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=311349263 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='支付明细表'";

        String str = mysqlDDL.split("PRIMARY KEY \\(")[0];
        String transformerJson;
        String check = FieldNameUtil.checkResult(str, "mobile,phone,id_card,idCard,id-card,identity,tele,contact,call,shouji,dianhua,lianxi,haoma");

        if (check.equals("")) {
            transformerJson = "";
        } else {
            String[] keywords = check.split("/");
            for (String keyword : keywords) {
                str = str.replace(keyword, "19951222");
            }
            StringBuilder columnIndex = new StringBuilder();
            for (LineListUtil list :new LineListUtil().getLineList(str)) {
                if(list.getCode().contains("19951222")) {
                    columnIndex.append(list.getIndex() -2).append(",");
                }
            }
            String[] parameter = columnIndex.toString().split(",");
            transformerJson = DataXUtil.createTransformer("dx_masking", parameter, 2);
        }

        String json = DataXUtil.createHeader() + "                \"reader\": {\n" + "                }," + "\n" +
                "" + "                \"writer\": {\n"
        + "                }" + transformerJson + DataXUtil.createTail();
        System.out.println(json);
    }*/
}
