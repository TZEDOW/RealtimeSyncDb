package com.zedow.flink.common;

import com.zedow.flink.utils.DataXUtil;
import com.zedow.flink.utils.FieldNameUtil;
import com.zedow.flink.utils.LineListUtil;

/**
 * Author: zhujiangtao
 * Date: 2022-10-12
 * Desc: 自动获取DataX批同步的转化器json文件，可实现自动自定义脱敏同步
 *          脱敏模式为1时，脱敏手机电话身份证号
 *          脱敏模式为2时，只脱敏身份证号
 */
public class DataxJob {

    public static String getTransforJson(String mysqlDDL, String mysqlTable,String maskKeywordList,String unmaskingTable) {
        String transformerJson;
        String str = mysqlDDL.split("PRIMARY KEY \\(")[0];
        String check = FieldNameUtil.checkResult(str, maskKeywordList);

        if (check.equals("")
        ) {
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
            String[] index = columnIndex.toString().split(",");

            if (unmaskingTable.contains(mysqlTable)) {
                transformerJson = DataXUtil.createTransformer("dx_masking",index,2);
            } else {
                transformerJson = DataXUtil.createTransformer("dx_masking",index,1);
            }
        }
        return transformerJson;
    }
}
