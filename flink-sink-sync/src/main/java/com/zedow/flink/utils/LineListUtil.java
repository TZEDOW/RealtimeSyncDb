package com.zedow.flink.utils;

import lombok.Data;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Author: zhujiangtao
 * Date: 2022-08-15
 * Desc: String转换成lineList工具类
 *      用于解析Mysql create sql
 */
@Data
public class LineListUtil {
    // line index
    private Integer index;
    private String code;

    public List<LineListUtil> getLineList(String s) {
        if (Objects.isNull(s)) {
            return Collections.emptyList();
        }
        List<LineListUtil> list = new ArrayList<>();
        try (InputStreamReader inputStreamReader = new InputStreamReader(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
             BufferedReader reader = new BufferedReader(inputStreamReader)
        ) {
            int index = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                index++;
                LineListUtil llu = new LineListUtil();
                llu.setCode(line);
                llu.setIndex(index);
                list.add(llu);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }
}
