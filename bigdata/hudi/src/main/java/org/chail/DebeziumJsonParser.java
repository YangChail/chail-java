package org.chail;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class DebeziumJsonParser {

    public static String getOP(String message){

        JSONObject json_obj = JSON.parseObject(message);
        String op = json_obj.getJSONObject("payload").get("op").toString();
        return  op;
    }
}