package com.chail.datasupport.tools;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : yangc
 * @date :2022/6/10 14:46
 * @description :
 * @modyified By:
 */
public class DelData {

    private static String[] tables = new String[]{"DW_ETL_CONTROL",
            "GY_BINGRENGMJL",
            "GY_BINGRENTZXX",
            "GY_BINGRENWZXX",
            "GY_BINGRENXX",
            "GY_BINGRENZDZL",
            "GY_BINGRENZDZL_BAK",
            "GY_HUIZHENDAN",
            "JC_JIANCHABG",
            "JC_SHENQINGDAN",
            "JY_JIANYANBG",
            "JY_JIANYANBGMX",
            "JY_JIANYANSQD",
            "JY_XIJUNJG",
            "JY_YAOMINJG",
            "MZ_CHUFANGJL",
            "MZ_CHUFANGMX",
            "MZ_JIUZHENXX",
            "MZ_SHOUFEIXX",
            "MZ_ZHIFUXX",
            "SM_SHOUSHUXX",
            "TJ_HUANZHETJJGJY",
            "TJ_HUANZHETJJGXX",
            "TJ_HUANZHETJXMXX",
            "TJ_HUANZHETJXX",
            "Y_JIANYANBGMX",
            "ZY_BINGCHENGJL",
            "ZY_BINGRENXX",
            "ZY_CHUANGWEIBD",
            "ZY_CHUANGWEIPXX",
            "ZY_CHUYUANJL",
            "ZY_FEIYONGJL",
            "ZY_FEIYONGXX",
            "ZY_JIESUANJL",
            "ZY_RUYUANJL",
            "ZY_YIZHUJL",
            "ZY_YIZHUZXJL",
            "ZY_ZHIFUXX"};

    public static void main(String[] args) {
        index();
        //delData("47062117833030311A1001");
    }


    public static void index(){
        List<String> indexs=new ArrayList<>();
        for (String table : tables) {
            String sql="CREATE INDEX IDX_UPTIME_%s ON WZGT.%s (\"ods_insert_time\");";
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sql= String.format(sql,System.currentTimeMillis(),table);
            indexs.add(sql);
        }
        for (String index : indexs) {
            System.out.println(index);
        }

    }

    public static void delData(String code){
        List<String> indexs=new ArrayList<>();
        for (String table : tables) {
            String sql="DELETE FROM %s WHERE YILIAOJGDM='%s' ;";
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sql= String.format(sql,table,code);
            indexs.add(sql);
        }
        for (String index : indexs) {
            System.out.println(index);
        }

    }



}
