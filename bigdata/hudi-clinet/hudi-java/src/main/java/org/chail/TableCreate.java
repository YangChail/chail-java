package org.chail;

public class TableCreate {


    public static void main(String[] args) {
        HiveJdbc jdbc = new HiveJdbc("192.168.239.223", "10000", "default", "hive", "hive");
        jdbc.create10Table();

    }
}