package com.chail.js;

import java.io.*;
import java.util.*;

/**
 * @author : yangc
 * @date :2023/4/23 17:29
 * @description :
 * @modyified By:
 */
public class Test {

    public static void main(String[] args) throws FileNotFoundException {
        String filePath = "D://aa.txt";
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            List<String> aa=new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                aa.add(line );
            }
            aa.sort(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return Integer.parseInt(o1.substring(0,o1.indexOf("-")))-Integer.parseInt(o2.substring(0,o2.indexOf("-")));
                }
            });

            for (String s : aa) {
                String format = String.format("file '%s'", s);
                System.out.println(format);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static void main1(String[] args) {

        String filePath = "D://aa.txt";

        // 关键字列表
        String[] keywords = {"第一章", "第二章", "第三章", "第四章", "第五章", "第六章", "第七章", "第八章", "第九章", "第十章", "第十一章", "第十二章", "第十三章", "第十四章", "第十五章", "第十六章", "第十七章", "第十八章", "第十九章"};

        // 分组统计数据
        Map<String, List<String>> groupCount = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 查找关键字
                for (String keyword : keywords) {
                    if (line.contains(keyword)) {
                        List<String> orDefault = groupCount.getOrDefault(keyword, new ArrayList<>());
                        orDefault.add(line);
                        groupCount.put(keyword,orDefault);
                    }
                }
            }
            Map<String, String> res = new HashMap<>();
            // 打印分组统计结果
            // ffmpeg.exe -f concat -safe 0 -i videolist.txt -c copy -y out.mp4
            for (String keyword : keywords) {
                System.out.println(keyword + ": " + groupCount.getOrDefault(keyword, new ArrayList<>()).size());
                List<String> orDefault = groupCount.getOrDefault(keyword, new ArrayList<>());
                if(orDefault.isEmpty()){
                    continue;
                }
                orDefault.sort(Comparator.naturalOrder());
                List <String> tem=new ArrayList<>();
                StringBuffer sfb=new StringBuffer();
                for (int i = 0; i < orDefault.size(); i++) {
                    String format = String.format("file '%s'", orDefault.get(i));
                    sfb.append(format);
                    sfb.append("\n");
                }
                res.putIfAbsent(keyword,sfb.toString());
                System.out.println(String.format("ffmpeg.exe -f concat -safe 0 -i %s.txt -c copy -y %s.mp4",keyword,keyword));
            }

            wei(res);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void wei( Map<String, String> res){

        // 输出文件夹路径
        String folderPath = "D://output";

        // 遍历分组数据，将每个键值对写入对应的文件中
        for (Map.Entry<String, String> entry : res.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // 输出文件路径
            String outputPath = folderPath + "//" + key + ".txt";

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
                writer.write(value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
