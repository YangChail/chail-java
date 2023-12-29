package org.chail;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {

    private static final String DB_URL0 = "jdbc:oracle:thin:@//10.89.2.41:1521/CZSB";
    private static final String DB_USER0 = "mc_qy";
    private static final String DB_PASSWORD0 = "Mc_qy2023";


    private static final String DB_URL = "jdbc:oracle:thin:@//10.89.2.179:1521/DWHSDB";
    private static final String DB_USER = "mc_qy";
    private static final String DB_PASSWORD = "Mc_qy2023";



    private static final String DB_URL2 = "jdbc:oracle:thin:@//10.88.2.159:1521/ZHYS";
    private static final String DB_USER2 = "mc_qy";
    private static final String DB_PASSWORD2 = "Mc_qy2023";


    public static void main(String[] args) {
        String fileName = "/schema2.txt";
        List<String> SCHEMAS = readLine(fileName);
        System.out.println("获取到shema数量"+SCHEMAS.size());
        SCHEMAS = SCHEMAS.stream().distinct().collect(Collectors.toList());
        AtomicInteger size = new AtomicInteger(SCHEMAS.size());
        for (String schema : SCHEMAS) {
            executorService.submit(() -> {
                //dropTablesInSchema(schema);
                count(schema);
                size.getAndDecrement();
            });
        }

        try {
            executorService.shutdown();
            boolean loop;
            do {
                loop = !executorService.awaitTermination(10, TimeUnit.SECONDS);
                System.out.println((String.format("----------------------耗时%sS-剩余%s......",(System.currentTimeMillis()-startTime)/1000,size)));
            } while (loop);
            executorService.shutdownNow();
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
            Thread.currentThread().interrupt();
        }


    }



    public  static List<String> readLine(String fileName) {
        List<String> lines = new ArrayList<>();
        try {
            // Read the file from the resources folder
            InputStream inputStream = DropTablesInSchemas.class.getResourceAsStream(fileName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            // Read each line and store it in the list
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            // Close the reader
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return lines;
    }

}
