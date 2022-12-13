package chail.test;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import chail.datasupport.tools.CloseUtils;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * @author : yangc
 * @date :2022/7/7 18:35
 * @description :
 * @modyified By:
 */
public class OracleTests {

    private static int num = 100;
    public static DruidDataSource dataSource;

    public static Map<String, List<Long>> TABLE_ID_MAP = new HashMap<>();
    public static Random random = new Random();

    public static void getConnection() throws ClassNotFoundException, SQLException {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        dataSource.setUrl("jdbc:oracle:thin:@//192.168.52.173:1521/ora11g");
        dataSource.setUsername("chail");
        dataSource.setPassword("chail");
        dataSource.setInitialSize(2);
        dataSource.setMaxActive(5);
        dataSource.setMinIdle(1);
        dataSource.setMaxWait(60000);
        dataSource.setValidationQuery("select * from dual");
        dataSource.setTestOnBorrow(true);
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnReturn(false);
        dataSource.setPoolPreparedStatements(true);
        dataSource.setTimeBetweenEvictionRunsMillis(6000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setMaxPoolPreparedStatementPerConnectionSize(20);

        Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@//192.168.52.173:1521/ora11g","chail","chail");
        connection.setNetworkTimeout(new Executor() {
            @Override
            public void execute(Runnable command) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }, 600000);
        connection.setAutoCommit(true);
        connection.close();


    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
        getConnection();
        insertDate();
        //dropTable();
        //createTable();
        //supplementalLog();
        //testData();
    }

    private static void testData(){
        while (true) {
            try {
                int i = random.nextInt(3);
                if (i == 0) {
                    insert();
                } else if (i == 1) {
                    update();
                } else if (i == 2) {
                    delete();
                }
                Thread.sleep(10);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }


    private static void supplementalLog() throws SQLException {
        String createSql="alter table CHAIL.TEST_INFO_%s add supplemental log data(all) columns";
        for (int i = 1; i <= 100; i++) {
            String sql = String.format(createSql, i);
            DruidPooledConnection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            System.out.println(sql);
            statement.execute(sql);
            CloseUtils.close(statement, connection);
        }

    }

    private static String getTableName() {
        int i = random.nextInt(99) + 1;
        return "test_info_" + i;
    }

    private static void exec(String sql) throws SQLException {
        System.out.println(sql);
        DruidPooledConnection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute(sql);
        CloseUtils.close(statement, connection);

    }



    private static void insertDate() throws SQLException, InterruptedException {
        String createSql = "insert into chail.TEST_TIME4 values(?,?,?,?)";
        DruidPooledConnection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(createSql);
        for (int i=80000;i<150000;i++){
            statement.setString(1,i+"");
            statement.setString(2,UUID.randomUUID().toString());
            statement.setDate(4,new Date(System.currentTimeMillis()));
            statement.setTimestamp(3,new Timestamp(System.currentTimeMillis()+new Random().nextInt(50000)));
            statement.addBatch();
        }
        System.out.println(statement.executeBatch().length);

        CloseUtils.close(statement,connection);
    }


    private static void insert() throws SQLException, InterruptedException {
        String tableName = getTableName();
        String createSql = "insert into chail.%s values(%s,'%s')";
        long id = System.currentTimeMillis();
        List<Long> ids = TABLE_ID_MAP.get(tableName);
        if (ids == null) {
            ids = new ArrayList<>();
            TABLE_ID_MAP.put(tableName,ids);
        }
        ids.add(id);
        exec( String.format(createSql, tableName, id, UUID.randomUUID()));

    }


    private static void update() throws SQLException, InterruptedException {
        String createSql = "update chail.%s set str='%s' where id=%s";
        Set<String> set = TABLE_ID_MAP.keySet();
        if(set.size()>0){
            List<String> tbs=new ArrayList<>(set);
            String tableName = tbs.get(0);
            List<Long> ids = TABLE_ID_MAP.get(tableName);
            if(ids.size()>0){
                exec(String.format(createSql, tableName,  UUID.randomUUID(), ids.get(random.nextInt(ids.size()))));
            }

        }
    }


    private static void delete() throws SQLException, InterruptedException {
        String createSql = "delete chail.%s  where id=%s";
        Set<String> set = TABLE_ID_MAP.keySet();
        if(set.size()>0){
            List<String> tbs=new ArrayList<>(set);
            String tableName = tbs.get(0);
            List<Long> ids = TABLE_ID_MAP.get(tableName);
            if(ids.size()>0){
                Long id = ids.get(random.nextInt(ids.size()));
                exec(String.format(createSql, tableName,id));
                ids.remove(id);
            }
        }
    }


    private static void createTable() throws SQLException {
        String createSql = "create table chail.test_info_%s (id number  primary key not null,str varchar2(40))";
        for (int i = 1; i <= 100; i++) {
            String sql = String.format(createSql, i);
            DruidPooledConnection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            //sql=changTtable(sql);
            System.out.println(sql);
            statement.execute(sql);
            CloseUtils.close(statement, connection);
        }
    }


    private static String changTtable(String sql){
        String add = ", \n" +
                "\"ods_insert_time\" DATE DEFAULT sysdate, \n" +
                "\"ods_update_time\" TIMESTAMP (6), \n" +
                "\"ods_delete_flag\" CHAR(1), \n" +
                "\"source_change_time\" TIMESTAMP (6), \n" +
                "\"ods_oper_type\" CHAR(1))";
        sql = sql.replace(")", add);
        return sql;

    }



    private static void dropTable() throws SQLException {
        String createSql = "drop table chail.test_info_%s";
        for (int i = 1; i <= 100; i++) {
            String sql = String.format(createSql, i);
            DruidPooledConnection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            try {
                System.out.println(sql);
                statement.execute(sql);
            } catch (Exception e) {

            }
            CloseUtils.close(statement, connection);
        }


    }


}
