package chail;

import com.mchz.mcdatasource.DbCoreInit;
import com.mchz.mcdatasource.model.db.DatasourceDatabase;
import com.mchz.mcdatasource.model.db.DatasourceDatabaseMeta;
import com.mchz.mcdatasource.model.db.exception.DatabaseException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author : yangc
 * @date :2022/6/23 18:04
 * @description :
 * @modyified By:
 */
public class TestSqlJdbc {


    public static void main(String[] args) throws SQLException, ParseException, DatabaseException {
//        String property = System.getProperty("user.dir");
//        String ss = property + File.separator + "mcdatasource";
//        System.setProperty("MCDATASOURCE_HOME",ss);
//        DbCoreInit.init();
//        testSql();
        String date="2022-06-07 12:00:36.067";
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date parse = simpleDateFormat.parse(date);

        System.out.println();
    }

   public static void testSql() throws ParseException, DatabaseException, SQLException {
       String finalType="Oracle";
       String host="wz.chail.top";
       String schema="wzwjorcl";
       String port="61046";
       String user="DW_WZWJW";
       String pass="DW_WZWJW";
       String sql="SELECT * FROM (SELECT  '47072673833032411A1001' as YILIAOJGDM,  T0.\"JLXH\" as GUOMINJLID,  'APP0022' as SENDSYSTEMID,  '永嘉人民医院' as YILIAOJGMC,  NULL as MPI,  T0.\"MZHM\" as BINGRENID,  T0.\"BRXM\" as XINGMING,  '1' as JIUZHENYWLX,  NVL((select TO_CHAR(T1.\"JZXH\")  from YJRMYYHIS.\"MS_CF01\" T1  where T1.\"CFSB\"=T0.\"CFSB\" AND ROWNUM =1 ) ,'99#') as JIUZHENYWID,  T0.\"CFSB\" as LAIYUANID,  NULL as JIAGEID,  T0.\"YPXH\" as YAOPINID,  (select T2.\"YPMC\"  from YJRMYYHIS.\"YK_TYPK\" T2  where T2.\"YPXH\"=T0.\"YPXH\" AND ROWNUM =1 )  as YAOWUMC,  '1' as GUOMINYDM,  '药物' as GUOMINYMC,  NULL as GUOMINZZ,  1 as YAOWUBZ,  T0.\"PSJG\" as PISHIJGDM,  DECODE(T0.\"PSJG\",1,'阳性',-1,'阴性') as PISHIJGMC,  NULL as CHULIYJDM,  NULL as CHULIYJMC,  NULL as YANZHONGCDDM,  NULL as YANZHONGCDMC,  T0.\"PSSJ\" as FASHENGRQ,  2 as JILULY,  T0.\"YSDM\" as CHUANGJIANRID,  NVL((select T3.\"YGXM\"  from YJRMYYHIS.\"GY_YGDM\" T3  where T3.\"YGDM\"=T0.\"YSDM\" AND ROWNUM =1 ) ,'99#') as CHUANGJIANRXM,  T0.\"KFRQ\" as CHUANGJIANRQ,  T0.\"YSDM\" as XIUGAIRID,  NVL((select T3.\"YGXM\"  from YJRMYYHIS.\"GY_YGDM\" T3  where T3.\"YGDM\"=T0.\"YSDM\" AND ROWNUM =1 ) ,'99#') as XIUGAIRXM,  T0.\"PSSJ\" as XIUGAIRQ,  NULL as BEIZHU,  T0.\"ZFBZ\" as ZUOFEIBZ,  '1' as BINGANSYXSBZ,  T0.\"ods_update_time\" as MC_INC_COL  FROM YJRMYYHIS.\"PS_PSJL\" T0  WHERE (T0.\"ods_delete_flag\"='0' AND T0.\"PSBZ\"=2 AND T0.\"PSSJ\">TO_DATE('2020-01-01', 'yyyy-mm-dd'))) where \"MC_INC_COL\" <=  ?  or \"MC_INC_COL\" is null ";
       DatasourceDatabaseMeta  datasourceDatabaseMeta = new DatasourceDatabaseMeta(finalType, host, schema, port, user, pass);
       DatasourceDatabase datasourceDatabase = new DatasourceDatabase(datasourceDatabaseMeta);
       datasourceDatabase.connect();
       //ValueMeta valueMeta=new ValueMetaDate();
       //subRowMeta.addValueMeta(valueMeta);
       String date="2022-06-07 12:00:36.67";
       SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
       Date parse = simpleDateFormat.parse(date);
       java.sql.Timestamp sdate = new java.sql.Timestamp( parse.getTime() );
       PreparedStatement preparedStatement = datasourceDatabase.getConnection().prepareStatement(sql);
       preparedStatement.setTimestamp( 1, sdate );
       ResultSet resultSet1 = preparedStatement.executeQuery();
       while (resultSet1.next()){

           ResultSetMetaData metaData = resultSet1.getMetaData();

           Object mc_inc_col = resultSet1.getObject("MC_INC_COL");

           Object object = resultSet1.getObject(1);
           System.out.println(object);

       }
       System.out.println();


   }



}
