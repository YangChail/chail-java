package org.chail;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.chail.orc.OrcField;
import org.chail.orc.input.OrcInputMeta;
import org.chail.orc.output.MyOrcOutputFormat;
import org.chail.orc.output.OrcOutputMeta;
import org.chail.orc.utils.HadoopConstant;
import org.chail.orc.utils.OrcUtils;
import org.chail.orc.utils.krb.KrbConstant;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.RowMetaAndData;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class OrcTest extends BaseTest {
    /**
     *
     * ----ddl
     * create table user_info_torc (id INT,name STRING,idcard VARCHAR(20),phone  STRING,salary  DECIMAL(20,6))
     * CLUSTERED BY (id) INTO 5 BUCKETS STORED AS ORC TBLPROPERTIES ("transactional"="true");
     *
     *
     */

	@Before
	public void before() {
		System.setProperty("KETTLE_PLUGIN_CLASSES=", "com.mchz.bigdata.plugins.hdfs.orc.input.OrcInputMeta");
		System.setProperty("KETTLE_PLUGIN_CLASSES=", "com.mchz.bigdata.plugins.hdfs.orc.output.OrcOutputMeta");
		init();
	}

	@Test
	public void testOrcIn() throws Exception {
		OrcInputMeta inputMeta = new OrcInputMeta();
		String path = "hdfs://192.168.239.1:8020";
		//path = path + "/user/hive/warehouse/autotest_in.db/data1w_orc/000000_0";
		// path = path+
		path = path + "/tmp/chail/000001.orc";
		//path = path + "/user/hive/warehouse/autotest_in.db/ods_orc/stat_date=20121224/15852963655591";
		// "/user/hive/warehouse/autotest_in.db/data1w_par/74d4a145c1b5e8c-d321a8dd00000001_2125671509_data.0.parq";
		inputMeta.setFilePath(path);
		List<Object[]> preview = preview(inputMeta, name, maxCount);
		for(int i=0;i<10;i++ ) {
			Object[] objects = preview.get(i);
		}
	}
    @org.junit.Test
    public void testOrcInKerberos() throws Exception {
        String user = "chail@TDH";
        String keytab = "D:/a.keytab";
        String dir = "hdfs://192.168.51.199:8020/inceptor1";
        addPro(dir);
        OrcInputMeta inputMeta = new OrcInputMeta();
        String path = "hdfs://192.168.51.199:8020/";
        path = path + "/inceptor1/user/hive/warehouse/chail.db/hive/orc_tabl_all/000000_0";
        inputMeta.setFilePath(path);
        inputMeta.setTDHOrc(true);
        inputMeta.setSchemaList( createOrcOut());

        List<Object[]> preview = preview(inputMeta, name, maxCount);
        for(int i=0;i<preview.size();i++ ) {
            Object[] objects = preview.get(i);
        }
    }


    @Test
    public void testOrcOut() throws Exception {
        OrcOutputMeta meta = new OrcOutputMeta();
        String path = "hdfs://192.168.239.1:8020";
        path = path + "/tmp/chail/000001.orc";
        Configuration conf = new Configuration();
        meta.setFilePath(path);
        meta.setSchemaList(createOrcOut());
        List<Object[]> preview = previewOut(meta, name, maxCount);
    }



    @Test
    public void testOrcInTDHKerberos() throws Exception {
        String  hdfsSchema="hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String dir2 = "/inceptor1/user/hive/warehouse/chail.db/chail/user_info_torc";
        //String dir2 = "/inceptor1/user/hive/warehouse/chail.db/chail/user_info_bucket_orc/000001_0";
        String pa=hdfsSchema+dir2;
        OrcUtils orcUtils=new OrcUtils(pa);
        List<String> allFiles = orcUtils.getAllFiles(new Path(pa));
        for (String allFile : allFiles) {
            getOrc(allFile);
        }
    }

    /**
     * ?????? torc
     * @throws Exception
     */
    @Test
    public void testTOrc() throws Exception {
        String  hdfsSchema="hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String warehose="/inceptor1/user/hive/warehouse/";
        OrcUtils orcUtils=new OrcUtils(hdfsSchema);
        List<String> allFiles = orcUtils.getAllFiles(new Path(hdfsSchema+warehose+"chail.db/chail/user_info_torc"));
        for (String allFile : allFiles) {
            OrcInputMeta inputMeta = new OrcInputMeta();
            inputMeta.setFilePath(allFile);
            String out=allFile.replace("chail.db","ycc.db");
            OrcOutputMeta outputMeta = new OrcOutputMeta();
            outputMeta.setTDHOrc(true);
            outputMeta.setSchemaList( createOrcOut());
            outputMeta.setFilePath(out);
            transRunning(generatePreviewTransformation(inputMeta, "In-" + 1, outputMeta, "Out-" + 1));
        }
    }


    @Test
    public void testTOrcRepete() throws Exception {
        String hdfsSchema = "hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String allfile = "hdfs://chail:chail@192.168.51.199:8020" +
            "/inceptor1/user/hive/warehouse/chail.db/chail/user_info_torc/delta_0018333_0018333/bucket_00002";
        OrcInputMeta inputMeta = new OrcInputMeta();
        inputMeta.setFilePath(allfile);
        inputMeta.setTDHOrc(true);
        inputMeta.setSchemaList( createOrcOut());
        String out = allfile.replace("chail.db", "ycc.db");
        OrcOutputMeta outputMeta = new OrcOutputMeta();
        outputMeta.setTDHOrc(true);
        outputMeta.setSchemaList(createOrcOut());
        outputMeta.setFilePath(out);
        outputMeta.setCompression(MyOrcOutputFormat.COMPRESSION.ZLIB.name());
        transRunning(generatePreviewTransformation(inputMeta, "In-" + 1, outputMeta, "Out-" + 1));
    }


    @Test
    public void testTOrcRepete1() throws Exception {
        String hdfsSchema = "hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String allfile = "hdfs://chail:chail@192.168.51.199:8020/inceptor1/user/hive/warehouse" +
            "/ycc.db/chail/user_info_torc/delta_0018333_0018333/bucket_00002";
        OrcInputMeta inputMeta = new OrcInputMeta();
        inputMeta.setFilePath(allfile);
        String out = allfile.replace("ycc.db", "ant.db");
        OrcOutputMeta outputMeta = new OrcOutputMeta();
        outputMeta.setTDHOrc(true);
        outputMeta.setSchemaList(createOrcOut());
        outputMeta.setFilePath(out);
        transRunning(generatePreviewTransformation(inputMeta, "In-" + 1, outputMeta, "Out-" + 1));
    }


    private void addPro(String hdfsSchema) throws URISyntaxException {
        String user = "chail@TDH";
        String keytab = "D:/a.keytab";
        System.setProperty("sun.security.krb5.debug", "false");
        Map<String, String> kerberosSubjectMap = new HashMap<String, String>();
        kerberosSubjectMap.put(HadoopConstant.HDFS_SITE_NAME, "D:/hadoop-conf/tdh/hdfs-site.xml");
        kerberosSubjectMap.put(HadoopConstant.CORE_SITE_FILE_NAME, "D:/hadoop-conf/tdh/core-site.xml");
        kerberosSubjectMap.put(HadoopConstant.KRB5_FILE_NAME,"D:/hadoop-conf/tdh/krb5.conf");
        kerberosSubjectMap.put(KrbConstant.KEY_DM_KERBEROS_PRINCIPAL, user);
        kerberosSubjectMap.put(KrbConstant.DM_HIVE_KERBEROS_ENABLE, "true");
        kerberosSubjectMap.put(KrbConstant.KEY_DM_KERBEROS_KEYTAB,"D:/hadoop-conf/tdh/chail.keytab");
        kerberosSubjectMap.put(KrbConstant.KEY_DM_KERBEROS_KRB5_CONF,"D:/hadoop-conf/tdh/krb5.conf");
        HadoopConstant.CONFIG_AND_SUBJECT_MAP.put(new URI(hdfsSchema).getAuthority(), kerberosSubjectMap);
    }


    private void getOrc(String  path) {
        OrcInputMeta inputMeta = new OrcInputMeta();
        inputMeta.setFilePath(path);
        List<Object[]> preview = preview(inputMeta, name, maxCount);
        for(int i=0;i<preview.size();i++ ) {
            Object[] objects = preview.get(i);
        }
    }


    /**
     * chail.user_info_torc.id int
     * chail.user_info_torc.name String
     * chail.user_info_torc.idcard varchar
     * chail.user_info_torc.phone string
     * chail.user_info_torc.salary decimal
     * @return
     */
    private List<OrcField> createOrcOut() {
		List<OrcField> asList =new ArrayList<OrcField>();
		asList.add( new OrcField("id","int"));
        asList.add( new OrcField("name","string"));
        asList.add( new OrcField("idcard","varchar"));
        asList.add( new OrcField("phone","string"));
        asList.add( new OrcField("salary","decimal"));
		return asList;
	}


    /**
     *
     * ????????????????????????
     * ????????????????????????????????????
     * ??????
     * create table user_info_torc (id INT,name STRING,idcard VARCHAR(20),phone  STRING,salary  DECIMAL(20,6))
     *      CLUSTERED BY (id) INTO 5 BUCKETS STORED AS ORC TBLPROPERTIES ("transactional"="true");
     * ??????
     * create table user_info_torc (id INT,idcard VARCHAR(20),phone  STRING,salary  DECIMAL(20,6)) CLUSTERED BY (id) INTO 5 BUCKETS STORED AS ORC TBLPROPERTIES ("transactional"="true");
     *
     *
     *
     * @throws Exception
     */
	@Test
	public void torcddldelcol() throws Exception {
        List<OrcField> asList =new ArrayList<OrcField>();
        asList.add( new OrcField("id","int"));
        asList.add( new OrcField("idcard","varchar"));
        asList.add( new OrcField("phone","string"));
        asList.add( new OrcField("salary","decimal"));

        String  hdfsSchema="hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String warehose="/inceptor1/user/hive/warehouse/";
        OrcUtils orcUtils=new OrcUtils(hdfsSchema);
        List<String> allFiles = orcUtils.getAllFiles(new Path(hdfsSchema+warehose+"chail.db/chail/user_info_torc"));
        for (String allFile : allFiles) {
            OrcInputMeta inputMeta = new OrcInputMeta();
            inputMeta.setFilePath(allFile);
            inputMeta.setTDHOrc(true);
            inputMeta.setSchemaList(asList);
            String out=allFile.replace("chail.db","ycc.db");
            OrcOutputMeta outputMeta = new OrcOutputMeta();
            outputMeta.setTDHOrc(true);
            outputMeta.setSchemaList( asList);
            outputMeta.setFilePath(out);
            transRunning(generatePreviewTransformation(inputMeta, "In-" + 1, outputMeta, "Out-" + 1));
        }
    }


    /**
     *
     *
     * create table ant.user_info_torc (id INT,name STRING,idcard VARCHAR(20),phone  STRING,salary  DECIMAL(20,6)) CLUSTERED BY (id) INTO 5 BUCKETS STORED AS ORC TBLPROPERTIES ("transactional"="true");
     *
     *
     * @throws Exception
     */
    @Test
    public void torcddladdcol() throws Exception {
        List<OrcField> asList =new ArrayList<OrcField>();
        asList.add( new OrcField("id","int"));
        asList.add( new OrcField("name","string"));
        asList.add( new OrcField("idcard","varchar"));
        asList.add( new OrcField("phone","string"));
        asList.add( new OrcField("salary","decimal"));

        String  hdfsSchema="hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String warehose="/inceptor1/user/hive/warehouse/";
        OrcUtils orcUtils=new OrcUtils(hdfsSchema);
        List<String> allFiles = orcUtils.getAllFiles(new Path(hdfsSchema+warehose+"ycc.db/chail/user_info_torc"));
        for (String allFile : allFiles) {
            OrcInputMeta inputMeta = new OrcInputMeta();
            inputMeta.setFilePath(allFile);
            inputMeta.setTDHOrc(true);
            inputMeta.setSchemaList(asList);
            String out=allFile.replace("ycc.db","ant.db");
            OrcOutputMeta outputMeta = new OrcOutputMeta();
            outputMeta.setTDHOrc(true);
            outputMeta.setSchemaList( asList);
            outputMeta.setFilePath(out);
            transRunning(generatePreviewTransformation(inputMeta, "In-" + 1, outputMeta, "Out-" + 1));
        }
    }


    /**
     create table user_info_orc (id INT,name STRING,idcard VARCHAR(20),phone  STRING,salary  DECIMAL(20,6)) stored as orc;

     drop table user_info_orc;

     create table user_info_orc (id INT,idcard VARCHAR(20),phone  STRING,salary  DECIMAL(20,6)) stored as orc;
     */
    @Test
    public void nomalOrcDelCol() throws Exception {
        List<OrcField> asList =new ArrayList<OrcField>();
        asList.add( new OrcField("id","int"));
        //asList.add( new OrcField("name","string"));
        asList.add( new OrcField("idcard","varchar"));
        asList.add( new OrcField("phone","string"));
        asList.add( new OrcField("salary","decimal"));
        String  hdfsSchema="hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String warehose="/inceptor1/user/hive/warehouse/";
        OrcUtils orcUtils=new OrcUtils(hdfsSchema);
        List<String> allFiles = orcUtils.getAllFiles(new Path(hdfsSchema+warehose+"chail.db/chail/user_info_orc"));
        for (String allFile : allFiles) {
            OrcInputMeta inputMeta = new OrcInputMeta();
            inputMeta.setFilePath(allFile);
            inputMeta.setTDHOrc(false);
            inputMeta.setSchemaList(asList);
            String out=allFile.replace("chail.db","ycc.db");
            OrcOutputMeta outputMeta = new OrcOutputMeta();
            outputMeta.setTDHOrc(false);
            outputMeta.setSchemaList( asList);
            outputMeta.setFilePath(out);
            transRunning(generatePreviewTransformation(inputMeta, "In-" + 1, outputMeta, "Out-" + 1));
        }
    }



    /**
     drop table user_info_orc;

     create table user_info_orc (id INT,idcard VARCHAR(20),phone  STRING,salary  DECIMAL(20,6)) stored as orc;

     create table user_info_orc (id INT,name STRING,idcard VARCHAR(20),phone  STRING,salary  DECIMAL(20,6)) stored as orc;


     */
    @Test
    public void nomalOrcAddCol() throws Exception {
        List<OrcField> asList =new ArrayList<OrcField>();
        asList.add( new OrcField("id","int"));
        asList.add( new OrcField("name","string"));
        asList.add( new OrcField("idcard","varchar"));
        asList.add( new OrcField("phone","string"));
        asList.add( new OrcField("salary","decimal"));
        String  hdfsSchema="hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String warehose="/inceptor1/user/hive/warehouse/";
        OrcUtils orcUtils=new OrcUtils(hdfsSchema);
        List<String> allFiles = orcUtils.getAllFiles(new Path(hdfsSchema+warehose+"ycc.db/chail/user_info_orc"));
        for (String allFile : allFiles) {
            OrcInputMeta inputMeta = new OrcInputMeta();
            inputMeta.setFilePath(allFile);
            inputMeta.setTDHOrc(false);
            inputMeta.setSchemaList(asList);
            String out=allFile.replace("ycc.db","ant.db");
            OrcOutputMeta outputMeta = new OrcOutputMeta();
            outputMeta.setTDHOrc(false);
            outputMeta.setSchemaList( asList);
            outputMeta.setFilePath(out);
            transRunning(generatePreviewTransformation(inputMeta, "In-" + 1, outputMeta, "Out-" + 1));
        }
    }


    /**
     * CREATE  TABLE tmp_gm2(
     * data_date string NOT NULL COMMENT '????????????',
     * etl_tx_time string NOT NULL COMMENT '??????????????????',
     * etl_job varchar(50) NOT NULL COMMENT '?????????',
     * etl_src_table varchar(50) NOT NULL COMMENT '?????????',
     * cust_id varchar(32) NOT NULL COMMENT '????????????',
     * cust_name varchar(50) DEFAULT NULL COMMENT '????????????',
     * en_name varchar(30) DEFAULT NULL COMMENT '?????????',
     * used_name varchar(50) DEFAULT NULL COMMENT '?????????',
     * cert_type varchar(4) DEFAULT NULL COMMENT '????????????',
     * cert_no varchar(50) DEFAULT NULL COMMENT '????????????',
     * cust_type varchar(4) DEFAULT NULL COMMENT '????????????',
     * sex varchar(2) DEFAULT NULL COMMENT '????????????',
     * nationnality varchar(4) DEFAULT NULL COMMENT '????????????',
     * country varchar(4) DEFAULT NULL COMMENT '????????????',
     * home_addr varchar(100) DEFAULT NULL COMMENT '????????????',
     * birth varchar(8) DEFAULT NULL COMMENT '????????????',
     * political varchar(4) DEFAULT NULL COMMENT '????????????',
     * educate_no varchar(4) DEFAULT NULL COMMENT '????????????',
     * degree varchar(4) DEFAULT NULL COMMENT '????????????',
     * graduate_school varchar(100) DEFAULT NULL COMMENT '????????????',
     * major varchar(50) DEFAULT NULL COMMENT '????????????',
     * marriage varchar(4) DEFAULT NULL COMMENT '????????????',
     * healthy varchar(4) DEFAULT NULL COMMENT '????????????',
     * social_guar varchar(30) DEFAULT NULL COMMENT '??????????????????',
     * local_resi_flag varchar(2) DEFAULT NULL COMMENT '??????????????????',
     * local_live_situ varchar(20) DEFAULT NULL COMMENT '??????????????????',
     * other_live varchar(30) DEFAULT NULL COMMENT '??????????????????',
     * local_duration string DEFAULT NULL COMMENT '?????????????????????',
     * about_relation varchar(4) DEFAULT NULL COMMENT '?????????????????????',
     * in_bank_duty varchar(30) DEFAULT NULL COMMENT '???????????????',
     * hold_stock_amt decimal(20,4) DEFAULT NULL COMMENT '????????????????????????',
     * vip_lev_cd varchar(4) DEFAULT NULL COMMENT 'VIP????????????',
     * about_cor_relation varchar(4) DEFAULT NULL COMMENT '?????????????????????',
     * create_loan_date varchar(8) DEFAULT NULL COMMENT '????????????????????????',
     * open_acct varchar(30) DEFAULT NULL COMMENT '???????????????????????????',
     * become_cust varchar(2) DEFAULT NULL COMMENT '????????????????????????',
     * per_character varchar(50) DEFAULT NULL COMMENT '????????????',
     * bank_cogo_situ varchar(2) DEFAULT NULL COMMENT '??????????????????',
     * gain_interest decimal(20,4) DEFAULT NULL COMMENT '??????????????????????????????'
     * )
     * COMMENT '????????????????????????'
     * CLUSTERED BY (
     * cust_id)
     * INTO 61 BUCKETS
     * ROW FORMAT SERDE
     * 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     * WITH SERDEPROPERTIES (
     * 'serialization.format'='1')
     * STORED AS INPUTFORMAT
     * 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     * OUTPUTFORMAT
     * 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     * TBLPROPERTIES (
     * 'numFiles'='61',
     * 'transactional'='true',
     * 'transient_lastDdlTime'='1602493297',
     * 'COLUMN_STATS_ACCURATE'='true',
     * 'totalSize'='117762872')
     * @throws Exception
     */
    @Test
    public void testT4_7Orc1() throws Exception {
        List<OrcField> asList =getCol();
        String hdfsSchema = "hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String allfile = hdfsSchema +
            "/inceptor1/user/hive/warehouse/chail.db/chail/tmp_gm2/delta_1298700_1298700/bucket_00051";
        OrcInputMeta inputMeta = new OrcInputMeta();
        inputMeta.setFilePath(allfile);
        inputMeta.setTDHOrc(true);
        inputMeta.setSchemaList( asList);
        String out = allfile.replace("chail.db", "ycc.db");
        OrcOutputMeta outputMeta = new OrcOutputMeta();
        outputMeta.setTDHOrc(true);
        for(int i=0;i<asList.size();i++){
            OrcField orcField = asList.get(i);
            orcField.setAliasName("_col"+i);
        }
        outputMeta.setSchemaList(asList);
        outputMeta.setFilePath(out);
        outputMeta.setCompression(MyOrcOutputFormat.COMPRESSION.ZLIB.name());
        transRunning(generatePreviewTransformation(inputMeta, "In-" + 1, outputMeta, "Out-" + 1));
    }


    @Test
    public void testT4_7Orc2() throws Exception {
        List<OrcField> asList =getCol();
        String hdfsSchema = "hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String allfile = hdfsSchema +
            "/inceptor1/user/hive/warehouse/ycc.db/chail/tmp_gm2/delta_1298700_1298700/bucket_00051";
        OrcInputMeta inputMeta = new OrcInputMeta();
        inputMeta.setFilePath(allfile);
        inputMeta.setTDHOrc(true);
        inputMeta.setSchemaList( asList);
        String out = allfile.replace("ycc.db", "ant.db");
        OrcOutputMeta outputMeta = new OrcOutputMeta();
        outputMeta.setTDHOrc(true);
        outputMeta.setSchemaList(asList);
        outputMeta.setFilePath(out);
        outputMeta.setCompression(MyOrcOutputFormat.COMPRESSION.ZLIB.name());
        transRunning(generatePreviewTransformation(inputMeta, "In-" + 1, outputMeta, "Out-" + 1));
    }



    private  List<OrcField> getCol(){
        String ddl="data_date string NOT NULL COMMENT '????????????',\n" +
            "etl_tx_time string NOT NULL COMMENT '??????????????????',\n" +
            "etl_job varchar(50) NOT NULL COMMENT '?????????',\n" +
            "etl_src_table varchar(50) NOT NULL COMMENT '?????????',\n" +
            "cust_id varchar(32) NOT NULL COMMENT '????????????',\n" +
            "cust_name varchar(50) DEFAULT NULL COMMENT '????????????',\n" +
            "en_name varchar(30) DEFAULT NULL COMMENT '?????????',\n" +
            "used_name varchar(50) DEFAULT NULL COMMENT '?????????',\n" +
            "cert_type varchar(4) DEFAULT NULL COMMENT '????????????',\n" +
            "cert_no varchar(50) DEFAULT NULL COMMENT '????????????',\n" +
            "cust_type varchar(4) DEFAULT NULL COMMENT '????????????',\n" +
            "sex varchar(2) DEFAULT NULL COMMENT '????????????',\n" +
            "nationnality varchar(4) DEFAULT NULL COMMENT '????????????',\n" +
            "country varchar(4) DEFAULT NULL COMMENT '????????????',\n" +
            "home_addr varchar(100) DEFAULT NULL COMMENT '????????????',\n" +
            "birth varchar(8) DEFAULT NULL COMMENT '????????????',\n" +
            "political varchar(4) DEFAULT NULL COMMENT '????????????',\n" +
            "educate_no varchar(4) DEFAULT NULL COMMENT '????????????',\n" +
            "degree varchar(4) DEFAULT NULL COMMENT '????????????',\n" +
            "graduate_school varchar(100) DEFAULT NULL COMMENT '????????????',\n" +
            "major varchar(50) DEFAULT NULL COMMENT '????????????',\n" +
            "marriage varchar(4) DEFAULT NULL COMMENT '????????????',\n" +
            "healthy varchar(4) DEFAULT NULL COMMENT '????????????',\n" +
            "social_guar varchar(30) DEFAULT NULL COMMENT '??????????????????',\n" +
            "local_resi_flag varchar(2) DEFAULT NULL COMMENT '??????????????????',\n" +
            "local_live_situ varchar(20) DEFAULT NULL COMMENT '??????????????????',\n" +
            "other_live varchar(30) DEFAULT NULL COMMENT '??????????????????',\n" +
            "local_duration string DEFAULT NULL COMMENT '?????????????????????',\n" +
            "about_relation varchar(4) DEFAULT NULL COMMENT '?????????????????????',\n" +
            "in_bank_duty varchar(30) DEFAULT NULL COMMENT '???????????????',\n" +
            "hold_stock_amt decimal(20,4) DEFAULT NULL COMMENT '????????????????????????',\n" +
            "vip_lev_cd varchar(4) DEFAULT NULL COMMENT 'VIP????????????',\n" +
            "about_cor_relation varchar(4) DEFAULT NULL COMMENT '?????????????????????',\n" +
            "create_loan_date varchar(8) DEFAULT NULL COMMENT '????????????????????????',\n" +
            "open_acct varchar(30) DEFAULT NULL COMMENT '???????????????????????????',\n" +
            "become_cust varchar(2) DEFAULT NULL COMMENT '????????????????????????',\n" +
            "per_character varchar(50) DEFAULT NULL COMMENT '????????????',\n" +
            "bank_cogo_situ varchar(2) DEFAULT NULL COMMENT '??????????????????',\n" +
            "gain_interest decimal(20,4) DEFAULT NULL COMMENT '??????????????????????????????'";
        String[] split = ddl.split("\n");
        List<OrcField> asList =new ArrayList<OrcField>();
        for(String str:split){
            String[] s = str.split(" ");
            asList.add( new OrcField(s[0],s[1]));
        }
        return asList;
    }


    @Test
    public void getFeilds() throws Exception {
        String hdfsSchema = "hdfs://chail:chail@192.168.51.199:8020";
        addPro(hdfsSchema);
        String allfile = hdfsSchema +
            "/inceptor1/user/hive/warehouse/ycc.db/chail/tmp_gm2/delta_1298700_1298700/bucket_00052";
        OrcUtils orcUtils=new OrcUtils(allfile);
        Iterator<RowMetaAndData> iterator = orcUtils.getRowMetaAndData(allfile);
        while (iterator.hasNext()){
           RowMetaAndData next = iterator.next();
           Object[] data = next.getData();
       }
    }





}
