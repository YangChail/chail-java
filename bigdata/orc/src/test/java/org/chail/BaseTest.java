package org.chail;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.SimpleLoggingObject;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BaseTest {
    protected static final Logger logger = LoggerFactory.getLogger(BaseTest.class);

    public String name = "test=xxxxxxx" + System.currentTimeMillis();
    public int maxCount = 10;

    public static final LoggingObjectInterface loggingObject = new SimpleLoggingObject("Step metadata",
        LoggingObjectType.STEPMETA, null);

    public void init() {
        initKettleEnvironment();
    }

    /*
     * 初始化kettle环境
     */
    private void initKettleEnvironment() {
        try {
            System.setProperty("KETTLE_EMPTY_STRING_DIFFERS_FROM_NULL", "true");

            //System.setProperty("HADOOP_USER_NAME", "hive");
            // 无视时区
            System.setProperty("KETTLE_COMPATIBILITY_DB_IGNORE_TIMEZONE", "Y");
            // db2乱码报错问题
            System.setProperty("db2.jcc.charsetDecoderEncoder", "3");
            KettleEnvironment.init();
            // 注册自定义的日志接收日志
//			DmKettleErrorLoggingEvent dmKettleErrorLoggingEvent=new DmKettleErrorLoggingEvent();
//			KettleLogStore.getAppender().addLoggingEventListener(dmKettleErrorLoggingEvent);
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }

    public static List<Object[]> preview(StepMetaInterface meta, String stepName, int maxCount) {
        List<Object[]> result = new ArrayList<>();
        TransMeta previewMeta = generatePreviewTransformation(meta, stepName);
        Trans trans = new Trans(previewMeta);
        AtomicInteger aa = new AtomicInteger(0);
        try {
            trans.prepareExecution(null);
        } catch (final KettleException e) {
            e.printStackTrace();
        }
        StepInterface serviceStep = trans.findRunThread(stepName);

        RowListener rowListener = new RowListener() {
            @Override
            public void rowReadEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
                System.out.println();
            }

            @Override
            public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
                if (maxCount < 0) {
                    return;
                }
                if (!trans.isFinished() && result.size() < maxCount) {
                    System.out.println(Arrays.toString(row));
                    result.add(row);
                }
            }

            @Override
            public void errorRowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
            }
        };
        serviceStep.addRowListener(rowListener);
        try {
            trans.startThreads();
        } catch (KettleException e) {
            e.printStackTrace();
        }
        if (maxCount > 0) {
            while (!trans.isFinished() && result.size() < maxCount) {
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            while (!trans.isFinished()) {
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (!trans.isFinished()) {
            trans.stopAll();
        }
        trans.cleanup();
        return result;
    }


    public static void transRunning(TransMeta transMeta) {
        Trans trans = new Trans(transMeta);
        AtomicInteger aa = new AtomicInteger(0);
        try {
            trans.prepareExecution(null);
            StepInterface serviceStep = trans.findRunThread("Test-" + System.currentTimeMillis());
            trans.startThreads();
            while (!trans.isFinished()) {
               if(trans.getErrors()>0) {
                   System.err.println("错误");
               }
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (final KettleException e) {
            e.printStackTrace();
        }
    }


    public static RowListener getListener(List<Object[]> result) {
        RowListener rowListener = new RowListener() {
            @Override
            public void rowReadEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
                System.out.println();
            }

            @Override
            public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
                result.add(row);

            }

            @Override
            public void errorRowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
                System.out.println();
            }
        };
        return rowListener;
    }


    public static List<Object[]> previewOut(StepMetaInterface metaTwo, String stepName, int maxCount) {
        List<Object[]> result = new ArrayList<>();
        TransMeta previewMeta = generatePreviewTransformation(getInputRow(), System.currentTimeMillis() + "One",
            metaTwo, System.currentTimeMillis() + "Two");
        Trans trans = new Trans(previewMeta);
        AtomicInteger aa = new AtomicInteger(0);
        try {
            trans.prepareExecution(null);
        } catch (final KettleException e) {
            e.printStackTrace();
        }
        RowListener listener = getListener(result);
        StepMetaDataCombi stepMetaDataCombi = trans.getSteps().get(0);
        stepMetaDataCombi.step.addRowListener(listener);
        try {
            trans.startThreads();
        } catch (KettleException e) {
            e.printStackTrace();
        }
        while (!trans.isFinished()) {
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (!trans.isFinished()) {
            trans.stopAll();
        }
        trans.cleanup();
        return result;
    }

    public static TransMeta generatePreviewTransformation(StepMetaInterface oneMeta, String oneStepname) {
        PluginRegistry registry = PluginRegistry.getInstance();
        TransMeta previewMeta = new TransMeta();
        previewMeta.setName(oneStepname);
        StepMeta one = new StepMeta(registry.getPluginId(StepPluginType.class, oneMeta), oneStepname, oneMeta);
        one.setLocation(50, 50);
        one.setDraw(true);
        previewMeta.addStep(one);
        return previewMeta;
    }

    public static TransMeta generatePreviewTransformation(StepMetaInterface oneMeta, String oneStepname,
                                                          StepMetaInterface twoMeta, String twoStepname) {
        PluginRegistry registry = PluginRegistry.getInstance();
        TransMeta previewMeta = new TransMeta();
        previewMeta.setName(oneStepname);
        StepMeta one = new StepMeta(registry.getPluginId(StepPluginType.class, oneMeta), oneStepname, oneMeta);
        one.setLocation(50, 50);
        one.setDraw(true);
        previewMeta.addStep(one);

        previewMeta.setName(twoStepname);
        StepMeta two = new StepMeta(registry.getPluginId(StepPluginType.class, twoMeta), twoStepname, twoMeta);
        two.setLocation(50, 100);
        two.setDraw(true);
        previewMeta.addStep(two);
        TransHopMeta hi = new TransHopMeta(one, two);
        previewMeta.addTransHop(hi);
        return previewMeta;
    }


    public static StepMetaInterface getInputRow() {
        RowGeneratorMeta generatorMeta = new RowGeneratorMeta();
        //generatorMeta.setDefault();
        generatorMeta.allocate(6);

        generatorMeta.setRowLimit("10");
        String fieldName[] = new String[6];
        String type[] = new String[6];
        String nullif[] = new String[6];
        int length[] = new int[6];
        int precision[] = new int[6];
        boolean set_empty_string[] = new boolean[6];
        String fieldFormat[] = new String[6];
        //
        fieldName[0] = "a6";
        type[0] = "Timestamp";
        nullif[0] = "2019-11-11 12:12:12.123450000";
        length[0] = -1;
        precision[0] = -1;
        set_empty_string[0] = false;
        //
        fieldName[1] = "a1";
        type[1] = "BigNumber";
        nullif[1] = "12";
        length[1] = -1;
        precision[1] = -1;
        set_empty_string[1] = false;
        //
        fieldName[2] = "a2";
        type[2] = "Integer";
        nullif[2] = "1234";
        length[2] = -1;
        precision[2] = -1;
        set_empty_string[2] = false;
        //
        fieldName[3] = "a3";
        type[3] = "Date";
        nullif[3] = "2019-11-11";
        length[3] = -1;
        precision[3] = -1;
        set_empty_string[3] = false;
        fieldFormat[3] = "yyyy-MM-dd";
        //
        fieldName[4] = "a4";
        type[4] = "Number";
        nullif[4] = "12345.678";
        length[4] = -1;
        precision[4] = -1;
        set_empty_string[4] = false;


        //
        fieldName[5] = "a5";
        type[5] = "String";
        nullif[5] = "1234aa";
        length[5] = -1;
        precision[5] = -1;
        set_empty_string[5] = false;

        ///////
        generatorMeta.setFieldName(fieldName);
        generatorMeta.setFieldType(type);
        generatorMeta.setFieldLength(length);
        generatorMeta.setValue(nullif);
        generatorMeta.setSetEmptyString(set_empty_string);
        generatorMeta.setFieldPrecision(precision);
        generatorMeta.setFieldFormat(fieldFormat);


        return generatorMeta;
    }

}
