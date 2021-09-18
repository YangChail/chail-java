package org.chail.orc.input;

import org.chail.orc.OrcField;
import org.chail.orc.utils.HdfsFileType;
import org.chail.orc.utils.OrcUtils;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.util.Iterator;
import java.util.List;

/**
 * @author yangc
 */
public class OrcInput extends BaseStep implements StepInterface {

    private OrcUtils hdfsUtils = null;

    public OrcInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                    Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);

    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        OrcInputMeta inputMeta = (OrcInputMeta) smi;
        try {
            List<OrcField> customerSchemaList = inputMeta.getSchemaList();
            if(customerSchemaList==null||customerSchemaList.size()<0){
                logError("初始化错误,字段信息为空");
                return false;
            }
            hdfsUtils = new OrcUtils(inputMeta.getFilePath());
        } catch (Exception e) {
            logError("初始化错误",e);
        }

        return super.init(smi, sdi);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)  {
        OrcInputMeta inputMeta = (OrcInputMeta) smi;
        String filePath = inputMeta.getFilePath();
        MyOrcRecordReader createRecordReader = null;
        List<OrcField> customerSchemaList = inputMeta.getSchemaList();
        MyOrcInputFormat format = null;
        Iterator<RowMetaAndData> iterator = null;
        try {
            if (first) {
                logBasic("ORC-IN START:" + filePath);
                format = new MyOrcInputFormat(hdfsUtils, filePath);
                format.setTorc(inputMeta.isTDHOrc());
                format.setConsumerInputFields(customerSchemaList);
                createRecordReader = format.createRecordReader();
                iterator = createRecordReader.iterator();
                first = false;
            }
            while (iterator.hasNext()) {
                if (!isRunning()) {
                    setOutputDone();
                    return false;
                }
                RowMetaAndData row = iterator.next();
                hdfsUtils.printLog(HdfsFileType.ORC,log);
                putRow(row.getRowMeta(), row.getData());
            }
            setOutputDone();
        } catch (Exception e) {
            logError("orc文件出错,路径为 " + filePath, e);
            stopAll();
            setErrors(1);
            return false;
        } finally {
            try {
                hdfsUtils.closeCloseableStream(createRecordReader);
            } catch (Exception e) {
                logError("关闭orc-input错误!", e);
            }
        }

        return false;
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        super.dispose(smi, sdi);
    }

}
