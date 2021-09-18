
package org.chail.orc.output;

import org.chail.orc.OrcField;
import org.chail.orc.utils.OrcUtils;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.util.List;

/**
 * @author yangc
 */
public class OrcOutput extends BaseStep implements StepInterface {
    String filePath = "";
    MyOrcRecordWriter writer = null;
    private OrcOutputMeta meta;
    private OrcUtils hdfsUtils = null;
    private RowMetaInterface rowMeta;

    public OrcOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                     Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        try {
            meta = (OrcOutputMeta) smi;
            filePath = meta.getFilePath();
            init();
        } catch (Exception e) {
            logError("初始化错误,字段信息为空",e);
            return false;
        }
        return super.init(smi, sdi);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        Object[] currentRow = getRow();
        if (currentRow == null) {
            setOutputDone();
            return false;
        }
        try {
            if (first) {
                logBasic("ORC-OUT START:" + filePath);
                rowMeta = getRowMeta(meta.getSchemaList());
                first = false;
            }
            RowMetaAndData row = new RowMetaAndData(rowMeta, currentRow);
            writer.write(row);
            putRow(rowMeta, row.getData());
        } catch (Exception e) {
            logError("error", e);
            setErrors(1);
            stopAll();
            return false;
        }
        return true;
    }

    public void init() throws Exception {
        if (filePath == null) {
            throw new KettleException("No output files defined");
        }
        hdfsUtils = new OrcUtils(filePath);
        MyOrcOutputFormat data = new MyOrcOutputFormat(hdfsUtils, filePath);
        data.setTDHOrc(meta.isTDHOrc());
        data.deleteOutFile(filePath, true);
        data.setCompression(meta.getCompression());
        writer = data.createRecordWriter(meta.getSchemaList(),meta.getUserMetadata());
    }


    /**
     * 获取元数据
     *
     * @param orcOutputField
     * @return
     * @throws Exception
     */
    private RowMetaInterface getRowMeta(List<OrcField> orcOutputField) throws Exception {
        List<OrcField> refiled=!meta.isTDHOrc() ? orcOutputField : TDHOrcFileds.createOrcOutputField(orcOutputField);
        RowMetaInterface outputRMI = new RowMeta();
        RowMetaInterface rowMetaInterface = getInputRowMeta().clone();
        for (int i = 0; i < refiled.size(); i++) {
            int inputRowIndex = rowMetaInterface.indexOfValue(refiled.get(i).getPentahoFieldName());
            if (inputRowIndex == -1) {
                throw new KettleException("Field name [" + refiled.get(i).getPentahoFieldName()
                    + " ] couldn't be found in the input stream!");
            } else {
                ValueMetaInterface vmi = ValueMetaFactory
                    .cloneValueMeta(getInputRowMeta().getValueMeta(inputRowIndex));
                outputRMI.addValueMeta(i, vmi);
            }
        }
        return outputRMI;
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        try {
            if (hdfsUtils != null) {
                hdfsUtils.closeCloseableStream(writer);
            }
        } catch (Exception e) {
            logError("close error!", e);
        }
        super.dispose(smi,sdi);
    }

}
