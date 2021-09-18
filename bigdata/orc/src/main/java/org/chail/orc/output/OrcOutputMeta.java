
package org.chail.orc.output;

import org.chail.orc.OrcField;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.util.List;
import java.util.Map;

/**
 * @author yangc
 */
@Step(id = "OrcOutput", name = "OrcOutput", description = "OrcOutput")
public class OrcOutputMeta extends BaseStepMeta implements StepMetaInterface {
    private String filePath;
    private List<OrcField> schemaList;

    private String compression;

    /**
     * 是否是tdh orc
     */
    private boolean isTDHOrc;

    private Map<String,String> userMetadata;

    @Override
    public void setDefault() {
        this.compression = "None";
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                                 TransMeta transMeta, Trans trans) {

        return new OrcOutput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {

        return new OrcOutputData();
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }


    public List<OrcField> getSchemaList() {
        return schemaList;
    }

    public void setSchemaList(List<OrcField> schemaList) {
        this.schemaList = schemaList;
    }

    public String getCompression() {
        return compression;
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }

    public boolean isTDHOrc() {
        return isTDHOrc;
    }

    public void setTDHOrc(boolean TDHOrc) {
        isTDHOrc = TDHOrc;
    }

    public Map<String, String> getUserMetadata() {
        return userMetadata;
    }

    public void setUserMetadata(Map<String, String> userMetadata) {
        this.userMetadata = userMetadata;
    }
}
