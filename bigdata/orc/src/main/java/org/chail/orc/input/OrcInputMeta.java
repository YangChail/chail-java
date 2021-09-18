
package org.chail.orc.input;

import org.chail.orc.OrcField;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.util.List;

/**
 * @author yangc
 */
@Step(id = "OrcInput", name = "OrcInput", description = "OrcInput.Description")
public class OrcInputMeta extends BaseStepMeta implements StepMetaInterface {
	private String filePath;
	private List<OrcField> schemaList;
    /**
     * 是否是tdh orc
     */
    private boolean isTDHOrc;

	@Override
	public void setDefault() {

	}

	@Override
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		return new OrcInput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	@Override
	public StepDataInterface getStepData() {
		return new OrcInputData();
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

    public boolean isTDHOrc() {
        return isTDHOrc;
    }

    public void setTDHOrc(boolean TDHOrc) {
        isTDHOrc = TDHOrc;
    }
}
