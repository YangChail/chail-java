package org.chail.orc.input;

import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.chail.orc.OrcField;
import org.chail.orc.OrcSchemaConverter;
import org.chail.orc.utils.OrcUtils;

import java.util.List;

/**
 * @author yangc
 */
public class MyOrcInputFormat {

	private String fileName;
	private List<OrcField> consumerInputFields;
	private OrcUtils fileUtil;
	private boolean torc=false;

	public MyOrcInputFormat(OrcUtils fileUtil, String fileName) {
		this.fileName = fileName;
        this.fileUtil = fileUtil;
	}

	public MyOrcRecordReader createRecordReader() throws Exception {
		if (fileName == null || consumerInputFields == null) {
			throw new IllegalStateException("fileName or inputFields must not be null");
		}
		return new MyOrcRecordReader(fileName, fileUtil, consumerInputFields,torc);
	}

	public List<OrcField> readSchema() throws Exception {
		return readSchema(fileUtil.getOrcReard(fileName,fileUtil.getConfiguration()));
	}

	protected List<OrcField> readSchema(Reader orcReader) throws Exception {
		List<OrcField> inputFields = OrcSchemaConverter.buildInputFields(readTypeDescription(orcReader));
		OrcMetaDataReader orcMetaDataReader = new OrcMetaDataReader(orcReader);
		orcMetaDataReader.read(inputFields);
		return inputFields;
	}

	public TypeDescription readTypeDescription(Reader orcReader) {
		return orcReader.getSchema();
	}

	public void setConsumerInputFields(List<OrcField> consumerInputFields) {
		this.consumerInputFields = consumerInputFields;
	}

    public boolean isTorc() {
        return torc;
    }

    public void setTorc(boolean torc) {
        this.torc = torc;
    }
}
