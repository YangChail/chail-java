
package org.chail.orc.input;

import org.apache.log4j.Logger;
import org.apache.orc.Reader;
import org.chail.orc.OrcField;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @author yangc
 */
public class OrcMetaDataReader {
	private static final Logger logger = Logger.getLogger(OrcMetaDataReader.class);
	Reader reader;

	public OrcMetaDataReader(Reader reader) {
		this.reader = reader;
	}

	public void read(List<OrcField> inputFields) {
		inputFields.forEach(field -> {
			try {
				readMetaData(field);
			} catch (Exception e) {
				logger.error("Field " + field.getFormatFieldName() + ": cannot read Orc Metadata");
			}
		});
	}

	private void readMetaData(OrcField inputField) {
		inputField.setPentahoType(readInt(inputField, OrcMetaData.propertyType.TYPE));
	}

	private String readValue(OrcField inputField, OrcMetaData.propertyType metaField) {
		String propertyName = OrcMetaData.determinePropertyName(inputField.getFormatFieldName(), metaField.toString());
		if (reader.hasMetadataValue(propertyName)) {
			ByteBuffer b = reader.getMetadataValue(propertyName);
			return b == null ? null : byteBufferToString(b, Charset.forName("UTF-8"));
		} else {
			return String.valueOf(inputField.getPentahoType());
		}
	}

	private int readInt(OrcField inputField, OrcMetaData.propertyType metaField) {
		String s = readValue(inputField, metaField);
		if (s != null) {
			return Integer.valueOf(readValue(inputField, metaField));
		}
		return 0;
	}

	private String byteBufferToString(ByteBuffer buffer, Charset charset) {
		byte[] bytes;
		if (buffer.hasArray()) {
			bytes = buffer.array();
		} else {
			bytes = new byte[buffer.remaining()];
			buffer.get(bytes);
		}
		return new String(bytes, charset);
	}

}
