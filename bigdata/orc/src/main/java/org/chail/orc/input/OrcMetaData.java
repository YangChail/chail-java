package org.chail.orc.input;

/**
 * @author yangc
 */
public class OrcMetaData {
	public static final String ORC_CUSTOM_METADATA_PREFIX = "pentaho";
	public static final String ORC_CUSTOM_METADATA_PROPERTY_DELIMITER = ".";

	public enum propertyType {
		TYPE, NULLABLE, DEFAULT
	}

	public static String determinePropertyName(String fieldName, String property) {
		StringBuilder s = new StringBuilder();
		s.append(ORC_CUSTOM_METADATA_PREFIX).append(ORC_CUSTOM_METADATA_PROPERTY_DELIMITER).append(fieldName)
				.append(ORC_CUSTOM_METADATA_PROPERTY_DELIMITER).append(property);
		return s.toString();
	}

}
