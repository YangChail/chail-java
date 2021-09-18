/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.chail.orc;

import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.trans.steps.file.BaseFileField;

/**
 * @author yangc
 */
public class BaseFormatOutputField extends BaseFileField {
	public static final int DEFAULT_DECIMAL_PRECISION = 10;
	public static final int DEFAULT_DECIMAL_SCALE = 0;
	public static final int DEFAULT_MAX_LENGTH = 100;


	protected int formatType;

	protected int pentahoType;

	protected String formatFieldName;

	protected String pentahoFieldName;

	protected boolean allowNull;

	protected String defaultValue;

	protected int precision;

	protected int scale;

	protected int maxLength;

	public String getFormatFieldName() {
		return formatFieldName;
	}

	public void setFormatFieldName(String formatFieldName) {
		this.formatFieldName = formatFieldName;
        setName(formatFieldName);
	}

	public String getPentahoFieldName() {
		return pentahoFieldName;
	}

	public void setPentahoFieldName(String pentahoFieldName) {
		this.pentahoFieldName = pentahoFieldName;
	}

	public boolean getAllowNull() {
		return allowNull;
	}

	public void setAllowNull(boolean allowNull) {
		this.allowNull = allowNull;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Injection(name = "FIELD_NULL_STRING", group = "FIELDS")
	public void setAllowNull(String allowNull) {
		if (allowNull != null && allowNull.length() > 0) {
			if (allowNull.equalsIgnoreCase("yes") || allowNull.equalsIgnoreCase("y")) {
				this.allowNull = true;
			} else if (allowNull.equalsIgnoreCase("no") || allowNull.equalsIgnoreCase("n")) {
				this.allowNull = false;
			} else {
				this.allowNull = Boolean.parseBoolean(allowNull);
			}
		}
	}

	public int getFormatType() {
		return formatType;
	}

	public void setFormatType(int formatType) {
		this.formatType = formatType;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(String precision) {
		if (precision == null || precision.equals("")) {
			this.precision = DEFAULT_DECIMAL_PRECISION;
		} else {
			this.precision = Integer.valueOf(precision);
			if (this.precision <= 0) {
				this.precision = DEFAULT_DECIMAL_PRECISION;
			}
		}
	}

	public int getScale() {
		return scale;
	}

	public void setScale(String scale) {
		if (scale == null || scale.equals("")) {
			this.scale = DEFAULT_DECIMAL_SCALE;
		} else {
			this.scale = Integer.valueOf(scale);
			if (this.scale < 0) {
				this.scale = DEFAULT_DECIMAL_SCALE;
			}
		}
	}

	public int getPentahoType() {
		return pentahoType;
	}

	public void setPentahoType(int pentahoType) {
		this.pentahoType = pentahoType;
	}

	public int getMaxLength() {
		return maxLength;
}

	public void setMaxLength(String maxLength) {
		if (maxLength == null || maxLength.equals("")) {
			this.maxLength = DEFAULT_MAX_LENGTH;
		} else {
			this.maxLength = Integer.valueOf(maxLength);
			if (this.maxLength < 0) {
				this.maxLength = DEFAULT_MAX_LENGTH;
			}
		}
	}
}
