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
public class BaseFormatInputField extends BaseFileField {
	@Injection(name = "FIELD_PATH", group = "FIELDS")
	protected String formatFieldName = null;

	private int formatType;
	private int precision = 0;
	private int scale = 0;
	private int maxLength = 0;
	private String stringFormat = "";

	public String getFormatFieldName() {
		return formatFieldName;
	}

	public void setFormatFieldName(String formatFieldName) {
		this.formatFieldName = formatFieldName;
	}

	public String getPentahoFieldName() {
		return getName();
	}

	public void setPentahoFieldName(String pentahoFieldName) {
		setName(pentahoFieldName);
	}

	public int getPentahoType() {
		return getType();
	}

	public void setPentahoType(int pentahoType) {
		setType(pentahoType);
	}

	public int getFormatType() {
		return formatType;
	}

	public void setFormatType(int formatType) {
		this.formatType = formatType;
	}
	@Override
	public int getPrecision() {
		return this.precision;
	}
	@Override
	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	public String getStringFormat() {
		return stringFormat;
	}

	public void setStringFormat(String stringFormat) {
		this.stringFormat = stringFormat == null ? "" : stringFormat;
	}

	public void setPentahoType(String value) {
		setType(value);
	}

	public int getMaxLength() {
		return maxLength;
}

	public void setMaxLength(int maxLength) {
		this.maxLength = maxLength;
	}
}
