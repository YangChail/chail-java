package com.mchz.bigdata.hbase;

public class HBaseColumn implements Comparable<HBaseColumn>{
	private boolean isKey=false;
	private String name;
	private String type = "String";

	private String familyName;

	public HBaseColumn() {
	}

	public HBaseColumn(String name, String familyName) {
		this.name = name;
		this.familyName = familyName;
	}

	public HBaseColumn(String name, String familyName,boolean isKey) {
		this.name = name;
		this.familyName = familyName;
		this.isKey = isKey;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof HBaseColumn) {
			HBaseColumn h = (HBaseColumn) obj;
			return h.getName().equals(name);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public int compareTo(HBaseColumn o) {
		return o.getName().compareTo( name);
	}

	public boolean isKey() {
		return isKey;
	}

	public void setKey(boolean isKey) {
		this.isKey = isKey;
	}

	public String toSQLName() {
		return String.format("\"%s:%s\"", familyName, name, "varchar");
	}	
	
	public String toSQL() {
		return String.format("%s %s", toSQLName(), "varchar");
	}	

    @Override
    public String toString() {
        return "HbaseColumn{" +
            "isKey=" + isKey +
            ", name='" + name + '\'' +
            ", type='" + type + '\'' +
            ", familyName='" + familyName + '\'' +
            '}';
    }
}
