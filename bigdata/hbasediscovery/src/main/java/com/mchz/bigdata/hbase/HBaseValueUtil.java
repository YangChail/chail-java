package com.mchz.bigdata.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.NavigableMap;

public class HBaseValueUtil {

	public static Object[] getValue(Result result, HBaseTable hbaseTable) throws Exception {
		List<HBaseColumn> hbaseColumnList = hbaseTable.getHbaseColumnList();
		Object[] obj = new Object[hbaseColumnList.size()];
		for (int i = 0; i < hbaseColumnList.size(); i++) {
			HBaseColumn ob = hbaseColumnList.get(i);
			String familyName = ob.getFamilyName();
			String name = ob.getName();
			if(HBaseUtil.ROW_KEY_NAME.equals(name)&&familyName.equals("")) {
				byte[] row = result.getRow();
				obj[i] = Bytes.toString(row);
				continue;
			}
			byte[] family = getByte(familyName);
			NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(family);
			if (familyMap == null) {
				obj[i] = null;
				continue;
			}
		
			byte[] col = getByte(name);
			byte[] bs = familyMap.get(col);
			if (bs == null) {
				obj[i] = null;
				continue;
			}
			String string = Bytes.toString(bs);
			obj[i] = string;
		}
		return obj;
	}

	public static byte[] getByte(String str) {
		return Bytes.toBytes(str);
	}

}
