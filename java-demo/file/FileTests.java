package com.chail.apputil.file;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.chail.apputil.jdbc.jdbcutilsone.JDBCUtil;

public class FileTests {

	private static final String path=System.getProperties().getProperty("user.dir")+"\\src\\test";
	@Test
	public void test() {
		String pathfile=path+"\\aa.txt";
		File file=new File(pathfile);
		List<String> readFileByLines = ReadFromFile.readFileByLines(pathfile);
		removeDuplicateWithOrder(readFileByLines);
		String url="jdbc:postgresql://192.168.200.103:5432/dm";
		String driver="org.postgresql.Driver";
		String user="dm";
		String pass="hzmcdm";
		JDBCUtil jdbcUtil=new JDBCUtil(url, driver, user, pass);
		jdbcUtil.getConnection();
		 for(String str:readFileByLines) {
			 List<Object> params=new ArrayList<Object>();
			 params.add(str);
			 jdbcUtil.executeUpdate("insert into mc_dic_postcode(zip) values (?)", params);
		 }
		 
	}
	
	// 删除ArrayList中重复元素，保持顺序     
	 public static void removeDuplicateWithOrder(List list) {    
	    Set set = new HashSet();    
	     List newList = new ArrayList();    
	   for (Iterator iter = list.iterator(); iter.hasNext();) {    
	         Object element = iter.next();
	         if(element.toString().length()!=6) {
	        	 continue;
	         }
	         if (set.add(element))    
	            newList.add(element);    
	      }     
	     list.clear();    
	     list.addAll(newList);    
	 }   
	

}
