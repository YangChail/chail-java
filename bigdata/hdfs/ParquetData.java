package com.chail.apputil.hdfs;

public class ParquetData {

	
	

	  public static ParquetData unmarshallField( String str ) {
	    String[] values = new String[7];
	    int prev = 0;
	    for ( int i = 0; i < 6; i++ ) {
	      int pos = str.indexOf( '|', prev );
	      if ( pos < 0 ) {
	        throw new RuntimeException( "Wrong field: " + str );
	      }
	      values[i] = str.substring( prev, pos );
	      prev = pos + 1;
	    }
	    if ( str.indexOf( '|', prev ) >= 0 ) {
	      throw new RuntimeException( "Wrong field: " + str );
	    }
	    values[6] = str.substring( prev );

	    ParquetData field = new ParquetData();
//	    field.setFormatFieldName( uc( values[ 0 ] ) );
//	    field.setPentahoFieldName( uc( values[ 1 ] ) );
//	    field.setFormatType( Integer.parseInt( values[ 2 ] ) );
//	    field.setPentahoType( Integer.parseInt( values[ 3 ] ) );
//	    field.setPrecision( Integer.parseInt( values[ 4 ] ) );
//	    field.setScale( Integer.parseInt( values[ 5 ] ) );
//	    field.setStringFormat( values[ 6 ] );
	    return field;
	  }
}
