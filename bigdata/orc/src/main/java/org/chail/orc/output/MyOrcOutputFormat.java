
package org.chail.orc.output;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.chail.orc.OrcField;
import org.chail.orc.OrcSchemaConverter;
import org.chail.orc.utils.OrcUtils;

import java.util.List;
import java.util.Map;

/**
 * @author yangc
 */
public class MyOrcOutputFormat {
    /**
     * In kilobytes
     */
    private static final  int DEFAULT_COMPRESS_SIZE = 16384;
    /**
     *In megabytes
     */
    private	static final int DEFAULT_STRIPE_SIZE = 64;
    /**
     * In rows
     */
    private static final int DEFAULT_ROW_INDEX_STRIDE = 10000;

    private String STRIPE_SIZE_KEY = "orc.stripe.size";
    private String COMPRESSION_KEY = "orc.compress";
    private String COMPRESS_SIZE_KEY = "orc.compress.size";
    private String ROW_INDEX_STRIDE_KEY = "orc.row.index.stride";
    private String CREATE_INDEX_KEY = "orc.create.index";
    private  Configuration conf;
    public enum COMPRESSION {
		NONE, SNAPPY, ZLIB, LZO
	}

	private String outputFilename;
	private OrcUtils orcUtils;
	private COMPRESSION compression = COMPRESSION.NONE;
	private int compressSize = 0;
	private int stripeSize = DEFAULT_STRIPE_SIZE;
	private int rowIndexStride = 0;
    private boolean isTDHOrc=false;

	public MyOrcOutputFormat(OrcUtils orcUtils, String outputFilename) throws Exception {
		this.orcUtils = orcUtils;
		this.outputFilename = outputFilename;
        this.conf = orcUtils.getConfiguration();
    }

	public MyOrcRecordWriter createRecordWriter(List<OrcField> fields, Map<String, String> userMetadata ) throws Exception {
		if (fields == null) {
			throw new Exception("Invalid state.  The fields to write are null");
		}
		if (outputFilename == null) {
			throw new Exception("Invalid state.  The outputFileName is null");
		}
        TypeDescription schema = !isTDHOrc ? OrcSchemaConverter.buildTypeDescription(fields) : TDHOrcFileds.getTDHOrcTypeDescription(fields);
        List<OrcField> resfields=!isTDHOrc ? fields : TDHOrcFileds.createOrcOutputField(fields);
        MyOrcRecordWriter myOrcRecordWriter = new MyOrcRecordWriter(resfields, schema, outputFilename, isTDHOrc, orcUtils,userMetadata);
        return myOrcRecordWriter;
	}

    /**
     * 设置目标文件如果存在删除
     * @param file
     * @param override
     * @throws Exception
     */
	public void deleteOutFile(String file, boolean override) throws Exception {
		Path outputFile = new Path(outputFilename);
        OrcUtils hdfsUtils = new OrcUtils(file);
		boolean deleteIfExist = hdfsUtils.deleteIfExist(outputFile, override);
		if(!deleteIfExist) {
			throw new Exception("文件存在或者删除错误");
		}

	}

	public void setCompression(String compression) {
		if(StringUtils.isEmpty(compression)) {
			setCompression(COMPRESSION.NONE);
			return ;
		}
		COMPRESSION compress = null;
		switch (compression.toUpperCase()) {
		case "SNAPPY":
			compress = COMPRESSION.SNAPPY;
			break;
		case "ZLIB":
			compress = COMPRESSION.ZLIB;
			break;
		case "LZO":
			compress = COMPRESSION.LZO;
			break;
		default:
			compress = COMPRESSION.NONE;
		}
		setCompression(compress);
	}

	public void setCompression(COMPRESSION compression) {
		this.compression = compression;
		conf.set(COMPRESSION_KEY, compression.toString());
		if (compression == COMPRESSION.NONE) {
			compressSize = 0;
			conf.unset(COMPRESS_SIZE_KEY);
		} else if (compressSize == 0) {
			compressSize = DEFAULT_COMPRESS_SIZE;
			conf.set(COMPRESS_SIZE_KEY, Integer.toString(DEFAULT_COMPRESS_SIZE));
		}
	}

	public void setStripeSize(int megabytes) {
		if (stripeSize > 0) {
			stripeSize = megabytes;
			conf.set(STRIPE_SIZE_KEY, Integer.toString(1024 * 1024 * stripeSize));
		}
	}

	public void setRowIndexStride(int numRows) {
		if (numRows > 0) {
			rowIndexStride = numRows;
			conf.set(CREATE_INDEX_KEY, "true");
			conf.set(ROW_INDEX_STRIDE_KEY, Integer.toString(1024 * 1024 * rowIndexStride));
		} else if (numRows == 0) {
			rowIndexStride = numRows;
			conf.set(CREATE_INDEX_KEY, "false");
			conf.unset(ROW_INDEX_STRIDE_KEY);
		}
	}


    public boolean isTDHOrc() {
        return isTDHOrc;
    }

    public void setTDHOrc(boolean TDHOrc) {
        isTDHOrc = TDHOrc;
    }
}
