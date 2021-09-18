
package org.chail.orc;

import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.Writer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangc
 */
public class OrcMetaDataUtil {


	public static void addMetaData(Writer writer, Map<String, String> userMetadata){
	    userMetadata.forEach((k,v)->{
            try {
                writer.addUserMetadata(k,toByteBuffer(v));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        });
    }

    public static Map<String, String>  getMetadata( Reader reader){
        OrcProto.Footer footer = reader.getFileTail().getFooter();
        Map<String, String> userMetadata=new HashMap<>();
        List<OrcProto.UserMetadataItem> metadataList = footer.getMetadataList();
        for (OrcProto.UserMetadataItem userMetadataItem : metadataList) {
            String name = userMetadataItem.getName();
            String value = userMetadataItem.getValue().toStringUtf8();
            userMetadata.put(name,value);
        }
        return userMetadata;
    }

	private static ByteBuffer toByteBuffer(int i) throws UnsupportedEncodingException {
		return toByteBuffer(String.valueOf(i));
	}

	private static ByteBuffer toByteBuffer(String s) throws UnsupportedEncodingException {
		return ByteBuffer.wrap(s.getBytes("UTF-8"));
	}

	private static ByteBuffer toByteBuffer(boolean b) throws UnsupportedEncodingException {
		return ByteBuffer.wrap(String.valueOf(b).getBytes("UTF-8"));
	}
}
