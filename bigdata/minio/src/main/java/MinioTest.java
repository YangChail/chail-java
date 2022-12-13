import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.errors.*;
import io.minio.messages.Bucket;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;

/**
 * @author : yangc
 * @date :2022/11/24 16:02
 * @description :
 * @modyified By:
 */
public class MinioTest {
    public static void main(String[] args) throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        MinioClient client = new MinioClient.Builder()
                .endpoint("http://192.168.51.194:9000")
                .credentials("WoeaMUcaBnjQQ6cY", "Lq6sFpBGYPnauHxc62Kow3MvSvOq0KCS")
                .build();

        List<Bucket> buckets = client.listBuckets();
        Bucket bucket = buckets.get(0);
        String name = bucket.name();
        ObjectWriteResponse objectWriteResponse = client.putObject(PutObjectArgs.builder().bucket(name).object(UUID.randomUUID().toString()).stream( new FileInputStream(new File("D://test.csv")), -1, -1).build());


        System.out.println();


    }


    public void minioClient(){



    }

}
