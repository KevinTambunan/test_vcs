package ist.los.ocr.services.utils;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import com.wm.util.Debug;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.MinioException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Bucket;
import okhttp3.OkHttpClient;
// --- <<IS-END-IMPORTS>> ---

public final class minio

{
	// ---( internal utility methods )---

	final static minio _instance = new minio();

	static minio _newInstance() { return new minio(); }

	static minio _cast(Object o) { return (minio)o; }

	// ---( server methods )---




	public static final void minioConfig (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(minioConfig)>> ---
		// @sigtype java 3.5
		// [i] field:0:required endpoint
		// [i] field:0:required accessKey
		// [i] field:0:required secretKey
		IDataCursor pipeCur = pipeline.getCursor();
		String endpoint = IDataUtil.getString(pipeCur,"http://127.0.0.1:9000");
		String accessKey = IDataUtil.getString(pipeCur,"XSYa83afalC3NFRl");
		String secretKey = IDataUtil.getString(pipeCur,"Kwwthuy4zbjzibrq5I3ztWPgAtF6Npui");
		 
		String status = "		// Get base64zippedpdf from pipeline\r\n"
				+ "		IDataCursor cursor = pipeline.getCursor();\r\n"
				+ "		String base64zippedpdf = IDataUtil.getString(cursor, \"base64zippedpdf\");\r\n"
				+ "		String bucketName = IDataUtil.getString(cursor, \"bucketName\");\r\n"
				+ "		String originalFilename = IDataUtil.getString(cursor, \"originalFilename\");\r\n"
				+ "		String mimeType = \"application/pdf\";\r\n"
				+ "		\r\n"
				+ "		// Convert base64zippedpdf to byte array\r\n"
				+ "		byte[] bytes = DatatypeConverter.parseBase64Binary(base64zippedpdf);\r\n"
				+ "\r\n"
				+ "		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);\r\n"
				+ "				ZipInputStream zis = new ZipInputStream(bais)) {\r\n"
				+ "			// Read the contents of the zip file\r\n"
				+ "			ZipEntry zipEntry = zis.getNextEntry();\r\n"
				+ "			while (zipEntry != null) {\r\n"
				+ "				if (!zipEntry.isDirectory()) {\r\n"
				+ "					String fileName = zipEntry.getName();\r\n"
				+ "					if (fileName.endsWith(\".pdf\")) {\r\n"
				+ "						ByteArrayOutputStream baos = new ByteArrayOutputStream();\r\n"
				+ "						byte[] buffer = new byte[1024];\r\n"
				+ "						int len;\r\n"
				+ "						while ((len = zis.read(buffer)) > 0) {\r\n"
				+ "							baos.write(buffer, 0, len);\r\n"
				+ "						}\r\n"
				+ "\r\n"
				+ "						// Upload the unzipped pdf file to Minio\r\n"
				+ "						uploadFile(bucketName, originalFilename, mimeType, baos.toByteArray());\r\n"
				+ "					}\r\n"
				+ "				}\r\n"
				+ "				zipEntry = z";
		MinioClient mc = minioClient(endpoint, accessKey, secretKey);
		try {
			List<Bucket> bucketList = mc.listBuckets();
			status = "Connection success, total buckets : "+bucketList.size();
		}catch (MinioException me){
			status = "Connection failed : "+me.getMessage();
		}catch (Exception e){
			status = "Connection failed : "+e.getMessage();
		}
		IDataUtil.put(pipeCur, "status",status);
		pipeCur.destroy();  
			 
		// --- <<IS-END>> ---

                
	}



	public static final void testServiceUploadFileToMinio (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(testServiceUploadFileToMinio)>> ---
		// @sigtype java 3.5
		// [i] field:0:required Untitled
	
		// --- <<IS-END>> ---

                
	}



	public static final void uploadFileToMinio (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(uploadFileToMinio)>> ---
		// @sigtype java 3.5
		// [i] field:0:required originalFilename
		// [i] field:0:required sourcePathFile
		// [i] field:0:required destPathFile
		// [i] field:0:required bucketName
		// [i] field:0:required mimeType
		// [i] field:0:required ext
		//Program 
		IDataCursor pipeCur = pipeline.getCursor();
		String bucketName = IDataUtil.getString(pipeCur,"bucketName");
		String originalFilename = IDataUtil.getString(pipeCur,"originalFilename");
		String mimeType = IDataUtil.getString(pipeCur,"mimeType");
		String ext = IDataUtil.getString(pipeCur,"ext");
		String sourcePathFile = IDataUtil.getString(pipeCur,"sourcePathFile");
		String destPathFile = IDataUtil.getString(pipeCur,"destPathFile");
		
		Path sftpFolderLocal = Paths.get(sourcePathFile);
		String status = "";
		
		Map<String, Object> result = new HashMap<>();
		  try (Stream<Path> pathStream = Files.list(sftpFolderLocal)) {
		     pathStream.parallel()
		        .filter(file -> !Files.isDirectory(file))
		        .filter(path -> path.toString().toLowerCase().endsWith(ext))
		        .forEach(path -> {
		           try {
		        	   byte[] bytes = Files.readAllBytes( path.toAbsolutePath());
		        	   uploadFile(bucketName, destPathFile+originalFilename, mimeType,bytes);
				       result.put("fileUploaded", destPathFile);
		              result.put("success", Boolean.TRUE);
		           } catch (Exception e) {
		              result.put("fileUploaded", null);
		              result.put("success", Boolean.FALSE);
		           }
		        });
		  } catch (IOException e1) {
		     e1.printStackTrace();
		     result.put("fileUploaded", null);
		     result.put("success", Boolean.FALSE);
		  }
				IDataUtil.put(pipeCur, "status",status);
				pipeCur.destroy();			
		// --- <<IS-END>> ---

                
	}

	// --- <<IS-START-SHARED>> ---
	public static MinioClient client;
	public static void uploadFile(String bucketName, String originalFilename, String mimeType, byte[] bytes) throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException{
		if(!client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())){
			client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
		}
		try(ByteArrayInputStream bais = new ByteArrayInputStream(bytes)){
			client.putObject(
		 			PutObjectArgs
						.builder()
						.bucket(bucketName)
						.object(originalFilename)
						.stream(bais, bais.available(), -1)
						.contentType(mimeType)
						.build()
					);
		}
	}
	
	private static MinioClient minioClient(String endpoint, String accessKey, String secretKey){
		MinioClient minioClient = 
				MinioClient.builder()
						.endpoint(endpoint)
						.credentials(accessKey, secretKey)
						.httpClient(okHttpClient())
						.build();
		return minioClient;
				
	}
	
	private static OkHttpClient okHttpClient(){
		OkHttpClient.Builder builder = new OkHttpClient.Builder();
		builder.connectTimeout(30, TimeUnit.SECONDS)
				.readTimeout(30, TimeUnit.SECONDS)
				.writeTimeout(30, TimeUnit.SECONDS)
				.retryOnConnectionFailure(true);
		return builder.build();
	}


	
	// --- <<IS-END-SHARED>> ---
}

