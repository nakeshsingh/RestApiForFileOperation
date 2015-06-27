package com.nakesh.mmx.rest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.io.IOUtils;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import com.nakesh.mmx.filemanagement.FileManager;
import com.nakesh.mmx.sql.SqlOperation;

@Path("/file")
public class UploadFileService {

	@POST
	@Path("/upload")
	@Consumes("multipart/form-data")
	public Response uploadFile(MultipartFormDataInput input) {

		String fileName = "";
		String filePath = "";
		Map<String, List<InputPart>> uploadForm = input.getFormDataMap();
		List<InputPart> inputParts = uploadForm.get("uploadedFile");

		for (InputPart inputPart : inputParts) {

			MultivaluedMap<String, String> header = inputPart.getHeaders();
			String contentType = header.getFirst("Content-Type");
			FileManager fileManager = new FileManager();
			fileName = fileManager.getFileName(header);

			byte[] bytes = getInputStream(inputPart);
			
			//constructs upload file path
		    filePath = fileManager.getFilePathToUpload(fileName);
			
		    fileManager.writeFile(bytes, filePath);
		    System.out.println("Input file : " + fileName + "   is written to server file");
			
			saveFileInDataBase(new File(filePath), contentType);
			System.out.println("Input file : " + fileName + "   is uploaded to server database");
			
			List<Map<String, Object>> dataList = SqlOperation.getDataFromDataBase();
			System.out.println("Douwnloaded files from server database");
			
			fileManager.writeData(dataList);
			System.out.println("Douwnloaded files to server file");
			
			System.out.println("uploadFile is called ::: Uploaded file name = " + fileName + "   ::   filePath = " + filePath + "   ::   content-type = " + contentType);
			System.out.println(".....Done.....");
		}
		return Response.status(200)
				.entity("uploadFile is called ::: Uploaded file name = " + fileName + "   ::   filePath = " + filePath).build();

	}
	
	@GET
	@Path("/get")
	@Produces("image/text") //@PathParam("fileName") String _fileName, @PathParam("version") String _version
	public Response getFile(@DefaultValue("") @QueryParam("fileName") String _fileName,
			@DefaultValue("")@QueryParam("version") String _version) {
		String filePathDir = "/Users/nsingh/Pictures/DownloadFromServer/";
		File file = new File(filePathDir);
		if(!file.isDirectory()) {	
			file.mkdirs();
		}
		String filePath = filePathDir + _fileName;
		byte[] dataByte = SqlOperation.getfileFromDataBase(_fileName, _version);
		ResponseBuilder response = null;
		if(dataByte == null) {
			response = Response.ok("No data is there");
		} else {
		FileManager fileManager = new FileManager();
		fileManager.writeFile(dataByte, filePath);
		File fileToSend = new File(filePath);
			response = Response.ok((Object) fileToSend);
		response.header("Content-Disposition",
			"attachment; filename=" + _fileName);
		}
		return response.build();
 
	}
	
	private void saveFileInDataBase(File _inputFile, String _contentType) {
		String version = SqlOperation.getLatestVersion();
		SqlOperation.saveToDataBase(_inputFile, _contentType, getNextVersion(version));
	}
	
	private String getNextVersion(String _version) {
		if(_version == null) {
			return "v_1";
		} else {
			String[] versionArray = _version.trim().split("_");
			return versionArray[0] + "_" + (Integer.parseInt(versionArray[1]) + 1);
		}
	}
	
	private byte[] getInputStream(InputPart _inputPart) {
		try {
			InputStream inputStream = _inputPart.getBody(InputStream.class, null);
			return IOUtils.toByteArray(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}