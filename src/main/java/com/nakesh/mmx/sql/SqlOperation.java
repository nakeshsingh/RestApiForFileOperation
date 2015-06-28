package com.nakesh.mmx.sql;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
<<<<<<< HEAD
=======
import java.io.InputStream;
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nakesh.mmx.connection.DbManager;

public class SqlOperation {
<<<<<<< HEAD

	public static boolean saveToDataBase(File _inputFile, String _contentType,
			String _version, int _length) {
=======
	
	public static boolean saveToDataBase(File _inputFile, String _contentType, String _version) {
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(_inputFile);
			connection = DbManager.getConnection();
<<<<<<< HEAD
			preparedStatement = connection.prepareStatement("INSERT INTO tblStorage (fileName, fileSize, fileLength, contentType, file, fileVersion) VALUES (?, ?, ?, ?, ?, ?)");
			preparedStatement.setString(1, _inputFile.getName());
			preparedStatement.setString(2, String.valueOf(_inputFile.getTotalSpace()));
			preparedStatement.setString(3, String.valueOf(_length));
			preparedStatement.setString(4, _contentType);
			preparedStatement.setBinaryStream(5, fis, (int) _inputFile.length());
			preparedStatement.setString(6, _version);
=======
			preparedStatement = connection
					.prepareStatement("INSERT INTO tblStorage (name, size, type, image, version) VALUES (?, ?, ?, ?, ?)");
			preparedStatement.setString(1, _inputFile.getName());
			preparedStatement.setString(2,
					String.valueOf(_inputFile.getTotalSpace()));
			preparedStatement.setString(3, _contentType);
			preparedStatement
					.setBinaryStream(4, fis, (int) _inputFile.length());
			preparedStatement.setString(5, _version);
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
			preparedStatement.executeUpdate();
			fis.close();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		} finally {
			DbManager.closeResources(null, preparedStatement, connection);
		}
		return true;
	}
<<<<<<< HEAD

=======
	
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
	public static List<Map<String, Object>> getDataFromDataBase() {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		Map<String, Object> dataMap = new HashMap<String, Object>();
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		try {
<<<<<<< HEAD
			connection = DbManager.getConnection();
			preparedStatement = connection
					.prepareStatement("SELECT * FROM tblStorage WHERE fileName = ?");
			preparedStatement.setString(1, "images.jpeg");
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				System.out.println("name : " + resultSet.getString("fileName"));
				System.out.println("type : " + resultSet.getString("contentType"));
				System.out.println("size : " + resultSet.getString("fileSize"));
				System.out.println("version : " + resultSet.getString("fileVersion"));
				System.out.println("uploadedDate : " + resultSet.getString("eventTime"));
				dataMap.put("name", resultSet.getString("fileName"));
				dataMap.put("type", resultSet.getString("contentType"));
				dataMap.put("size", resultSet.getString("fileSize"));
				dataMap.put("length", resultSet.getString("fileLength"));
				dataMap.put("fileVersion", resultSet.getBytes("fileVersion"));
				dataMap.put("fileData", resultSet.getBytes("file"));
				dataList.add(dataMap);
=======
			byte[] byteImg = null;
			connection = DbManager.getConnection();
			preparedStatement = connection.prepareStatement("SELECT * FROM tblStorage WHERE name = ?");
			preparedStatement.setString(1, "images.jpeg");
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				
				System.out.println("name : " + resultSet.getString("name"));
				System.out.println("type : " + resultSet.getString("type"));
				System.out.println("size : " + resultSet.getString("size"));
				System.out.println("version : " + resultSet.getString("version"));
				System.out.println("uploadedDate : " + resultSet.getString("eventTime"));
				InputStream ins = resultSet.getBinaryStream("image");
		//	    byteImg = resultSet.getBytes("image");
			    dataMap.put("name", resultSet.getString("name"));
			    dataMap.put("type", resultSet.getString("type"));
			    dataMap.put("size", resultSet.getString("size"));
			    dataMap.put("fileData", resultSet.getBytes("image"));
			    dataList.add(dataMap);
	//		    writeFile(byteImg, "/Users/nsingh/Pictures/Uploaded/again.jpeg");
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbManager.closeResources(resultSet, preparedStatement, connection);
		}
		return dataList;
	}
<<<<<<< HEAD

=======
	
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
	public static String getLatestVersion() {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		String version = null;
		try {
<<<<<<< HEAD
			String getVersion = "SELECT fileVersion FROM tblStorage order by id desc limit 1";
=======
			String getVersion = "SELECT version FROM tblStorage order by id desc limit 1";
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
			connection = DbManager.getConnection();
			preparedStatement = connection.prepareStatement(getVersion);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
<<<<<<< HEAD
				System.out.println("latestVersion : "
						+ resultSet.getString("fileVersion"));
				version = resultSet.getString("fileVersion");
=======
				System.out.println("latestVersion : " + resultSet.getString("version"));
				version = resultSet.getString("version");
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbManager.closeResources(resultSet, preparedStatement, connection);
		}
		return version;
	}
<<<<<<< HEAD

	public static List<Object> getfileFromDataBase(String _fileName, String _version) {
=======
	
	public static byte[] getfileFromDataBase(String _fileName, String _version) {
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		byte[] byteImg = null;
<<<<<<< HEAD
		List<Object> dataList = new ArrayList<Object>();
		try {
			connection = DbManager.getConnection();
			StringBuilder builder = new StringBuilder();
			builder.append("SELECT * FROM tblStorage WHERE fileName = ?");
			if (_version != null && !_version.isEmpty()) {
				builder.append(" AND fileVersion = ?");
			}
			builder.append(" limit 1");
			preparedStatement = connection.prepareStatement(builder.toString());
			preparedStatement.setString(1, _fileName);
			if (_version != null && !_version.isEmpty()) {
=======
		try {
			
			connection = DbManager.getConnection();
			StringBuilder builder = new StringBuilder();
			builder.append("SELECT * FROM tblStorage WHERE name = ?");
			if(_version != null && !_version.isEmpty()) {
				builder.append(" AND version = ?");	
			}
			preparedStatement = connection.prepareStatement(builder.toString());
			preparedStatement.setString(1, _fileName);
			if(_version != null && !_version.isEmpty()) {
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
				preparedStatement.setString(2, _version);
			}
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
<<<<<<< HEAD

				System.out.println("name : " + resultSet.getString("fileName"));
				System.out.println("type : " + resultSet.getString("contentType"));
				System.out.println("size : " + resultSet.getString("fileSize"));
				System.out.println("length : " + resultSet.getString("fileLength"));
				System.out.println("version : " + resultSet.getString("fileVersion"));
				System.out.println("uploadedDate : " + resultSet.getString("eventTime"));
				dataList.add(resultSet.getString("fileName"));
				dataList.add(resultSet.getString("contentType"));
				dataList.add(resultSet.getString("fileSize"));
				dataList.add(resultSet.getString("fileLength"));

				byteImg = resultSet.getBytes("file");
				dataList.add(byteImg);
=======
				
				System.out.println("name : " + resultSet.getString("name"));
				System.out.println("type : " + resultSet.getString("type"));
				System.out.println("size : " + resultSet.getString("size"));
				System.out.println("version : " + resultSet.getString("version"));
				System.out.println("uploadedDate : " + resultSet.getString("eventTime"));
		//		InputStream ins = resultSet.getBinaryStream("image");
			    byteImg = resultSet.getBytes("image");
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbManager.closeResources(resultSet, preparedStatement, connection);
		}
<<<<<<< HEAD
		return dataList;
	}
	
	public static List<Object> getfileFromDataBase(String _version) {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		byte[] byteImg = null;
		List<Object> dataList = new ArrayList<Object>();
		try {
			connection = DbManager.getConnection();
			preparedStatement = connection.prepareStatement("SELECT * FROM tblStorage WHERE fileVersion = ? limit 1");
			preparedStatement.setString(1, _version);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {

				System.out.println("name : " + resultSet.getString("fileName"));
				System.out.println("type : " + resultSet.getString("contentType"));
				System.out.println("size : " + resultSet.getString("fileSize"));
				System.out.println("length : " + resultSet.getString("fileLength"));
				System.out.println("version : " + resultSet.getString("fileVersion"));
				System.out.println("uploadedDate : " + resultSet.getString("eventTime"));
				dataList.add(resultSet.getString("fileName"));
				dataList.add(resultSet.getString("contentType"));
				dataList.add(resultSet.getString("fileSize"));
				dataList.add(resultSet.getString("fileLength"));

				byteImg = resultSet.getBytes("file");
				dataList.add(byteImg);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbManager.closeResources(resultSet, preparedStatement, connection);
		}
		return dataList;
=======
		return byteImg;
>>>>>>> d44fbb889c5da0ddb45627745cb2dca1ce74122c
	}
}
