package com.nakesh.mmx.sql;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
	
	public static boolean saveToDataBase(File _inputFile, String _contentType, String _version) {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(_inputFile);
			connection = DbManager.getConnection();
			preparedStatement = connection
					.prepareStatement("INSERT INTO tblStorage (name, size, type, image, version) VALUES (?, ?, ?, ?, ?)");
			preparedStatement.setString(1, _inputFile.getName());
			preparedStatement.setString(2,
					String.valueOf(_inputFile.getTotalSpace()));
			preparedStatement.setString(3, _contentType);
			preparedStatement
					.setBinaryStream(4, fis, (int) _inputFile.length());
			preparedStatement.setString(5, _version);
			preparedStatement.executeUpdate();
			fis.close();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		} finally {
			DbManager.closeResources(null, preparedStatement, connection);
		}
		return true;
	}
	
	public static List<Map<String, Object>> getDataFromDataBase() {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		Map<String, Object> dataMap = new HashMap<String, Object>();
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		try {
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
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbManager.closeResources(resultSet, preparedStatement, connection);
		}
		return dataList;
	}
	
	public static String getLatestVersion() {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		String version = null;
		try {
			String getVersion = "SELECT version FROM tblStorage order by id desc limit 1";
			connection = DbManager.getConnection();
			preparedStatement = connection.prepareStatement(getVersion);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				System.out.println("latestVersion : " + resultSet.getString("version"));
				version = resultSet.getString("version");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbManager.closeResources(resultSet, preparedStatement, connection);
		}
		return version;
	}
	
	public static byte[] getfileFromDataBase(String _fileName, String _version) {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		byte[] byteImg = null;
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
				preparedStatement.setString(2, _version);
			}
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				
				System.out.println("name : " + resultSet.getString("name"));
				System.out.println("type : " + resultSet.getString("type"));
				System.out.println("size : " + resultSet.getString("size"));
				System.out.println("version : " + resultSet.getString("version"));
				System.out.println("uploadedDate : " + resultSet.getString("eventTime"));
		//		InputStream ins = resultSet.getBinaryStream("image");
			    byteImg = resultSet.getBytes("image");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbManager.closeResources(resultSet, preparedStatement, connection);
		}
		return byteImg;
	}
}
