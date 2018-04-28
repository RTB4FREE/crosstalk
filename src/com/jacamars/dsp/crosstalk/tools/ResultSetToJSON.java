package com.jacamars.dsp.crosstalk.tools;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jacamars.dsp.crosstalk.manager.Configuration;

public class ResultSetToJSON {
	public static ObjectMapper mapper = new ObjectMapper();
	public static final JsonNodeFactory factory = JsonNodeFactory.instance;
	static InitStub stub = new InitStub();              // declared in Initialize.java
	
	public static void main(String [] args) throws Exception {
		Configuration config = Configuration.getInstance();
		config.initialize("config.json", stub, stub);
		config.connectToSql();
		Statement stmt = config.getStatement();
		ResultSet rs = stmt.executeQuery("select * from Campaign");
		ArrayNode nodes = convert(rs);
		
		for (int i = 0; i < nodes.size(); i++) {
			ObjectNode x = (ObjectNode)nodes.get(i);
			int campaignid = x.get("campaignid").asInt();
			rs = stmt.executeQuery("select * from Targetting where campaignid = " + campaignid);
			ArrayNode inner = convert(rs);
			ObjectNode y = (ObjectNode)inner.get(i);
			x.set("targetting", y);
		}
		
		for (int i = 0; i < nodes.size(); i++) {
			ObjectNode x = (ObjectNode)nodes.get(i);
			int campaignid = x.get("campaignid").asInt();
			rs = stmt.executeQuery("select * from Banner where campaignid = " + campaignid);
			ArrayNode inner = convert(rs);
			x.set("banner", inner);
		}
		
		for (int i = 0; i < nodes.size(); i++) {
			ObjectNode x = (ObjectNode)nodes.get(i);
			int campaignid = x.get("campaignid").asInt();
			rs = stmt.executeQuery("select * from `Banner-Video` where campaignid = " + campaignid);
			ArrayNode inner = convert(rs);
			x.set("banner_video", inner);
		}
		
		String content = mapper
				.writer()
				.withDefaultPrettyPrinter()
				.writeValueAsString(nodes);
		System.out.println(content);
		
	}

	public static synchronized ArrayNode convert(ResultSet rs) throws Exception {
		ArrayNode array = factory.arrayNode();
		ResultSetMetaData rsmd = rs.getMetaData();

		while (rs.next()) {
			ObjectNode child = factory.objectNode();
			int numColumns = rsmd.getColumnCount();
			for (int i = 1; i < numColumns + 1; i++) {
				String column_name = rsmd.getColumnName(i);

				switch (rsmd.getColumnType(i)) {
				case java.sql.Types.BIGINT:
					child.put(column_name, rs.getInt(column_name));
					break;
				case java.sql.Types.BOOLEAN:
					child.put(column_name,rs.getBoolean(column_name));
					break;
				case java.sql.Types.BLOB:
					child.put(column_name, rs.getBlob(column_name)
							.toString().getBytes());
					break;
				case java.sql.Types.DOUBLE:
					child.put(column_name,rs.getDouble(column_name));
					break;
				case java.sql.Types.FLOAT:
					child.put(column_name, rs.getFloat(column_name));
					break;
				case java.sql.Types.INTEGER:
					try {
						child.put(column_name, rs.getInt(column_name));
					} catch (Exception error) {
						System.out.println("NULL POINTER PULLING COLUMN NAME: " + column_name);
						throw error;
					}
					break;		
				case java.sql.Types.NVARCHAR:
					child.put(column_name,rs.getNString(column_name));
					break;
				case java.sql.Types.VARCHAR:
					child.put(column_name,rs.getString(column_name));
					break;
				case java.sql.Types.TINYINT:
					child.put(column_name,rs.getInt(column_name));
					break;
				case java.sql.Types.SMALLINT:
					child.put(column_name,rs.getInt(column_name));
					break;
				case java.sql.Types.DATE:
					child.put(column_name,rs.getDate(column_name)
							.getTime());
					break;
				case java.sql.Types.TIMESTAMP:
					try {
						Timestamp x = rs.getTimestamp(column_name);
						child.put(column_name,x.getTime());
					} catch (Exception e) {       // caused when date is 00:00:00
						child.put(column_name,new Integer(0));
					}
					break;
				case java.sql.Types.DECIMAL:
					BigDecimal decimal = rs.getBigDecimal(column_name);
					child.put(column_name,decimal);
					//System.out.println("------------>" + column_name);
					break;
				case -1:
					child.put(column_name,rs.getString(column_name));
					break;
				case java.sql.Types.CHAR:
					child.put(column_name,rs.getString(column_name));
					break;
				default:
					if  (rsmd.getColumnTypeName(i).equals("TINYINT")) {
						child.put(column_name,rs.getInt(column_name));
					} else
						System.err.println("Can't convert " + column_name
							+ " type is not supported: "
									+ rsmd.getColumnTypeName(i));
				}
			}
			array.add(child);
		}
		return array;

	}
	
	public static String toString(ArrayNode nodes) throws Exception {
        return mapper
        .writer()
        .withDefaultPrettyPrinter()
        .writeValueAsString(nodes);
	}
}