package org.apache.nifi.processors.oraclecdc.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.codec.binary.Base32;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.oraclecdc.OracleChangeCapture;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class ProcessLCR {
	private ClassLoader classLoader;
	 protected ProcessLCR(ClassLoader classLoader){
		 this.classLoader=classLoader;
	 }
	
	 protected final byte[] lcr2ff(Object row, final ProcessContext context, final ProcessSession session) throws Throwable{
	    	
		    try{
	    		JsonObject jsonObj = new JsonObject();
	    		
    	  		jsonObj.addProperty("timestamp", getTimeStamp(getValue(row, "getSourceTime").toString()));
    	  		jsonObj.addProperty("database", getValue(row, "getSourceDatabaseName").toString());
    	  		jsonObj.addProperty("schema", getValue(row, "getObjectOwner").toString());
    	  		jsonObj.addProperty("table", getValue(row, "getObjectName").toString());
    	  		String commandType=getValue(row, "getCommandType").toString();
    	  		jsonObj.addProperty("cdc_type",  commandType);
    	  		jsonObj.addProperty("transactionId", getValue(row, "getTransactionId").toString());
    	  		jsonObj.addProperty("position",new String(new Base32(true).encode((byte[])getValue(row, "getPosition"))));
    	  		JsonArray columns = new JsonArray(); 
    	  		jsonObj.add("columns",columns);
    	  		for (Object columnValue : (Object[])getValue(row, "getNewValues")) {
    	  			JsonObject column = convert(columnValue);
    	  			if(column!=null){
    	  				columns.add(column);
    	  			}
    	  		}
	    	  		
	    	 System.out.println(jsonObj.toString());
	    	FlowFile flowFile = session.create();
	    	
	    	flowFile = session.putAttribute(flowFile, "cdcType", commandType);
	    	
	    	flowFile = session.write(flowFile, new OutputStreamCallback() {
				
				@Override
				public void process(OutputStream outputStream) throws IOException {
					// TODO Auto-generated method stub
					outputStream.write(jsonObj.toString().getBytes());
				}
			});
	    	
	    	switch(commandType){
				case "INSERT":
					session.transfer(flowFile,OracleChangeCapture.INSERTS);
					break;
		        case "UPDATE":
		        	session.transfer(flowFile,OracleChangeCapture.UPDATES);
		  			break;
		        case "DELETE":
		        	session.transfer(flowFile,OracleChangeCapture.DELETES);
		  			break;
			      default:
			        session.transfer(flowFile, OracleChangeCapture.UNMATCHED);
			        break;
	    	}
	    	
	    	return (byte[])getValue(row, "getPosition");
	    	}catch(Exception ex){
	    		ex.printStackTrace();
	    		throw new ProcessException("error creating json message" +ex.getMessage());
	    	}
	    }
	    
	    protected final JsonObject convert(Object value) throws SQLException,Throwable {
	        Object datum = getColumnValue(value, "getColumnData");
	    	Class columnValue = Class.forName("oracle.streams.ColumnValue",false,this.classLoader);

	        JsonObject column= new JsonObject();
	        if (null == datum) {
	        	return null;
	        }

	        column.addProperty("name", getColumnValue(value,"getColumnName").toString());
	        column.addProperty("oracleType", getColumnValue(value,"getColumnDataType").toString());
	        int dataType = (int)getColumnValue(value,"getColumnDataType");
	        DatumParser parser = new DatumParser(datum,this.classLoader);
	        switch (dataType) {
	          case 101:
	        	column.addProperty("type","double");
	          	column.addProperty("value",parser.doubleValue());
	            break;
	          case 100:
	        	  column.addProperty("type","float");
	          	column.addProperty("value",parser.floatValue());
	            break;
	          case 1:
	        	  column.addProperty("type","String");
	          	column.addProperty("value",parser.stringValue());
	            break;
	          case 12:
	        	  column.addProperty("type","Date");
	          	column.addProperty("value",new Date(parser.timeStampValue(Calendar.getInstance())).toGMTString());
	            break;
	          case 2:
	        	  column.addProperty("type","BigDecimal");
	          	column.addProperty("value",parser.bigDecimalValue());
	          	break;
//	          case 231:
//	            value = convertTimestampLTZ(changeKey, datum);
//	            break;
//	          case 100:
//	            value = convertTimestampTZ(changeKey, datum);
//	            break;
	          default:
	        	  column.addProperty("type","String");
	          	column.addProperty("value", parser.stringValue());
	        }
	        return column;
	    }
	    
	    protected  Object getValue(Object row,String methodName) throws Throwable{
	    	Class<?> rowLCR = Class.forName("oracle.streams.RowLCR",false,this.classLoader);
	    	Method method = rowLCR.getMethod(methodName);
	    	return method.invoke(row);
	    }
	    
	    protected  long getTimeStamp(String timeStamp) throws Throwable{
	    	
	    	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    	Date date = format.parse(timeStamp);
	    	return date.getTime();
	    }
	    
	    protected  Object getColumnValue(Object columnValue, String methodName) throws Throwable{
	    	 Class<?> columnCls = Class.forName("oracle.streams.ColumnValue",false,this.classLoader);
	    	 Method method = columnCls.getMethod(methodName);
	    	 return method.invoke(columnValue);
	    }
	    
	    protected  int getTypeConstant(String typeName) throws Throwable{
	    	Class<?> columnCls = Class.forName("oracle.streams.ColumnValue",false,this.classLoader);
	    	return columnCls.getDeclaredField(typeName).getInt(null);
	    	
	    }

}
