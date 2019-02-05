/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.oraclecdc;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base32;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.oraclecdc.utils.LCRCallBackHandler;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;



@Tags({"Oracle","CDC","change capture"})
@CapabilityDescription("capture oracle CDC changes using xstream api")
@SeeAlso({})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class OracleChangeCapture extends AbstractProcessor {

    public static final PropertyDescriptor XS_OUT = new PropertyDescriptor
            .Builder().name("XS_OUT")
            .displayName("XStream Outbound Server Name")
            .description("xout")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor DB_HOST = new PropertyDescriptor
            .Builder().name("DB_HOST")
            .displayName("source oracle db port")
            .description("localhost")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor DB_PORT = new PropertyDescriptor
            .Builder().name("DB_PORT")
            .displayName("source oracle db port")
            .description("1521")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor DB_SID = new PropertyDescriptor
            .Builder().name("DB_SID")
            .displayName("source oracle SID ")
            .description("source oracle SID ")
            .sensitive(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor DB_USER = new PropertyDescriptor
            .Builder().name("DB_USER")
            .displayName("source oracle xstream capture user")
            .description("source oracle xstream capture user")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor DB_PASS = new PropertyDescriptor
            .Builder().name("DB_PASS")
            .displayName("source oracle xstream capture user password")
            .description("source oracle xstream capture user password")
            .sensitive(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("database-driver-locations")
            .displayName("Database Driver Location(s)")
            .description("Comma-separated list of files/folders and/or URLs containing the driver JAR and its dependencies (if any). For example '/var/tmp/mariadb-java-client-1.1.7.jar'")
            .defaultValue(null)
            .required(true)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.createURLorFileValidator()))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
	

    public static final Relationship INSERTS = new Relationship.Builder()
            .name("INSERTS")
            .description("CDC INSERTS")
            .build();
    
    public static final Relationship UPDATES = new Relationship.Builder()
            .name("UPDATES")
            .description("CDC UPDATES")
            .build();
    
    public static final Relationship DELETES = new Relationship.Builder()
            .name("DELETES")
            .description("CDC DELETES")
            .build();
    public static final Relationship UNMATCHED = new Relationship.Builder()
            .name("UNMATCHED")
            .description("UNSUPPORTED OPERATION")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    
    protected DBCPService dbcpService;
    
    protected Object xsOut;
    
    protected byte[] lastPosition=null;
    
    private ClassLoader driverClassLoader;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(XS_OUT);
        descriptors.add(DB_HOST);
        descriptors.add(DB_PORT);
        descriptors.add(DB_USER);
        descriptors.add(DB_PASS);
        descriptors.add(DB_SID);
        descriptors.add(DB_DRIVER_LOCATION);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(INSERTS);
        relationships.add(UPDATES);
        relationships.add(DELETES);
        relationships.add(UNMATCHED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    	 attach(context);

    }
    
    public void attach(final ProcessContext context){
    	getLogger().info("in attach");
     String xsoutName=context.getProperty(XS_OUT).getValue();	
   	 
   	 try
   	    {
   		 final Connection conn = createConnection(context);
   		 Class<?> xstreamOut = loadClass("oracle.streams.XStreamOut");
   		 Method method = xstreamOut.getMethod("attach", loadClass("oracle.jdbc.OracleConnection"),String.class,byte[].class,int.class);
   		 xsOut = method.invoke(null, loadClass("oracle.jdbc.OracleConnection").cast(conn),xsoutName,lastPosition, 
   				 xstreamOut.getDeclaredField("DEFAULT_MODE").getInt(null)); 
   		 
   	      getLogger().info("Attached to outbound server:"+xsoutName);
   	      getLogger().info("Last Position is: "+lastPosition);  
   	    }
   	    catch(Exception e)
   	    {
   	      System.out.println("cannot attach to outbound server: "+xsoutName);
   	      getLogger().error(e.getMessage());
   	      e.printStackTrace();
   	      throw new ProcessException("cannot attach to outbound server: "+xsoutName);
   	    }
    }
    
    @OnShutdown
    public void shutdown(final ProcessContext context){
    	  detach(context);
    }
    
    public void detach(final ProcessContext context){
    	String xsoutName=context.getProperty(XS_OUT).getValue();
    	getLogger().info("in detach");
    	try
        {
    	  Class xstreamOut = loadClass("oracle.streams.XStreamOut");
    	  Method method = xstreamOut.getDeclaredMethod("detach", int.class);
    	  method.invoke(xsOut, xstreamOut.getDeclaredField("DEFAULT_MODE").getInt(null));
        }
        catch(Exception e)
        {
          getLogger().info("cannot detach from the outbound server: "+xsoutName);
          throw new ProcessException("cannot detach from the outbound server: "+xsoutName);
        }     
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	if (null == xsOut)
        {
          getLogger().info("xstreamOut is null");
          System.exit(0);
        }
     
        try
        {
          	  //LCRCallBackHandler hdlr = new LCRCallBackHandler(context,session);
        	LCRCallBackHandler hdlr =  new LCRCallBackHandler(context,session,this.driverClassLoader);
        	Object proxy=Proxy.newProxyInstance(this.driverClassLoader,
        			new Class[] {loadClass("oracle.streams.XStreamLCRCallbackHandler")},hdlr);
          	  //hdlr.setFinalPosition(xsOut.getFetchLowWatermark());
          	 Class<?> xstreamOut = loadClass("oracle.streams.XStreamOut");
          	 Method method = xstreamOut.getMethod("receiveLCRCallback", loadClass("oracle.streams.XStreamLCRCallbackHandler"),int.class);
          	  //xsOut.receiveLCRCallback(hdlr, XStreamOut.DEFAULT_MODE);
          	 method.invoke(xsOut, proxy,loadClass("oracle.streams.XStreamOut").getDeclaredField("DEFAULT_MODE").getInt(null));
          	  System.out.print("done");
          	  if(hdlr.isMoveWaterMark()){
          		 System.out.println(new String(new Base32(true).encode(hdlr.getFinalPosition())));
          		method = xstreamOut.getMethod("setProcessedLowWatermark",byte[].class, int.class);
              	  //xsOut.receiveLCRCallback(hdlr, XStreamOut.DEFAULT_MODE);
              	 method.invoke(xsOut, hdlr.getFinalPosition(),loadClass("oracle.streams.XStreamOut").getDeclaredField("DEFAULT_MODE").getInt(null));
          		  //xsOut.setProcessedLowWatermark(hdlr.getFinalPosition(),XStreamOut.DEFAULT_MODE); 
          	  }
        }
        catch(Exception e)
        { 
          e.printStackTrace();
          getLogger().warn("exception when processing LCRs");
          getLogger().warn(e.getMessage());
          throw new ProcessException("exception when processing LCRs "+e.getMessage());
        }
    	 
    }
    
//    protected final void lcr2ff(RowLCR row, final ProcessContext context, final ProcessSession session){
//    	try{
//    		JsonObject jsonObj = new JsonObject();
//    	  	if(null != row){
//    	  		jsonObj.addProperty("timestamp", row.getSourceTime().timestampValue().getTime());
//    	  		jsonObj.addProperty("database", row.getSourceDatabaseName());
//    	  		jsonObj.addProperty("schema", row.getObjectOwner());
//    	  		jsonObj.addProperty("table", row.getObjectName());
//    	  		jsonObj.addProperty("cdc_type",  row.getCommandType());
//    	  		jsonObj.addProperty("transactionId", row.getTransactionId());
//    	  		jsonObj.addProperty("position",new Base32().encode(row.getPosition()).toString());
//    	  		JsonArray columns = new JsonArray(); 
//    	  		jsonObj.add("columns",columns);
//    	  		for (oracle.streams.ColumnValue columnValue : row.getNewValues()) {
//    	  			JsonObject column = convert(columnValue);
//    	  			if(column!=null){
//    	  				columns.add(column);
//    	  			}
//    	  		}
//    	  		
//    	  	}
//    	getLogger().info(jsonObj.toString());
//    	FlowFile flowFile = session.create();
//    	
//    	flowFile = session.putAttribute(flowFile, "cdcType", row.getCommandType());
//    	
//    	flowFile = session.write(flowFile, new OutputStreamCallback() {
//			
//			@Override
//			public void process(OutputStream outputStream) throws IOException {
//				// TODO Auto-generated method stub
//				outputStream.write(jsonObj.toString().getBytes());
//			}
//		});
//    	
//    	switch(row.getCommandType()){
//			case RowLCR.INSERT:
//				session.transfer(flowFile,INSERTS);
//				break;
//	        case RowLCR.UPDATE:
//	        	session.transfer(flowFile,UPDATES);
//	  			break;
//	        case RowLCR.DELETE:
//	        	session.transfer(flowFile,DELETES);
//	  			break;
//		      default:
//		        session.transfer(flowFile, UNMATCHED);
//		        break;
//    	}
//    	
//    	
//    	}catch(Exception ex){
//    		throw new ProcessException("error creating json message" +ex.getMessage());
//    	}
//    }
//    
//    protected final JsonObject convert(oracle.streams.ColumnValue columnValue) throws SQLException {
//        Datum datum = columnValue.getColumnData();
//
//        JsonObject column= new JsonObject();
//        if (null == datum) {
//        	return null;
//        }
//
//        Object value;
//        column.addProperty("name", columnValue.getColumnName());
//        column.addProperty("type", columnValue.getColumnDataType());
//        switch (columnValue.getColumnDataType()) {
//          case oracle.streams.ColumnValue.BINARY_DOUBLE:
//          	column.addProperty("value",datum.doubleValue());
//            break;
//          case oracle.streams.ColumnValue.BINARY_FLOAT:
//          	column.addProperty("value",datum.floatValue());
//            break;
//          case oracle.streams.ColumnValue.CHAR:
//          	column.addProperty("value",datum.stringValue());
//            break;
//          case oracle.streams.ColumnValue.DATE:
//          	column.addProperty("value",new Date(((DATE) datum).timestampValue(Calendar.getInstance()).getTime()).toGMTString());
//            break;
//          case oracle.streams.ColumnValue.NUMBER:
//          	column.addProperty("value",datum.bigDecimalValue());
//          	break;
////          case oracle.streams.ColumnValue.TIMESTAMPLTZ:
////            value = convertTimestampLTZ(changeKey, datum);
////            break;
////          case oracle.streams.ColumnValue.TIMESTAMPTZ:
////            value = convertTimestampTZ(changeKey, datum);
////            break;
//          default:
//          	column.addProperty("value", datum.stringValue());
//        }
//        return column;
//    }
    
    public Connection createConnection(ProcessContext context) throws Exception
    {
    	String dbHost = context.getProperty(DB_HOST).getValue();
    	String dbPort = context.getProperty(DB_PORT).getValue();
    	String dbUser = context.getProperty(DB_USER).getValue();
    	String dbPass = context.getProperty(DB_PASS).getValue();
    	String dbSid = context.getProperty(DB_SID).getValue();
    	
    	String dburl = "jdbc:oracle:oci:@"+dbHost+":"+dbPort+":"+dbSid;
   	 	final String urlString = context.getProperty(DB_DRIVER_LOCATION).evaluateAttributeExpressions().getValue();
   	 	setDriverClassLoader(urlString, "oracle.jdbc.OracleDriver");
        
		try
		{
		return DriverManager.getConnection(dburl, dbUser, dbPass);
		}
		catch(Exception e)
		{
		System.out.println("fail to establish DB connection to: " +dburl);
		e.printStackTrace();
		return null;
		}
    }
    
    /**
     * using Thread.currentThread().getContextClassLoader(); will ensure that you are using the ClassLoader for you NAR.
     *
     * @throws InitializationException
     *             if there is a problem obtaining the ClassLoader
     */
    private void setDriverClassLoader(String locationString, String drvName) throws InitializationException {
        if (locationString != null && locationString.length() > 0) {
            try {
                // Split and trim the entries
                final ClassLoader classLoader = ClassLoaderUtils.getCustomClassLoader(
                        locationString,
                        this.getClass().getClassLoader(),
                        (dir, name) -> name != null && name.endsWith(".jar")
                );

                // Workaround which allows to use URLClassLoader for JDBC driver loading.
                // (Because the DriverManager will refuse to use a driver not loaded by the system ClassLoader.)
                final Class<?> clazz = Class.forName(drvName, true, classLoader);
                if (clazz == null) {
                    throw new InitializationException("Can't load Database Driver " + drvName);
                }
                final Driver driver = (Driver) clazz.newInstance();
                DriverManager.registerDriver(new DriverShim(driver));

                this.driverClassLoader = classLoader;
            } catch (final MalformedURLException e) {
                throw new InitializationException("Invalid Database Driver Jar Url", e);
            } catch (final Exception e) {
                throw new InitializationException("Can't load Database Driver", e);
            }
        } else {
            // That will ensure that you are using the ClassLoader for you NAR.
             this.driverClassLoader = Thread.currentThread().getContextClassLoader();
        }
        
    }
    private Class<?> loadClass(String className) throws ClassNotFoundException{
    	return Class.forName(className,false,this.driverClassLoader);
    }
}
