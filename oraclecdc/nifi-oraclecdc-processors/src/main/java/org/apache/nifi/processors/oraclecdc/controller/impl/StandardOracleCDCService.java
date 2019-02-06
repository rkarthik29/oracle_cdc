package org.apache.nifi.processors.oraclecdc.controller.impl;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.oraclecdc.controller.OracleCDCEventHandler;
import org.apache.nifi.processors.oraclecdc.controller.OracleCDCService;
import org.apache.nifi.processors.oraclecdc.utils.LCRCallBackHandler;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

public class StandardOracleCDCService extends AbstractControllerService implements OracleCDCService {

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
	    
	    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
	            .name("Max Total Connections")
	            .description("The maximum number of active connections that can be allocated from this pool at the same time, "
	                + " or negative for no limit.")
	            .defaultValue("8")
	            .required(true)
	            .addValidator(StandardValidators.INTEGER_VALIDATOR)
	            .sensitive(false)
	            .build();
	    
	    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
	            .displayName("Minimum Idle Connections")
	            .name("dbcp-min-idle-conns")
	            .description("The minimum number of connections that can remain idle in the pool, without extra ones being " +
	                    "created, or zero to create none.")
	            .defaultValue("0")
	            .required(false)
	            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
	            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
	            .build();

	    public static final PropertyDescriptor MAX_IDLE = new PropertyDescriptor.Builder()
	            .displayName("Max Idle Connections")
	            .name("dbcp-max-idle-conns")
	            .description("The maximum number of connections that can remain idle in the pool, without extra ones being " +
	                    "released, or negative for no limit.")
	            .defaultValue("8")
	            .required(false)
	            .addValidator(StandardValidators.INTEGER_VALIDATOR)
	            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
	            .build();
	
	    private static final List<PropertyDescriptor> properties;

	    protected ClassLoader driverClassLoader;
	    static {
	        final List<PropertyDescriptor> props = new ArrayList<>();
	        props.add(DB_HOST);
	        props.add(DB_PORT);
	        props.add(DB_USER);
	        props.add(DB_PASS);
	        props.add(DB_SID);
	        props.add(DB_DRIVER_LOCATION);
	        props.add(MAX_TOTAL_CONNECTIONS);
	        props.add(MAX_IDLE);
	        props.add(MIN_IDLE);

	        properties = Collections.unmodifiableList(props);
	    }

	    private volatile BasicDataSource dataSource;
	    //private volatile String dbUrl;
	    
	    @Override
	    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
	        return properties;
	    }

	    @Override
	    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
	        return new PropertyDescriptor.Builder()
	                .name(propertyDescriptorName)
	                .required(false)
	                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
	                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
	                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
	                .dynamic(true)
	                .build();
	    }
	
	    /**
	     * @param context
	     *            the configuration context
	     * @throws InitializationException
	     *             if unable to create a database connection
	     */
	    @OnEnabled
	    public void onEnabled(final ConfigurationContext context) throws InitializationException {
	    	final String host = context.getProperty(DB_HOST).getValue();
	    	final String port = context.getProperty(DB_PORT).getValue();
	        final String user = context.getProperty(DB_USER).getValue();
	        final String passw = context.getProperty(DB_PASS).getValue();
	        final String dbSid = context.getProperty(DB_SID).getValue();
	        final String urlString = context.getProperty(DB_DRIVER_LOCATION).getValue();
	        final String drv="oracle.jdbc.OracleDriver";
	        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).asInteger();
	        final Integer maxIdle = context.getProperty(MAX_IDLE).asInteger();
	        final Integer miIdle = context.getProperty(MIN_IDLE).asInteger();
	        dataSource = new BasicDataSource();
	        dataSource.setAccessToUnderlyingConnectionAllowed(true);
	        dataSource.setDriverClassName(drv);

	        // Optional driver URL, when exist, this URL will be used to locate driver jar file location
	        dataSource.setDriverClassLoader(getDriverClassLoader(urlString, drv));

	        final String dburl = "jdbc:oracle:oci:@"+host+":"+port+":"+dbSid;
//	        dataSource.setMaxWaitMillis(maxWaitMillis);
	        dataSource.setMaxTotal(maxTotal);
	        dataSource.setMinIdle(1);
	        dataSource.setMaxIdle(maxTotal);
//	        dataSource.setMaxConnLifetimeMillis(maxConnLifetimeMillis);
//	        dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
//	        dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
//	        dataSource.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);

	        dataSource.setUrl(dburl);
	        dataSource.setUsername(user);
	        dataSource.setPassword(passw);

	        context.getProperties().keySet().stream().filter(PropertyDescriptor::isDynamic)
	                .forEach((dynamicPropDescriptor) -> dataSource.addConnectionProperty(dynamicPropDescriptor.getName(),
	                        context.getProperty(dynamicPropDescriptor).evaluateAttributeExpressions().getValue()));
	    	
	    }
	
	@Override
	public void receiveEvents(Object xsOut, OracleCDCEventHandler handler) {
		if (null == xsOut)
        {
          getLogger().info("xstreamOut is null");
          return;
        }
     
        try
        {
          	  LCRCallBackHandler hdlr =  new LCRCallBackHandler(this.driverClassLoader,handler);
        	  Object proxy=Proxy.newProxyInstance(this.driverClassLoader,
        			new Class[] {loadClass("oracle.streams.XStreamLCRCallbackHandler")},hdlr);
          	  Class<?> xstreamOut = loadClass("oracle.streams.XStreamOut");
          	  Method method = xstreamOut.getMethod("receiveLCRCallback", loadClass("oracle.streams.XStreamLCRCallbackHandler"),int.class);
          	  method.invoke(xsOut, proxy,loadClass("oracle.streams.XStreamOut").getDeclaredField("DEFAULT_MODE").getInt(null));
          	  System.out.print("done");
        }
        catch(Exception e)
        { 
          e.printStackTrace();
          getLogger().warn("exception when processing LCRs");
          getLogger().warn(e.getMessage());
          throw new ProcessException("exception when processing LCRs "+e.getMessage());
        }
	}
	
	
	@Override
	public void setProcessedLowWaterMark(Object xsOutServer,byte[] position){
		try{
		System.out.println(new String(new Base32(true).encode(position)));
		Class<?> xstreamOut = loadClass("oracle.streams.XStreamOut");
  		Method method = xstreamOut.getMethod("setProcessedLowWatermark",byte[].class, int.class);
      	method.invoke(xsOutServer,position,loadClass("oracle.streams.XStreamOut").getDeclaredField("DEFAULT_MODE").getInt(null));
		}catch(Exception e)
        { 
	          e.printStackTrace();
	          getLogger().warn("exception when set low watermark");
	          getLogger().warn(e.getMessage());
	          throw new ProcessException("exception when setting low water mark "+e.getMessage());
	        }
	}
	
	
	
	@Override
	public Object attach(String xsOutName,byte[] lastPosition){
    	getLogger().info("in attach");
   	 try
   	    {
   		 final Connection conn = getConnection();
   		 Class<?> xstreamOut = loadClass("oracle.streams.XStreamOut");
   		 Method method = xstreamOut.getMethod("attach", loadClass("oracle.jdbc.OracleConnection"),String.class,byte[].class,int.class);
   		 Object xsOut = method.invoke(null,conn.unwrap(loadClass("oracle.jdbc.OracleConnection")),xsOutName,lastPosition, 
   				 xstreamOut.getDeclaredField("DEFAULT_MODE").getInt(null)); 
   		 return xsOut;
   	    }
   	    catch(Exception e)
   	    {
   	      System.out.println("cannot attach to outbound server: "+xsOutName);
   	      getLogger().error(e.getMessage());
   	      e.printStackTrace();
   	      throw new ProcessException("cannot attach to outbound server: "+xsOutName);
   	    }
    }
	
	@Override
	public void detach(Object xsOut) {
		try
        {
    	  Class xstreamOut = loadClass("oracle.streams.XStreamOut");
    	  Method method = xstreamOut.getDeclaredMethod("detach", int.class);
    	  method.invoke(xsOut, xstreamOut.getDeclaredField("DEFAULT_MODE").getInt(null));
        }
        catch(Exception e)
        {
          getLogger().info("cannot detach from the outbound server: ");
          throw new ProcessException("cannot detach from the outbound server: ");
        }
		
	}
	
    private Connection getConnection() throws ProcessException {
        try {
            final Connection con = dataSource.getConnection();
            return con;
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }
	
	/**
     * using Thread.currentThread().getContextClassLoader(); will ensure that you are using the ClassLoader for you NAR.
     *
     * @throws InitializationException
     *             if there is a problem obtaining the ClassLoader
     */
    private ClassLoader getDriverClassLoader(String locationString, String drvName) throws InitializationException {
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

                this.driverClassLoader=classLoader;
                return classLoader;
            } catch (final MalformedURLException e) {
                throw new InitializationException("Invalid Database Driver Jar Url", e);
            } catch (final Exception e) {
                throw new InitializationException("Can't load Database Driver", e);
            }
        } else {
            // That will ensure that you are using the ClassLoader for you NAR.
        	this.driverClassLoader=Thread.currentThread().getContextClassLoader();
             return Thread.currentThread().getContextClassLoader();
        }
        
    }
    private Class<?> loadClass(String className) throws ClassNotFoundException{
    	return Class.forName(className,false,this.driverClassLoader);
    }


}
