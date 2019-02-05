package org.apache.nifi.processors.oraclecdc.utils;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Calendar;

public class DatumParser{

	private Object datum;
	private Class<?> datumCls;
	private ClassLoader classLoader;
	public DatumParser(Object datum,ClassLoader classLoader) throws Exception{
		this.datum=datum;
		this.datumCls=Class.forName("oracle.sql.Datum",false,classLoader);
		this.classLoader=classLoader;
	}
	
	public double doubleValue() throws Exception{
		Method method = datumCls.getMethod("doubleValue");
		return (double)method.invoke(datum);
	}
	
	public String stringValue() throws Exception{
		Method method = datumCls.getMethod("stringValue");
		return (String)method.invoke(datum);
	}
	
	public float floatValue() throws Exception{
		Method method = datumCls.getMethod("floatValue");
		return (float)method.invoke(datum);
	}
	
	public BigDecimal bigDecimalValue() throws Exception{
		Method method = datumCls.getMethod("bigDecimalValue");
		return (BigDecimal)method.invoke(datum);
	}
	
	public long timeStampValue() throws Exception{
		Method method = datumCls.getMethod("timeStampValue");
		Timestamp ts = (Timestamp)method.invoke(datum);
		return ts.getTime();
	}
	
	public long timeStampValue(Calendar cal) throws Exception{
		Method method = datumCls.getMethod("timeStampValue",Calendar.class);
		Timestamp ts = (Timestamp)method.invoke(datum,cal);
		return ts.getTime();
	}

}
