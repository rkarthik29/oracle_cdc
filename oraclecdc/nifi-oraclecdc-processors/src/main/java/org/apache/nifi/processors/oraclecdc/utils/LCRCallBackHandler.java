package org.apache.nifi.processors.oraclecdc.utils;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class LCRCallBackHandler implements InvocationHandler {

	final ProcessContext context;
	final ProcessSession session;
	byte[] finalPosition;
	boolean moveWatermark=false;
	private ClassLoader classLoader;
	private ProcessLCR processLCR;

	public LCRCallBackHandler( ProcessContext context, ProcessSession session,ClassLoader classLoader) {
		// TODO Auto-generated constructor stub
		this.context=context;
		this.session=session;
		this.classLoader=classLoader;
		this.processLCR=new ProcessLCR(classLoader);
	}
	
	public Object createChunk() throws Throwable {
		// TODO Auto-generated method stub
		return null;
	}

	public Object createLCR() throws Throwable {
		// TODO Auto-generated method stub
		return null;
	}

	public void processChunk(Object arg0) throws Throwable {
		// TODO Auto-generated method stub
		System.out.println("in process chunk");
	}


	public void processLCR(Object alcr) throws Throwable {
		// TODO Auto-generated method stub
		System.out.println("in process lcr");
          this.finalPosition=processLCR.lcr2ff(alcr,this.context,this.session);
          this.moveWatermark=true;
	}
	
	public byte[] getFinalPosition(){
		return this.finalPosition;
	}
	
	public boolean isMoveWaterMark(){
		return this.moveWatermark;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		// TODO Auto-generated method stub
		switch(method.getName()){
		case "processLCR":
			if (Class.forName("oracle.streams.RowLCR",false,this.classLoader).isAssignableFrom(args[0].getClass()))
				processLCR(args[0]);
			break;
		}
		return null;
	}

}
