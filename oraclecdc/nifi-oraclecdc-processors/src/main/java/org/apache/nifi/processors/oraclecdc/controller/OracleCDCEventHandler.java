package org.apache.nifi.processors.oraclecdc.controller;

public interface OracleCDCEventHandler {
	
	public void inserts(String events,byte[] position);
	
	public void updates(String events,byte[] position);
	
	public void deletes(String events,byte[] position);
	
	public void other(String events,byte[] position);

}
