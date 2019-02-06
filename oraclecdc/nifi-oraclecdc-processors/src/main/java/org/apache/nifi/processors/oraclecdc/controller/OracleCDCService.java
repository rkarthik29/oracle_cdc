package org.apache.nifi.processors.oraclecdc.controller;

import org.apache.nifi.controller.ControllerService;

public interface OracleCDCService extends ControllerService {
	
	
	public Object attach (String xstreamOutServerName,byte[] lastPostion);

	public void receiveEvents(Object xsOutServer, OracleCDCEventHandler handler);
	
	public void detach(Object xsOutServer);
	
	public void setProcessedLowWaterMark(Object xsOutServer,byte[] position);
}
