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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class TestOracleChangeCapture {

    private TestRunner testRunner;
    
    @Before
    public void setup() throws InitializationException{
        testRunner = TestRunners.newTestRunner(OracleChangeCapture.class);
        //final DBCPService dbcp = new DBCPServiceSimpleImpl();
        //final Map<String, String> dbcpProperties = new HashMap<>();

        //testRunner.addControllerService("dbcp", dbcp, dbcpProperties);
        //testRunner.enableControllerService(dbcp);
        testRunner.setProperty(OracleChangeCapture.DB_HOST,"localhost");
        testRunner.setProperty(OracleChangeCapture.DB_PORT,"32776");
        testRunner.setProperty(OracleChangeCapture.DB_USER,"xstrmadmin");
        testRunner.setProperty(OracleChangeCapture.DB_PASS,"welcome1");
        testRunner.setProperty(OracleChangeCapture.DB_SID,"orcl");
        testRunner.setProperty(OracleChangeCapture.DB_DRIVER_LOCATION,"/Users/knarayanan/Downloads/instantclient_12_2/ojdbc8.jar,"
        		+ "/Users/knarayanan/Downloads/instantclient_12_2/xstreams.jar");
    }

    @Test
    public void testProcessor() {
    	
    	testRunner.setProperty(OracleChangeCapture.XS_OUT, "xout1");
    	//testRunner.setRunSchedule(5000);
    	testRunner.run(1);
    	//testRunner.shutdown();
    	
    }
    
//    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {
//
//        @Override
//        public String getIdentifier() {
//            return "dbcp";
//        }
//
//        @Override
//        public Connection getConnection() throws ProcessException {
//        	String host="oracledb.cyfepzqonej8.us-west-2.rds.amazonaws.com";
//        	String port="1521";
//        	String orasid="orcl";
//        	String xstreamuser = "xstream_admin";
//        	xstreamuser="xstrmadmin";
//        	host="localhost";
//        	port="32770";
//            String out_url = "jdbc:oracle:oci:@"+host+":"+port+":"+orasid;
//            try {
//                Class.forName("oracle.jdbc.OracleDriver");
//                final Connection con = DriverManager.getConnection(out_url,xstreamuser,"welcome1");
//                return con;
//            } catch (final Exception e) {
//                throw new ProcessException("getConnection failed: " + e);
//            }
//        }
//    }

}
