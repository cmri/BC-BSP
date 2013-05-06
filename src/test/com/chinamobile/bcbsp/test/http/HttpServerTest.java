/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.http;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.http.HttpServer;

public class HttpServerTest {
    
    HttpServer hs ;
    public void setUp() throws IOException, InterruptedException {
       
        hs = new HttpServer("httpserver","bindaddress",555,true);
        hs.setAttribute("mayue",hs);
        hs.setThreads(10, 100);
       
      
    }
    

    @Test
    @Ignore("HttpServerStringStringIntBoolean(): to be tested")
    public void testHttpServerStringStringIntBoolean() {
       
    }

    @Test
    @Ignore("HttpServerStringStringIntBooleanConfiguration(): to be tested")
    public void testHttpServerStringStringIntBooleanConfiguration() {
        
    }

    @Test
    @Ignore("CreateBaseListener(): to be tested")
    public void testCreateBaseListener() {
//        SelectChannelConnector ret = new SelectChannelConnector();
//        ret.setLowResourceMaxIdleTime(10000);
//        ret.setAcceptQueueSize(128);
//        ret.setResolveNames(false);
//        ret.setUseDirectBuffers(false);
//        
        
    }

    @Test
    @Ignore("AddDefaultApps(): to be tested")
    public void testAddDefaultApps() {
       
    }

    @Test
    @Ignore("AddDefaultServlets(): to be tested")
    public void testAddDefaultServlets() {
       
    }

    @Test
    @Ignore("AddContextContextBoolean(): to be tested")
    public void testAddContextContextBoolean() {
        
    }

    @Test
    @Ignore("AddContextStringStringBoolean(): to be tested")
    public void testAddContextStringStringBoolean() {
        
    }

    @Test
    @Ignore("SetAttribute(): to be tested")
    public void testSetAttribute() {
        
    }

    @Test
    @Ignore("AddServlet(): to be tested")
    public void testAddServlet() {
      
    }

    @Test
    @Ignore("AddInternalServlet(): to be tested")
    public void testAddInternalServlet() {
        
    }

    @Test
    @Ignore("DefineFilter(): to be tested")
    public void testDefineFilter() {
        
    }

    @Test
    @Ignore("AddFilterPathMapping(): to be tested")
    public void testAddFilterPathMapping() {
       
    }

    @Test
    
    public void testGetAttribute() {
        //fail("Not yet implemented");
       
        
    }

    @Test
    @Ignore("GetWebAppsPath(): to be tested")
    public void testGetWebAppsPath() {
        
    }

    @Test
    @Ignore("GetPort(): to be tested")
    public void testGetPort() {
        
    }

    @Test
    @Ignore("SetThreads(): to be tested")
    public void testSetThreads() {
       
        
    }

    @Test
    @Ignore("AddSslListenerInetSocketAddressStringStringString(): to be tested")
    public void testAddSslListenerInetSocketAddressStringStringString() {
        
    }

    @Test
    @Ignore("AddSslListenerInetSocketAddressConfigurationBoolean(): to be tested")
    public void testAddSslListenerInetSocketAddressConfigurationBoolean() {
        
    }

    @Test
    @Ignore("Start(): to be tested")
    public void testStart() {
        
    }

    @Test
    @Ignore("Stop(): to be tested")
    public void testStop() {
        
    }

    @Test
    @Ignore("Join(): to be tested")
    public void testJoin() {
        
    }

}
