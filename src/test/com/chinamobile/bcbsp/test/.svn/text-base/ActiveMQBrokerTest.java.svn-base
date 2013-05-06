/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test;

import java.net.BindException;
import java.net.ServerSocket;

import com.chinamobile.bcbsp.ActiveMQBroker;
import org.junit.Test;

public class ActiveMQBrokerTest {

    @Test(expected=BindException.class)
    public void testRun() throws Exception {
        ActiveMQBroker broker = new ActiveMQBroker("testBroker");
        broker.startBroker(60007);
        @SuppressWarnings("unused")
        ServerSocket s;    
        s = new ServerSocket(60007);
        broker.stopBroker();

    }

}
