/**
 * CopyRight by Chinamobile
 * 
 * ActiveMQBroker.java
 */
package com.chinamobile.bcbsp;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;

public final class ActiveMQBroker {
    
    BrokerService broker;
    
    String brokerName = "localhost";
    
    public ActiveMQBroker(String brokerName) {
        this.brokerName = brokerName;
    }

    public static void main(String[] args) throws Exception {
        
        String hostName = args[0];
        
        BrokerService broker = new BrokerService();
        
        broker.setBrokerName(hostName);
        broker.setDataDirectory("activemq_data/");
        broker.setUseJmx(true);
        
        broker.setPersistent(false);
        broker.addConnector("tcp://0.0.0.0:61616");
        
        broker.start();
        
        System.out.println("Start broker successfully!");

        // now lets wait forever to avoid the JVM terminating immediately
        Object lock = new Object();
        synchronized (lock) {
            lock.wait();
        }
        
        System.out.println("Run over.");
    }
    
    public void startBroker(int port) throws Exception {
        
        broker = new BrokerService();
        
        broker.setBrokerName(this.brokerName);
        broker.setUseJmx(true);
        broker.setPersistent(false);
        broker.getSystemUsage().getMemoryUsage().setLimit(512*1024*1024);
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry policy = new PolicyEntry();
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long max = memoryUsage.getMax();
        long memoryLimit = max/2;
        policy.setMemoryLimit(memoryLimit);
        policy.setQueue("BSP");
        entries.add(policy);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policy);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);
        
        String connectortUri = "tcp://0.0.0.0:" + port;
        
        broker.addConnector(connectortUri);
        
        broker.start();
    }
    
    public void stopBroker() throws Exception {
        
        if (broker != null) {
            broker.stop();
        }
    }
}
