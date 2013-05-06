/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.mini;

import java.io.IOException;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.util.BSPJobID;

public class MiniBSPController extends BSPController {
    
    private Server controllerServer;
    
    public MiniBSPController(BSPConfiguration conf) throws Exception {
        super();
        try {
            String host = getAddress(conf).getHostName();
            int port = getAddress(conf).getPort();
            this.controllerServer = RPC.getServer(this, host, port, conf);
            this.controllerServer.start();
        } catch (Exception e) {
        }
    }
    
    @Override
    public void recordFault(Fault f) {
        System.out.println(f.toString());
    }
    
    @Override
    public boolean recovery(BSPJobID jobId) {
        return true;
    }
    
    @Override
    public void killJob(BSPJobID jobId) throws IOException {
        System.out.println("Kill job " + jobId.toString());
    }
}
