/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.bspcontroller;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import junit.framework.TestCase;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.rpc.WorkerManagerProtocol;
import com.chinamobile.bcbsp.test.BSPConfigurationSetter;
import com.chinamobile.bcbsp.test.TestUtil;
import com.chinamobile.bcbsp.util.StaffStatus;
import com.chinamobile.bcbsp.workermanager.WorkerManager;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

public class BSPControllerTest extends TestCase {

    BSPController controller = null;
    BSPConfiguration conf = null;
    BSPConfigurationSetter bspconfSetter = null;
    WorkerManager workermanager = null;

    @SuppressWarnings("static-access")
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        if (conf == null) {
            conf = new BSPConfiguration();
            bspconfSetter = new BSPConfigurationSetter();

            bspconfSetter.mkTestDir("bcbsp_test");
            conf.set(Constants.BC_BSP_CONTROLLER_ADDRESS, "127.0.0.1:"
                    + bspconfSetter.getFreePort(40000));
            conf.set(Constants.BC_BSP_SHARE_DIRECTORY,
                    new File(bspconfSetter.getBcbsptestDir(), "share")
                            .toString());
            conf.set(Constants.BC_BSP_WORKERMANAGER_RPC_HOST, "127.0.0.1");
            conf.set(Constants.BC_BSP_WORKERMANAGER_RPC_PORT, ""
                    + bspconfSetter.getFreePort(40000));
        }

        if (controller == null) {
            controller = new BSPController();
            TestUtil.set(controller, "conf", conf);
        }
    }

    @After
    protected void tearDown() throws Exception {
        super.tearDown();
        // controller.shutdown();
        bspconfSetter.cleanUp();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAddWorkerToGray() throws Exception {
        WorkerManagerStatus wms = new WorkerManagerStatus("127.0.0.1",
                new ArrayList<StaffStatus>(), 1, 0, 0, 0);

        ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> grayWorkerManagers = null;
        try {
            grayWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "grayWorkerManagers");
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(false, grayWorkerManagers.containsKey(wms));

        controller.addWorkerToGray(wms,
                TestUtil.createDonothingObject(WorkerManagerProtocol.class));

        assertEquals(true, grayWorkerManagers.containsKey(wms));

        grayWorkerManagers.clear();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRegister() throws Exception {

        int port = Integer.valueOf(conf
                .get(Constants.BC_BSP_WORKERMANAGER_RPC_PORT));
        workermanager = new WorkerManager(conf);
        Server testServer = RPC.getServer(workermanager,
                conf.get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST), port, conf);
        testServer.start();

        WorkerManagerStatus wms = new WorkerManagerStatus(
                conf.get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST),
                new ArrayList<StaffStatus>(), 1, 0, 0, 0,
                conf.get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST) + ":" + port,
                new ArrayList<Fault>());

        Map<WorkerManagerStatus, WorkerManagerProtocol> whiteWorkerManagers = null;
        try {
            whiteWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "whiteWorkerManagers");
        } catch (Exception e) {
            System.out.println("get Attribute Exception.");
            e.printStackTrace();
        }
        assertEquals(false, whiteWorkerManagers.containsKey(wms));

        assertEquals(true, whiteWorkerManagers.containsKey(wms));

        whiteWorkerManagers.clear();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUpdateWhiteWorkerManagersKey() {
        WorkerManagerStatus oldone = new WorkerManagerStatus("old",
                new ArrayList<StaffStatus>(), 1, 0, 0, 0);
        WorkerManagerStatus newone = new WorkerManagerStatus("new",
                new ArrayList<StaffStatus>(), 1, 0, 0, 0);

        Map<WorkerManagerStatus, WorkerManagerProtocol> whiteWorkerManagers = null;
        try {
            whiteWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "whiteWorkerManagers");
        } catch (Exception e) {
            e.printStackTrace();
        }

        whiteWorkerManagers.put(oldone,
                TestUtil.createDonothingObject(WorkerManagerProtocol.class));

        assertEquals(true, whiteWorkerManagers.containsKey(oldone));
        assertEquals(false, whiteWorkerManagers.containsKey(newone));

        controller.updateWhiteWorkerManagersKey(oldone, newone);

        assertEquals(false, whiteWorkerManagers.containsKey(oldone));
        assertEquals(true, whiteWorkerManagers.containsKey(newone));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUpdateGrayWorkerManagersKey() {
        WorkerManagerStatus oldone = new WorkerManagerStatus("old",
                new ArrayList<StaffStatus>(), 1, 0, 0, 0);
        oldone.setPauseTime(100l);
        WorkerManagerStatus newone = new WorkerManagerStatus("new",
                new ArrayList<StaffStatus>(), 1, 0, 0, 0);
        newone.setPauseTime(200l);

        Map<WorkerManagerStatus, WorkerManagerProtocol> grayWorkerManagers = null;
        try {
            grayWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "grayWorkerManagers");
        } catch (Exception e) {
            e.printStackTrace();
        }

        grayWorkerManagers.put(oldone,
                TestUtil.createDonothingObject(WorkerManagerProtocol.class));

        assertEquals(true, grayWorkerManagers.containsKey(oldone));
        assertEquals(false, grayWorkerManagers.containsKey(newone));

        controller.updateGrayWorkerManagersKey(oldone, newone);

        assertEquals(false, grayWorkerManagers.containsKey(oldone));
        assertEquals(true, grayWorkerManagers.containsKey(newone));
        assertEquals(100l, newone.getPauseTime());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReport() {
        WorkerManagerStatus wms = new WorkerManagerStatus("127.0.0.1",
                new ArrayList<StaffStatus>(), 1, 0, 0, 0);
        wms.setLastSeen(100l);
        ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> whiteWorkerManagers = null, grayWorkerManagers = null;
        try {
            whiteWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "whiteWorkerManagers");
            grayWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "grayWorkerManagers");
        } catch (Exception e) {
            e.printStackTrace();
        }
        whiteWorkerManagers.put(wms,
                TestUtil.createDonothingObject(WorkerManagerProtocol.class));
        assertEquals(100l, wms.getLastSeen());

        try {
            controller.report(new Directive(wms));
        } catch (Exception e) {
            System.out.println("Processing");
        }

        assertEquals(true, whiteWorkerManagers.containsKey(wms));
        assertEquals(false, grayWorkerManagers.containsKey(wms));
        assertEquals(true, wms.getLastSeen() != 100l);

        whiteWorkerManagers.clear();
        grayWorkerManagers.clear();

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFindWorkerManager() {
        WorkerManagerStatus wms = new WorkerManagerStatus(
                conf.get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST),
                new ArrayList<StaffStatus>(), 1, 0, 0, 0,
                conf.get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST) + ":"
                        + conf.get(Constants.BC_BSP_WORKERMANAGER_RPC_PORT),
                new ArrayList<Fault>());
        ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> whiteWorkerManagers = null;
        try {
            whiteWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "whiteWorkerManagers");
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertNull(controller.findWorkerManager(wms));

        whiteWorkerManagers.put(wms,
                TestUtil.createDonothingObject(WorkerManagerProtocol.class));

        assertNotNull(controller.findWorkerManager(wms));

        whiteWorkerManagers.clear();

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFindWorkerManagers() {

        ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> whiteWorkerManagers = null;
        try {
            whiteWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "whiteWorkerManagers");
        } catch (Exception e) {
            e.printStackTrace();
        }
        Collection<WorkerManagerProtocol> managers = controller
                .findWorkerManagers();
        assertEquals(true, managers == whiteWorkerManagers.values());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWorkerServerStatusKeySet() {

        ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> whiteWorkerManagers = null;
        try {
            whiteWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "whiteWorkerManagers");
        } catch (Exception e) {
            e.printStackTrace();
        }
        Collection<WorkerManagerStatus> wmses = controller
                .workerServerStatusKeySet();
        assertEquals(true, wmses == whiteWorkerManagers.keySet());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetActiveWorkerManagersName() {

        int i = 2, j = 3;
        WorkerManagerStatus[] wmses = new WorkerManagerStatus[i + j];
        String[] names = new String[i + j];
        for (int k = 0; k < i + j; k++) {
            names[k] = "test" + (k < i ? "gray-" + k : "white-" + (k - i));
            wmses[k] = new WorkerManagerStatus(names[k],
                    new ArrayList<StaffStatus>(), 1, 0, 0, 0);
        }

        ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> whiteWorkerManagers = null, grayWorkerManagers = null, tmp = null;
        try {
            whiteWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "whiteWorkerManagers");
            grayWorkerManagers = ( ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> ) TestUtil
                    .get(controller, "grayWorkerManagers");
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(0, whiteWorkerManagers.size());
        assertEquals(0, grayWorkerManagers.size());

        for (int k = 0; k < i + j; k++) {
            tmp = (k < i ? grayWorkerManagers : whiteWorkerManagers);
            tmp.put(wmses[k], ( WorkerManagerProtocol ) TestUtil
                    .createDonothingObject(WorkerManagerProtocol.class));
        }
        
        String[] checks = controller.getActiveWorkerManagersName();
        Arrays.sort(checks);
        Arrays.sort(names);
        for(int k = 0; k < i + j; k++){
            assertEquals(true, checks[k].equals(names[k]));
        }
        
        whiteWorkerManagers.clear();
        grayWorkerManagers.clear();

    }

    @SuppressWarnings("static-access")
    public static void main(String[] args) {
        @SuppressWarnings("unused")
        WorkerManager workermanager = null;
        BSPConfiguration conf = new BSPConfiguration();
        BSPController controller = null;
        BSPConfigurationSetter bspconfSetter = new BSPConfigurationSetter();

        bspconfSetter.mkTestDir("bcbsp_test");
        conf.set(Constants.BC_BSP_CONTROLLER_ADDRESS, "127.0.0.1:"
                + bspconfSetter.getFreePort(40000));
        conf.set(Constants.BC_BSP_SHARE_DIRECTORY,
                new File(bspconfSetter.getBcbsptestDir(), "share").toString());
        try {
            workermanager = new WorkerManager(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        WorkerManagerStatus wms = new WorkerManagerStatus("TEST",
                new ArrayList<StaffStatus>(), 1, 0, 0, 0, "127.0.0.1:0");

        try {
            controller = new BSPController(conf);
            controller.register(wms);
        } catch (Exception e) {
            e.printStackTrace();
        }

        bspconfSetter.cleanUp();
    }
}
