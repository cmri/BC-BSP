/**
 * CopyRight by Chinamobile
 * 
 * WorkerManagerControlInterface.java
 */
package com.chinamobile.bcbsp.workermanager;

import java.util.Collection;

import com.chinamobile.bcbsp.rpc.WorkerManagerProtocol;
import com.chinamobile.bcbsp.bspcontroller.ClusterStatus;
import com.chinamobile.bcbsp.bspcontroller.JobInProgressListener;

/**
 * Manages information about the {@link WorkerManager}s in the cluster
 * environment. This interface is not intended to be implemented by users.
 * 
 * @author
 * @version
 */
public interface WorkerManagerControlInterface {

    /**
     * Get the current status of the cluster
     * 
     * @param detailed
     *            if true then report workerManager names as well
     * @return summary of the state of the cluster
     */
    ClusterStatus getClusterStatus(boolean detailed);

    /**
     * Find WorkerManagerProtocol with corresponded workerManager server status
     * 
     * @param status
     *            WorkerManagerStatus
     * @return WorkerManagerProtocol
     */
    WorkerManagerProtocol findWorkerManager(WorkerManagerStatus status);

    /**
     * Find the collection of workerManager servers.
     * 
     * @return Collection of workerManager servers list.
     */
    Collection<WorkerManagerProtocol> findWorkerManagers();

    /**
     * Collection of WorkerManagerStatus as the key set.
     * 
     * @return Collection of WorkerManagerStatus.
     */
    Collection<WorkerManagerStatus> workerServerStatusKeySet();

    /**
     * Registers a JobInProgressListener to WorkerManagerControlInterface.
     * Therefore, adding a JobInProgress will trigger the jobAdded function.
     * 
     * @param the
     *            JobInProgressListener listener to be added.
     */
    void addJobInProgressListener(JobInProgressListener listener);

    /**
     * Unregisters a JobInProgressListener to WorkerManagerControlInterface.
     * Therefore, the remove of a JobInProgress will trigger the jobRemoved
     * action.
     * 
     * @param the
     *            JobInProgressListener to be removed.
     */
    void removeJobInProgressListener(JobInProgressListener listener);

    /**
     * Update the WorkerManagerStatus
     * Cache(now it is used in SimpleStaffScheduler and BSPController)
     * 
     * @param old
     *            the original WorkerManagerStatus, it will be replaced by the
     *            new WorkerManagerStatus.
     * @param new the new WorkerManagerStatus.
     */
    void updateWhiteWorkerManagersKey(WorkerManagerStatus old,
            WorkerManagerStatus newKey);

    /**
     * Current WorkerManager.
     * 
     * @return return WorkerManagersName.
     */
    String[] getActiveWorkerManagersName();
    
    public WorkerManagerProtocol removeWorkerFromWhite(WorkerManagerStatus wms);
    
    public void addWorkerToGray(WorkerManagerStatus wms, WorkerManagerProtocol wmp);
    public int getMaxFailedJobOnWorker();
}
