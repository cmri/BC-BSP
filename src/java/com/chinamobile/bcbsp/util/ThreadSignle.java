/**
 * CopyRight by Chinamobile
 * 
 * ThreadSignle.java
 */
package com.chinamobile.bcbsp.util;
import org.apache.hadoop.io.BytesWritable;

import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;

/**
 * One "worker thread". Used to send data.
 */
public class ThreadSignle extends Thread {
	private int threadNumber;
	private boolean status = false;
	private BytesWritable data;
	WorkerAgentForStaffInterface worker = null;
	BSPJobID jobId = null;
	StaffAttemptID taskId = null;
	int belongPartition = -1;

	/**
	 * 
	 * @param g
	 * @param sn
	 */
	public ThreadSignle(ThreadGroup g, int sn) {
		super(g, "Thread #" + sn);
		threadNumber = sn;

	}

	/**
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		while (true) {
			while (!this.isStatus()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					return;
				}
			}
			worker.putHeadNode(jobId, taskId, belongPartition, this.data);
			this.data=null;			
			this.worker = null;
			this.jobId = null;
			this.taskId = null;
			this.belongPartition = -1;

			this.setStatus(false);

		}

	}

	public synchronized boolean isStatus() {
		return status;
	}

	public int getThreadNumber() {
		return this.threadNumber;
	}

	protected void kill() {
		this.interrupt();		
	}

	public synchronized void setStatus(boolean status) {
		this.status = status;
	}

	public void setData(BytesWritable data) {
		this.data = data;
	}

	public void setWorker(WorkerAgentForStaffInterface worker) {
		this.worker = worker;
	}

	public void setJobId(BSPJobID jobId) {
		this.jobId = jobId;
	}

	public void setTaskId(StaffAttemptID taskId) {
		this.taskId = taskId;
	}

	public void setBelongPartition(int belongPartition) {
		this.belongPartition = belongPartition;
	}
}
