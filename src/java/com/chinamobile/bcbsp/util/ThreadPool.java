/**
 * CopyRight by Chinamobile
 * 
 * ThreadPool.java
 */
package com.chinamobile.bcbsp.util;

/**
 * 
 * Thread Pool. The thread in this is used to send graph data.
 * 
 */
public class ThreadPool extends ThreadGroup {
	
	public static int DEFAULT_TOE_PRIORITY = Thread.NORM_PRIORITY - 1;
	protected int nextSerialNumber = 0;
	protected int targetSize = 0;

	/**
	 * Initialize the thread pool. There is threadNum threads in the pool.
	 * 
	 * @param threadNum
	 */
	public ThreadPool(int threadNum) {
		super("ToeThreads");
		for (int i = 0; i < threadNum; i++) {
			this.startNewThread();
		}
	}

	/**
	 * Close the thread pool.
	 */
	public void cleanup() {
		Thread[] toes = getAllThread();
		for (int i = 0; i < toes.length; i++) {
			if (!(toes[i] instanceof ThreadSignle)) {
				continue;
			}
			ThreadSignle t = (ThreadSignle) toes[i];
			while (t.isStatus()) {

			}
			killThread(t);
		}
	}

	/**
	 * @return The number of ThreadSignle that are available.
	 */
	public int getActiveToeCount() {
		Thread[] toes = getAllThread();
		int count = 0;
		for (int i = 0; i < toes.length; i++) {
			if ((toes[i] instanceof ThreadSignle)
					&& ((ThreadSignle) toes[i]).isAlive()) {
				count++;
			}
		}
		return count;
	}

	/**
	 * @return The number of ThreadSignle. This may include killed Threads that
	 *         were not replaced.
	 */
	public int getToeCount() {
		Thread[] toes = getAllThread();
		int count = 0;
		for (int i = 0; i < toes.length; i++) {
			if ((toes[i] instanceof ThreadSignle)) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Obtain a free ThreadSignle to send data.
	 * 
	 * @return
	 */
	public ThreadSignle getThread() {
		Thread[] toes = getAllThread();
		for (int i = 0; i < toes.length; i++) {
			if (!(toes[i] instanceof ThreadSignle)) {
				continue;
			}
			ThreadSignle toe = (ThreadSignle) toes[i];
			if (!toe.isStatus())
				return toe;
		}
		return null;

	}

	/**
	 * get all threads in pool.
	 * 
	 * @return
	 */
	private Thread[] getAllThread() {
		Thread[] toes = new Thread[activeCount()];
		this.enumerate(toes);
		return toes;
	}

	/**
	 * Create a new threadSignle
	 */
	private synchronized void startNewThread() {
		ThreadSignle newThread = new ThreadSignle(this, this.nextSerialNumber++);
		newThread.setPriority(DEFAULT_TOE_PRIORITY);
		newThread.start();
	}

	/**
	 * Kills specified thread.
	 * 
	 * @param id
	 */
	public void killThread(ThreadSignle t) {
		t.kill();
	}
}
