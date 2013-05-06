/**
 * CopyRight by Chinamobile
 * 
 * MessageService.java
 */
package com.chinamobile.bcbsp.fault.browse;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.fault.storage.Fault;



public class MessageService {
	private int currentPage = 1;
	private int pageSize = 10;
	private List<Fault> totalList = new ArrayList<Fault>();
	 private static final Log LOG = LogFactory.getLog(MessageService.class);

	public static enum Type {
		TYPE, LEVEL, TIMEOFFAILURE, WORKERNODENAME, JOBNAME, STAFFNAME, EXCEPTIONMESSAGE, FAULTSTATUS
	}

	private List<Fault> getTotalList() {
		return totalList;
	}

	private void setTotalList(List<Fault> totalList) {
		this.totalList = totalList;
	}

	public void setCurrentPage(int currentPage) {
		this.currentPage = currentPage;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getCurrentPage() {
		return this.currentPage;
	}

	public int getPageSize() {
		return this.pageSize;
	}

	public int getTotalRows() {
		List<Fault> list = getTotalList();
		return list.size();
	}

	public int getTotalPages() {
		int total = getTotalRows() / getPageSize();
		int left = getTotalRows() % getPageSize();
		if (left != 0) {
			total = total + 1;
		}
		return total;
	}

	public List<Fault> getPageByType(int pageNumber, String type) {
		Browse br = new Browse();
		
		if (type.equals("TYPE")) {
			setTotalList(br.retrieveByType());
		} else if (type.equals("LEVEL")) {
			setTotalList(br.retrieveByLevel());
		} else if (type.equals("TIME")) {
		    LOG.info("+++++++++++enter the getFile(n) from TIME");
			setTotalList(br.retrieveByTime());
		} else if (type.equals("WORKER")) {
			setTotalList(br.retrieveByPosition());
		} else {
			setTotalList(br.retrieveByTime());
		}

		List<Fault> res = new ArrayList<Fault>();
		if(getTotalList().size()==0){
			return res;
		}
		this.setCurrentPage(pageNumber);
		int totalPages = getTotalPages();
		if (currentPage <= 1) {
			currentPage = 1;
		}
		if (currentPage >= totalPages) {
			currentPage = totalPages;
		}
		if (currentPage >= 1 && currentPage < totalPages) {
			for (int i = (currentPage - 1) * pageSize; i < currentPage
					* pageSize; i++) {
				res.add(totalList.get(i));
			}
		} else if (currentPage == totalPages) {
			for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
				res.add(totalList.get(i));
			}
		}
		return res;
	}
	public List<Fault> getPageByType(int pageNumber, String type,int num) {
		Browse br = new Browse();
		
		if (type.equals("TYPE")) {
			setTotalList(br.retrieveByType(num));
		} else if (type.equals("LEVEL")) {
			setTotalList(br.retrieveByLevel(num));
		} else if (type.equals("TIME")) {
		    
			setTotalList(br.retrieveByTime(num));
		} else if (type.equals("WORKER")) {
			setTotalList(br.retrieveByPosition(num));
		} else {
			setTotalList(br.retrieveByTime(num));
		}
		
		List<Fault> res = new ArrayList<Fault>();
		if(getTotalList().size()==0){
			return res;
		}
		this.setCurrentPage(pageNumber);
		int totalPages = getTotalPages();
		if (currentPage <= 1) {
			currentPage = 1;
		}
		if (currentPage >= totalPages) {
			currentPage = totalPages;
		}
		if (currentPage >= 1 && currentPage < totalPages) {
			for (int i = (currentPage - 1) * pageSize; i < currentPage
			* pageSize; i++) {
				res.add(totalList.get(i));
			}
		} else if (currentPage == totalPages) {
			for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
				res.add(totalList.get(i));
			}
		}
		return res;
	}
	
	public List<Fault> getPageByKey(int pageNumber, String key) {
		Browse br = new Browse();
		String[] keys ={key};
		setTotalList(br.retrieveWithMoreKeys(keys));

		List<Fault> res = new ArrayList<Fault>();
		if(getTotalList().size()==0){
			return res;
		}
		this.setCurrentPage(pageNumber);

		int totalPages = getTotalPages();
		if (currentPage <= 1) {
			currentPage = 1;
		}
		if (currentPage >= totalPages) {
			currentPage = totalPages;
		}
		if (currentPage >= 1 && currentPage < totalPages) {
			for (int i = (currentPage - 1) * pageSize; i < currentPage
					* pageSize; i++) {
				res.add(totalList.get(i));
			}
		} else if (currentPage == totalPages) {
			for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
				res.add(totalList.get(i));
			}
		}
		return res;
	}
	public List<Fault> getPageByKey(int pageNumber, String key,int num) {
		Browse br = new Browse();
		String[] keys ={key};
		  if(key!=null){
		  }
		setTotalList(br.retrieveWithMoreKeys(keys,num));

		List<Fault> res = new ArrayList<Fault>();
		if(getTotalList().size()==0){
			return res;
		}
		this.setCurrentPage(pageNumber);

		int totalPages = getTotalPages();
		if (currentPage <= 1) {
			currentPage = 1;
		}
		if (currentPage >= totalPages) {
			currentPage = totalPages;
		}
		if (currentPage >= 1 && currentPage < totalPages) {
			for (int i = (currentPage - 1) * pageSize; i < currentPage
					* pageSize; i++) {
				res.add(totalList.get(i));
			}
		} else if (currentPage == totalPages) {
			for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
				res.add(totalList.get(i));
			}
		}
		return res;
	}
	
	public List<Fault> getPageByKeys(int pageNumber, String[] keys) {
		Browse br = new Browse();
		setTotalList(br.retrieveWithMoreKeys(keys));

		List<Fault> res = new ArrayList<Fault>();
		if(getTotalList().size()==0){
			return res;
		}
		this.setCurrentPage(pageNumber);

		int totalPages = getTotalPages();
		if (currentPage <= 1) {
			currentPage = 1;
		}
		if (currentPage >= totalPages) {
			currentPage = totalPages;
		}
		if (currentPage >= 1 && currentPage < totalPages) {
			for (int i = (currentPage - 1) * pageSize; i < currentPage
					* pageSize; i++) {
				res.add(totalList.get(i));
			}
		} else if (currentPage == totalPages) {
			for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
				res.add(totalList.get(i));
			}
		}
		return res;
	}

public List<Fault> getPageByKeys(int pageNumber, String[] keys, int num) {
	Browse br = new Browse();
	setTotalList(br.retrieveWithMoreKeys(keys,num));
	
	List<Fault> res = new ArrayList<Fault>();
	if(getTotalList().size()==0){
		return res;
	}
	this.setCurrentPage(pageNumber);
	
	int totalPages = getTotalPages();
	if (currentPage <= 1) {
		currentPage = 1;
	}
	if (currentPage >= totalPages) {
		currentPage = totalPages;
	}
	if (currentPage >= 1 && currentPage < totalPages) {
		for (int i = (currentPage - 1) * pageSize; i < currentPage
		* pageSize; i++) {
			res.add(totalList.get(i));
		}
	} else if (currentPage == totalPages) {
		for (int i = (currentPage - 1) * pageSize; i < getTotalRows(); i++) {
			res.add(totalList.get(i));
		}
	}
	return res;
}
}
