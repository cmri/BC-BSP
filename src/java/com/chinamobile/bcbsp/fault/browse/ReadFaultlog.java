/**
 * CopyRight by Chinamobile
 * 
 * ReadFaultlog.java
 */
package com.chinamobile.bcbsp.fault.browse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.fault.storage.Fault.Level;
import com.chinamobile.bcbsp.fault.storage.Fault.Type;
import com.chinamobile.bcbsp.fault.storage.ManageFaultLog;

/**
 *  read Fault log from local path or hdfs path;
 * @author root
 *
 */
public class ReadFaultlog {
	private int defaultNum = 3;
	private String localRecordPath = null;
	private String hdfsRecordPath = null;
	private BufferedReader br = null;


	private GetFile getfile = null;

	private static final Log LOG = LogFactory.getLog(ReadFaultlog.class);

	public ReadFaultlog(String dirPath, String hdfsHostName) {
		this(dirPath, dirPath, hdfsHostName);
	}

	/**
	 * @param localDirPath
	 *            is the directory of the record.bak
	 * @param domainName
	 *            the namenode domainName
	 * @param hdfsDir
	 *            the directory of the record.bak in hdfs
	 */
	public ReadFaultlog(String localDirPath, String hdfsDir, String domainName) {
		super();
		if (localDirPath.substring(localDirPath.length() - 1) != "/"
				&& localDirPath.substring(localDirPath.length() - 2) != "\\") {
			localDirPath = localDirPath + File.separator;
		}
		if (hdfsDir.substring(hdfsDir.length() - 1) != "/"
				&& hdfsDir.substring(hdfsDir.length() - 2) != "\\") {
			hdfsDir = hdfsDir + File.separator;
		}

		this.localRecordPath = localDirPath + ManageFaultLog.getRecordFile();
		this.hdfsRecordPath = domainName + hdfsDir
				+ ManageFaultLog.getRecordFile();
		getfile = new GetFile(localRecordPath, hdfsRecordPath);
	}



	public List<Fault> read() {
		List<Fault> records = new ArrayList<Fault>();
		List<String> monthDirs = null;

		monthDirs = getfile.getFile(defaultNum);
		for (String eachMonth : monthDirs) {
			records.addAll(readDir(eachMonth));
		}

		getfile.deletehdfsDir();// delete the distributeFile in the localdisk;
		return records;
		
	}

	/**
	 * @param n
	 * @return a list of fault in order to browse
	 */
	public List<Fault> read(int n) {
		List<Fault> records = new ArrayList<Fault>();
		List<String> monthDirs = null;

		monthDirs = getfile.getFile(n);
		for (String eachMonth : monthDirs) {
			records.addAll(readDir(eachMonth));
		}

		getfile.deletehdfsDir();// delete the distributeFile in the localdisk;
		return records;
	}

	/**
	 * @param keys
	 *            the given restrain
	 * @return the records with the given keys
	 */
	public List<Fault> read(String[] keys) {

		List<Fault> records = new ArrayList<Fault>();
		List<String> monthDirs = null;

		monthDirs = getfile.getFile(defaultNum);
		for (String eachMonth : monthDirs) {
			records.addAll(readDirWithKey(eachMonth, keys));
		}

		getfile.deletehdfsDir();// delete the distributeFile in the localdisk;
		return records;
	}

	/**
	 * @param keys
	 *            the given restrain
	 * @param n
	 *            is the month ready to browse
	 * @return the records with the given keys
	 */
	public List<Fault> read(String[] keys, int n) {

		List<Fault> records = new ArrayList<Fault>();
		List<String> monthDirs = null;

		monthDirs = getfile.getFile(n);
		for (String eachMonth : monthDirs) {
			records.addAll(readDirWithKey(eachMonth, keys));
		}

		getfile.deletehdfsDir();// delete the distributeFile in the localdisk;
		return records;
	}

	
	/**
	 * @param path
	 *            the directory of the ready to browse
	 * @return each record of the files in the directory
	 */
	public List<Fault> readDir(String path) {
		List<Fault> records = new ArrayList<Fault>();
		File srcFile = new File(path);
		if (srcFile.isDirectory()) {
			ArrayList<File> files = getAllFiles(srcFile);
			for (File file : files) {
				if (!records.addAll(readFile(file)))  
					return new ArrayList<Fault>();
			}
		} else {
			if (!records.addAll(readFile(srcFile)))
				return new ArrayList<Fault>();
		}

		return records;
	}

	/**
	 * @param path
	 * @param keys
	 *            the given restrain
	 * @return
	 */
	public List<Fault> readDirWithKey(String path, String[] keys) {
		List<Fault> records = new ArrayList<Fault>();
		File srcFile = new File(path);
		if (srcFile.isDirectory()) {
			ArrayList<File> files = getAllFiles(srcFile);
			for (File file : files) {
				records.addAll(readFileWithKey(file, keys));
			}
		} 
		else {
			if (!records.addAll(readFileWithKey(srcFile, keys)))
				return new ArrayList<Fault>();
		}

		return records;
	}


	/**
	 * @param file
	 * @return
	 */
	private List<Fault> readFile(File file) { 
		FileReader fr = null;
		try {
			List<Fault> records = new ArrayList<Fault>();
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			String tempRecord = null;
			String filds[] = null;

			while ((tempRecord = br.readLine()) != null) {
				filds = tempRecord.split("--");
				Fault fault = new Fault();

				
				
				fault.setTimeOfFailure(filds[0].trim());
				fault.setType(getType(filds[1].trim()));
				fault.setLevel(getLevel(filds[2].trim()));
				fault.setWorkerNodeName(filds[3].trim());
				fault.setJobName((filds[4].trim()));
				fault.setStaffName(filds[5].trim());
				fault.setExceptionMessage(filds[6].trim());
				fault.setFaultStatus(getBoolean(filds[7].trim()));
				records.add(fault);

			}

			br.close();
			fr.close();

			return records;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			try {
				br.close();
				fr.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			return null;
		}
	}

	private Fault.Level getLevel(String level) {
		if (level.equals("INDETERMINATE")) {
			return Level.INDETERMINATE;
		} else if (level.equals("WARNING")) {
			return Level.WARNING;
		} else if (level.equals("CRITICAL")) {
			return Level.CRITICAL;
		} else if (level.equals("MAJOR")) {
			return Level.MAJOR;
		} else
			return Level.MINOR;
	}

	private Fault.Type getType(String type) {
		if (type.equals("DISK")) {
			return Type.DISK;
		} else if (type.equals("NETWORK")) {
			return Type.NETWORK;
		} else if (type.equals("SYSTEMSERVICE")) {
			return Type.SYSTEMSERVICE;
		} else if(type.equals("FORCEQUIT")){
			return Type.FORCEQUIT;
		}else{
		    return Type.WORKERNODE;
		}
	}
	
	private boolean getBoolean(String flag){
		if(flag.equals("true"))return true;
		return false;
	}

	/*
	 * file is the srcFile,and keys is the given key and the keys can be null
	 * then the function is read all the record in the file,if none of keys ,it
	 * is better to use read(File file) ,the function need not judge for each
	 * record;
	 * 
	 * return the match condition record;
	 */
	private List<Fault> readFileWithKey(File file, String[] keys) {

		FileReader fr = null;

		try {
			List<Fault> records = new ArrayList<Fault>();
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			String tempRecord = null;
			String filds[] = null;

			while ((tempRecord = br.readLine()) != null) {
				boolean matching = true;
				if (keys != null) {
					for (String key : keys) {
						if(key==null)continue;
						if ((tempRecord.indexOf(key) == -1)
								&& (tempRecord.toLowerCase().indexOf(
										key.toLowerCase()) == -1)) {
							matching = false;
							break;
						}
					}
				}
				if (matching == true) {
					filds = tempRecord.split("--");
					Fault fault = new Fault();
										fault.setTimeOfFailure(filds[0].trim());
										fault.setType(getType(filds[1].trim()));
										fault.setLevel(getLevel(filds[2].trim()));
										fault.setWorkerNodeName(filds[3].trim());
										fault.setJobName((filds[4].trim()));
										fault.setStaffName(filds[5].trim());
										fault.setExceptionMessage(filds[6].trim());
										fault.setFaultStatus(getBoolean(filds[7].trim()));
										records.add(fault);
					
					} else
					continue;
			}

			br.close();
			fr.close();

			return records;

		} catch (FileNotFoundException e) {
			LOG.error("[readFileWithKey]", e);
			return new ArrayList<Fault>();
		} catch (IOException e) {
		    LOG.error("[readFileWithKey]", e);
			try {
				br.close();
				fr.close();
			} catch (IOException e1) {
			    LOG.error("[readFileWithKey]", e1);
			}
			return new ArrayList<Fault>();
		}
	}

	/**
	 * get all the files in the given directory;
	 * 
	 * @param filesrc
	 * @return
	 */
	private ArrayList<File> getAllFiles(File filesrc) {
		ArrayList<File> allFiles = new ArrayList<File>();
		File[] files = filesrc.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				allFiles.addAll(getAllFiles(file));
			} else {
				allFiles.add(file);
			}
		}

		return allFiles;

	}

}
