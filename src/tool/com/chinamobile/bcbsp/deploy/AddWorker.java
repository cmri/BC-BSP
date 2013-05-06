/**
 * CopyRight by Chinamobile
 * 
 * AddWorker.java
 */
package com.chinamobile.bcbsp.deploy;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class AddWorker {
		String newUserName;
		String newIpAddress;
		String newHostName;
		String hadoopSourcePath;
		String hadoopDstPath;
		String bcbspSourcePath;
		String bcbspDstPath;
		
		public  AddWorker(String newUserName, String newIpAddress, String newHostName,
				String hadoopSourcePath, String hadoopDstPath, String bcbspSourcePath, String bcbspDstPath) {
			this.newUserName = newUserName;
			this.newIpAddress = newIpAddress;
			this.newHostName = newHostName;
			
			this.hadoopSourcePath = hadoopSourcePath;
			this.hadoopDstPath = hadoopDstPath;
			this.bcbspSourcePath = bcbspSourcePath;
			this.bcbspDstPath = bcbspDstPath;
		}
		
		public void deployHadoopBCBSP() throws Exception {
			// copy system
			/*String command_scp_hadoop = ("scp -r" + " " + this.hadoopSourcePath 
					+ " " + this.newUserName + "@" + this.newIpAddress + ":" + this.hadoopDstPath);*/
			String command_scp_bcbsp = ("scp -r" + " " + this.bcbspSourcePath 
					+ " " + this.newUserName + "@" + this.newIpAddress + ":" + this.bcbspDstPath);
			
			//String cmd1[] = {"/bin/bash", "-c", command_scp_hadoop};
			String cmd2[] = {"/bin/bash", "-c", command_scp_bcbsp};
			//System.out.println(command_scp_hadoop);
			//System.out.println(command_scp_bcbsp);
			//Process pro_hadoop = Runtime.getRuntime().exec(cmd1);
			Process pro_bcbsp  = Runtime.getRuntime().exec(cmd2);
			//pro_hadoop.waitFor();
			pro_bcbsp.waitFor();
			
			// start hdfs & workerManager
			/*String command_start_hdfs = ("ssh -l " + this.newUserName + " " + this.newIpAddress  + " " + "\"" 
					+ "$HADOOP_HOME/bin/hadoop-daemon.sh start datenode" + "\"");*/
			String command_start_bcbsp = ("ssh -l " + this.newUserName + " " + this.newIpAddress  + " " + "\"" 
					+ "$BCBSP_HOME/bin/bcbsp-daemon.sh start workermanager" + "\"");
			//String cmd3[] = {"/bin/bash", "-c", command_start_hdfs};
			String cmd4[] = {"/bin/bash", "-c", command_start_bcbsp};
			//pro_hadoop = Runtime.getRuntime().exec(cmd3);
			pro_bcbsp  = Runtime.getRuntime().exec(cmd4);
			//pro_hadoop.waitFor();
			pro_bcbsp.waitFor();
		}
		
		public void changeWorkermanager(String rootPath, String hadoopIP, String hadoopUserName,
				String bcbspIP, String bcbspUserName) throws Exception {
			String command = null;
			String[] cmd = {"/bin/bash", "-c", command};
			Process p;
			
			// change Hadoop
			command = "scp " + hadoopUserName + "@" + hadoopIP + ":" + this.hadoopDstPath + "/" + Util.HadoopConf.HADOOP_CONF_DIR + "/"
					+ Util.HadoopConf.HADOOP_CONF_SLAVES_FILE + " " + rootPath + "/"
					+ Util.SystemConf.DEPLOY_TEMP_DIR + "/";
			cmd[2] = command;
			p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			
			File hadoopFile = new File(rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR, Util.HadoopConf.HADOOP_CONF_SLAVES_FILE);
			FileWriter hadoopFW = new FileWriter(hadoopFile, true);
			BufferedWriter hadoopBW = new BufferedWriter(hadoopFW);
			hadoopBW.write(this.newHostName);
			hadoopBW.close();
			hadoopFW.close();
			
			command = "scp " + hadoopFile.toString() + " " + hadoopUserName
					+ "@" + hadoopIP + ":" + this.hadoopDstPath + "/" + Util.HadoopConf.HADOOP_CONF_DIR + "/";
			cmd[2] = command;
			p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			hadoopFile.delete();
			
			// change BCBSP
			command = "scp " + bcbspUserName + "@" + bcbspIP + ":" + this.bcbspDstPath + "/" + Util.BCBSPConf.BCBSP_CONF_DIR + "/"
				+ Util.BCBSPConf.BCBSP_CONF_WORKERS_FILE+ " " + rootPath + "/"
				+ Util.SystemConf.DEPLOY_TEMP_DIR + "/";
			cmd[2] = command;
			p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
	
			File bcbspFile = new File(rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR, Util.BCBSPConf.BCBSP_CONF_WORKERS_FILE);
			FileWriter bcbspFW = new FileWriter(bcbspFile, true);
			BufferedWriter bcbspBW = new BufferedWriter(bcbspFW);
			bcbspBW.write(this.newHostName);
			bcbspBW.close();
			bcbspFW.close();
	
			command = "scp " + bcbspFile.toString() + " " + bcbspUserName
					+ "@" + bcbspIP + ":" + this.bcbspDstPath + "/" + Util.BCBSPConf.BCBSP_CONF_DIR + "/";
			cmd[2] = command;
			p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			bcbspFile.delete();
		}
}