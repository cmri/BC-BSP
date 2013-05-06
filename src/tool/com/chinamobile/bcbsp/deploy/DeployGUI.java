/**
 * CopyRight by Chinamobile
 * 
 * DeployGUI.java
 */
package com.chinamobile.bcbsp.deploy;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JDesktopPane;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ScrollPaneConstants;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;


import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;

public class DeployGUI extends JFrame {

	public class ButtonAction implements ActionListener {
		public void actionPerformed(ActionEvent e) {
			if (e.getSource() == ping) {
				ping();
			} else if (e.getSource() == save) {
				save();
			} else if (e.getSource() == refresh) {
				refresh();
			} else if (e.getSource() == addWorker) {
				addWorker();
			} else if (e.getSource() == removeWorker) {
				removeWorker();
			} else if (e.getSource() == sshExe) {
				sshExe();
			} else if (e.getSource() == copyExe) {
				copyExe();
			} else if (e.getSource() == setHadoop) {
				setHadoop();
			} else if (e.getSource() == setBCBSP) {
				setBCBSP();
			} else if (e.getSource() == setOS) {
				setOS();
			} else if (e.getSource() == browseHadoop) {
				browseHadoop();
			} else if (e.getSource() == browseBCBSP) {
				browseBCBSP();
			} else if (e.getSource() == deployHadoop) {
				deployHadoop();
			} else if (e.getSource() == deployBCBSP) {
				deployBCBSP();
			}
		}
	}

	private static final long serialVersionUID = 1L;
	private Container c = getContentPane();

	private JPanel operationCluster = new JPanel();
	private JPanel masterName = new JPanel();
	private JPanel operationWorker = new JPanel();
	private JPanel deployCluster = new JPanel();
	private JPanel definedExe = new JPanel();
	private JPanel copyRight = new JPanel();

	private JScrollPane workerList = new JScrollPane();
	private JScrollPane js_command = new JScrollPane();
	private JScrollPane js_result = new JScrollPane();
	private JTextArea ta_command = new JTextArea();
	private JTextArea ta_result = new JTextArea();

	private JTextField hostName = new JTextField();
	private JTextField hostIP = new JTextField();
	private JTextField hostAccount = new JTextField();
	private JTextField hostPassWord = new JTextField();
	private JTextField staffNum = new JTextField();
	private JTextField jdkLocation = new JTextField();
	
	private JTextField sourcePathBCBSP = new JTextField();
	private JTextField sourcePathHadoop = new JTextField();
	
	private JTextField bspController = new JTextField();
	private JTextField nameNode = new JTextField();
	private JTextField secondNameNode = new JTextField();
	private JTextField BCBSPHomePath = new JTextField();
	private JTextField HadoopHomePath = new JTextField();

	String[] col = {"HostName", "IP", "User", "PassWord", "Staff", "JDK"};
	DefaultTableModel mm = new DefaultTableModel(col, 0);
	JTable table = new JTable(mm);

	JButton ping = new JButton("Ping");
	JButton save = new JButton("Save");
	JButton refresh = new JButton("Refresh");
	JButton setHadoop = new JButton("Hadoop");
	JButton setBCBSP = new JButton("BCBSP");
	JButton setOS = new JButton("System");

	JButton addWorker = new JButton("Add");
	JButton removeWorker = new JButton("Remove");
	
	JButton browseHadoop = new JButton("Browse");
	JButton browseBCBSP = new JButton("Browse");
	JButton deployHadoop = new JButton("Deploy");
	JButton deployBCBSP = new JButton("Deploy");

	//JButton copyExe = new JButton("Copy");
	JButton sshExe = new JButton("Execute");
	JButton copyExe = new JButton("Copy");

	static DeployGUI win;
	
	String rootPath = null;
	String hadoopPath = null;

	public DeployGUI() {
		super("BC-BSP Deploy Tool");
		setBounds(200, 100, 780, 555);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		c.setLayout(null);
		operationCluster.setLayout(null);
		masterName.setLayout(null);
		operationWorker.setLayout(null);
		deployCluster.setLayout(null);
		definedExe.setLayout(null);
		copyRight.setLayout(null);

		// fist
		operationCluster.setBounds(10, 10, 800, 20);
		JLabel wo=new JLabel("Worker Operation");
		wo.setBounds(0, 0, 110, 20);
		operationCluster.add(wo);
		
		ping.setBounds(115, 0, 80, 20);
		ping.addActionListener(new ButtonAction());
		operationCluster.add(ping);

		save.setBounds(205, 0, 80, 20);
		save.addActionListener(new ButtonAction());
		operationCluster.add(save);

		refresh.setBounds(295, 0, 81, 20);
		refresh.addActionListener(new ButtonAction());
		operationCluster.add(refresh);
		
		JLabel sc=new JLabel("Set Conf");
		sc.setBounds(420, 0, 60, 20);
		operationCluster.add(sc);
		
		setOS.setBounds(485, 0, 85, 20);
		setOS.addActionListener(new ButtonAction());
		operationCluster.add(setOS);
		
		setHadoop.setBounds(575, 0, 85, 20);
		setHadoop.addActionListener(new ButtonAction());
		operationCluster.add(setHadoop);
		
		setBCBSP.setBounds(665, 0, 85, 20);
		setBCBSP.addActionListener(new ButtonAction());
		operationCluster.add(setBCBSP);

		masterName.setBounds(10, 55, 500, 45);
		JLabel bc = new JLabel("BSPController");
		bc.setBounds(0, 0, 85, 20);
		bspController.setBounds(90, 0, 65, 20);
		bspController.setText("null");
		masterName.add(bc);
		masterName.add(bspController);
		
		JLabel nn = new JLabel("NameNode");
		nn.setBounds(165, 0, 70, 20);
		nameNode.setBounds(240, 0, 65, 20);
		nameNode.setText("null");
		masterName.add(nn);
		masterName.add(nameNode);
		
		JLabel snn = new JLabel("SecondNameNode");
		snn.setBounds(315, 0, 120, 20);
		secondNameNode.setBounds(435, 0, 65, 20);
		secondNameNode.setText("null");
		masterName.add(snn);
		masterName.add(secondNameNode);
		
		JLabel bbhp = new JLabel("BCBSPHomePath");
		bbhp.setBounds(0, 25, 110, 20);
		BCBSPHomePath.setText("null");
		BCBSPHomePath.setBounds(115, 25, 120, 20);
		masterName.add(bbhp);
		masterName.add(BCBSPHomePath);
		
		JLabel hdhp = new JLabel("HadoopHomePath");
		hdhp.setBounds(255, 25, 120, 20);
		HadoopHomePath.setText("null");
		HadoopHomePath.setBounds(380, 25, 120, 20);
		masterName.add(hdhp);
		masterName.add(HadoopHomePath);
		
		// second
		int[] width = new int[]{40, 80, 4, 40, 1, 150};
		TableColumnModel columns = table.getColumnModel();  
	    for (int i = 0; i < width.length; i++) {  
	        TableColumn column = columns.getColumn(i);
	        column.setMinWidth(1);
	        column.setMaxWidth(200);
	        column.setPreferredWidth(width[i]);
	    }
		table.setColumnModel(columns);
		workerList.setViewportView(table);

		JDesktopPane desktopPane_worker = new JDesktopPane();
		desktopPane_worker.setBounds(10, 105, 500, 385);
		JInternalFrame interFram_shop = new JInternalFrame("WorkerServer List",
				false, false, false, false);
		interFram_shop.setBounds(0, 0, 500, 250);
		interFram_shop.setVisible(true);
		interFram_shop.add(workerList, BorderLayout.CENTER);
		desktopPane_worker.add(interFram_shop);
		
		ta_result.setLineWrap(true);
		js_result.setViewportView(ta_result);
		JInternalFrame interFram_result = new JInternalFrame("Result List",
				false, false, false, false);
		interFram_result.setVisible(true);				
		interFram_result.setBounds(0, 250, 500, 135);
		interFram_result.add(js_result);
		js_result.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		desktopPane_worker.add(interFram_result);

		// three
		operationWorker.setBounds(530, 55, 250, 170);
		JLabel hn = new JLabel("HostName");
		hn.setBounds(0, 0, 80, 20);
		hostName.setBounds(85, 0, 145, 20);
		operationWorker.add(hn);
		operationWorker.add(hostName);

		JLabel hip = new JLabel("HostIP");
		hip.setBounds(0, 25, 80, 20);
		hostIP.setBounds(85, 25, 145, 20);
		operationWorker.add(hip);
		operationWorker.add(hostIP);

		JLabel ha = new JLabel("Account");
		ha.setBounds(0, 50, 80, 20);
		hostAccount.setBounds(85, 50, 145, 20);
		operationWorker.add(ha);
		operationWorker.add(hostAccount);

		JLabel hp = new JLabel("PassWord");
		hp.setBounds(0, 75, 80, 20);
		hostPassWord.setBounds(85, 75, 145, 20);
		operationWorker.add(hp);
		operationWorker.add(hostPassWord);
		
		JLabel sn = new JLabel("StaffNum");
		sn.setBounds(0, 100, 80, 20);
		staffNum.setBounds(85, 100, 145, 20);
		operationWorker.add(sn);
		operationWorker.add(staffNum);
		
		JLabel jl = new JLabel("JDK");
		jl.setBounds(0, 125, 80, 20);
		jdkLocation.setBounds(85, 125, 145, 20);
		operationWorker.add(jl);
		operationWorker.add(jdkLocation);

		addWorker.setBounds(10, 150, 90, 20);
		addWorker.addActionListener(new ButtonAction());
		removeWorker.setBounds(130, 150, 90, 20);
		removeWorker.addActionListener(new ButtonAction());
		operationWorker.add(addWorker);
		operationWorker.add(removeWorker);
		
		// four
		deployCluster.setBounds(530, 230, 235, 122);
		JLabel dcTag = new JLabel("Deploy Hadoop & BCBSP");
		dcTag.setBounds(40, 0, 195, 20);
		deployCluster.add(dcTag);
		
		JLabel hadTag = new JLabel("Hadoop");
		hadTag.setBounds(0, 25, 50, 20);
		sourcePathHadoop.setBounds(52, 25, 183, 20);
		browseHadoop.setBounds(52, 47, 80, 20);
		browseHadoop.addActionListener(new ButtonAction());
		deployHadoop.setBounds(155, 47, 80, 20);
		deployHadoop.addActionListener(new ButtonAction());
		deployCluster.add(hadTag);
		deployCluster.add(sourcePathHadoop);
		deployCluster.add(browseHadoop);
		deployCluster.add(deployHadoop);
		
		JLabel bspTag = new JLabel("BCBSP");
		bspTag.setBounds(0, 80, 50, 20);
		sourcePathBCBSP.setBounds(52, 80, 183, 20);
		browseBCBSP.setBounds(52, 102, 80, 20);
		browseBCBSP.addActionListener(new ButtonAction());
		deployBCBSP.setBounds(155, 102, 80, 20);
		deployBCBSP.addActionListener(new ButtonAction());
		deployCluster.add(bspTag);
		deployCluster.add(sourcePathBCBSP);
		deployCluster.add(browseBCBSP);
		deployCluster.add(deployBCBSP);
		
		// five
		ta_command.setLineWrap(true);
		js_command.setViewportView(ta_command);

		JDesktopPane desktopPane_command = new JDesktopPane();
		desktopPane_command.setBounds(530, 360, 235, 105);
		JInternalFrame interFram_command = new JInternalFrame(
				"User-Defined Command", false, false, false, false);
		interFram_command.setBounds(0, 0, 235, 105);
		interFram_command.setVisible(true);
		interFram_command.add(js_command, BorderLayout.CENTER);
		desktopPane_command.add(interFram_command);
		
		// six
		definedExe.setBounds(530, 470, 235, 20);
		//copyExe.setBounds(2, 0, 70, 20);
		//copyExe.addActionListener(new ButtonAction());
		sshExe.setBounds(0, 0, 85, 20);
		sshExe.addActionListener(new ButtonAction());
		copyExe.setBounds(147, 0, 85, 20);
		copyExe.addActionListener(new ButtonAction());
		//definedExe.add(copyExe);
		definedExe.add(sshExe);
		definedExe.add(copyExe);

		// seven
		copyRight.setBounds(200, 500, 600, 20);
		JLabel copy = new JLabel("2011-2013  copyright(c)  all rights reserved v1.0");
		copy.setBounds(0, 0, 600, 20);
		copyRight.add(copy);

		c.add(operationCluster);
		c.add(masterName);
		c.add(desktopPane_worker);
		c.add(operationWorker);
		c.add(deployCluster);
		c.add(desktopPane_command);
		c.add(definedExe);
		c.add(copyRight);
		rootPath = getParentPath(this.getClass());
		refresh();
	}

	public void ping() {
		boolean tmp = false;
		for (int i = 0; i < mm.getRowCount(); i++) {
			try {
				Connection conn = new Connection((String) mm.getValueAt(i, 1));
				conn.connect();
				conn.close();
			} catch (Exception e) {
				tmp = true;
				JOptionPane.showMessageDialog(win, "ERROR!\nFail to connect: "
						+ mm.getValueAt(i, 0) + "!");
			}
		}
		if (!tmp) {
			JOptionPane.showMessageDialog(win,
					"SUCCESS!\nAll nodes are normal!");
		}
	}

	public void save() {
		int a = JOptionPane.showConfirmDialog(null,
				"Are you sure to save the current information?",
				"Note", JOptionPane.YES_NO_OPTION);
		if (a != 0) {
			return;
		}
		
		try {
			File fileRoot = new File(rootPath + "/" + Util.SystemConf.DEPLOY_CACHE_DIR);
			if (!fileRoot.exists()) {
					fileRoot.mkdirs();
				}
				File file = new File(fileRoot, Util.SystemConf.DEPLOY_CACHE_File);
				if (!file.exists()) {
					file.createNewFile();
				}
				FileWriter fw = new FileWriter(file);
				BufferedWriter bw = new BufferedWriter(fw, 65536);
				
				bw.write(Util.SystemConf.MASTER_NAME_HEADER + "\t" + this.bspController.getText()
						+ "\t" + this.nameNode.getText()
						+ "\t" + this.secondNameNode.getText());
				bw.newLine();
				bw.write(Util.SystemConf.BCBSP_HOME_PATH_HEADER + "\t" + this.BCBSPHomePath.getText());
				bw.newLine();
				bw.write(Util.SystemConf.HADOOP_HOME_PATH_HEADER + "\t" + this.HadoopHomePath.getText());
				bw.newLine();
				
				for (int i = 0; i < mm.getRowCount(); i++) {
					bw.write(mm.getValueAt(i, 0) + "\t" + mm.getValueAt(i, 1)
							+ "\t" + mm.getValueAt(i, 2)
							+ "\t" + mm.getValueAt(i, 3)
							+ "\t" + mm.getValueAt(i, 4)
							+ "\t" + mm.getValueAt(i, 5));
					bw.newLine();
				}
				bw.close();
				fw.close();
				JOptionPane.showMessageDialog(win, "Save data successfully!");	
			} catch (Exception e) {
				JOptionPane.showMessageDialog(win, "ERROR! Fail to save!");
			}	
	}

	private String searchJDKLocation(String workerName, String ipAddress, String userName) {
		String result = null;
		String command = "scp " + userName + "@" + ipAddress + ":/etc/" + Util.SystemConf.SYSTEM_CHECK_FILE
				+ " " + rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR + "/";
		String[] cmd={"/bin/bash","-c",command};
		try{
			Process p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			File file = new File(rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR + "/" + Util.SystemConf.SYSTEM_CHECK_FILE);
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr, 65536);
			String read;
			while((read = br.readLine()) != null) {
				int index = read.indexOf(Util.SystemConf.JDK_HOME_CHECK_HEADER);
				if (index != -1) {
					result = read.substring(index + 10);
					break;
				}
			}
			br.close();
			fr.close();
			file.delete();
		}catch(Exception e){
			JOptionPane.showMessageDialog(win, "ERROR!\nFail to fetch JDK's path at worker: "+ workerName +" !");
			//e.printStackTrace();
		}	
		return result;
	}
	
	public void refresh() {
		try {
				File tmpDir = new File(rootPath + "/" + Util.SystemConf.DEPLOY_TEMP_DIR);
				if (!tmpDir.exists()) {
					tmpDir.mkdirs();
				}
				
				File file = new File(rootPath + "/" + Util.SystemConf.DEPLOY_CACHE_DIR + "/" + Util.SystemConf.DEPLOY_CACHE_File);
				if (!file.exists()) {
					JOptionPane.showMessageDialog(win,
							"ERROR! The file is not exist!");
					return;
				}
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr, 65536);
				String read;
				Vector<String> row;
				int count = mm.getRowCount();
				for (int i = 0; i < count; i++) {
					mm.removeRow(0);
				}
				while ((read = br.readLine()) != null) {
					String[] tmp = read.split("\t");
					
					if (tmp[0].equals(Util.SystemConf.MASTER_NAME_HEADER)) {
						this.bspController.setText(tmp[1]);
						this.nameNode.setText(tmp[2]);
						this.secondNameNode.setText(tmp[3]);
						continue;
					}
					
					if (tmp[0].equals(Util.SystemConf.BCBSP_HOME_PATH_HEADER)) {
						this.BCBSPHomePath.setText(tmp[1]);
						continue;
					}
					
					if (tmp[0].equals(Util.SystemConf.HADOOP_HOME_PATH_HEADER)) {
						this.HadoopHomePath.setText(tmp[1]);
						continue;
					}
					
					row = new Vector<String>();
					if (tmp.length == 5) {
						row.add(tmp[0]);
						row.add(tmp[1]);
						row.add(tmp[2]);
						row.add(tmp[3]);
						row.add(tmp[4]);
						String jdk = searchJDKLocation(tmp[0], tmp[1], tmp[2]);
						if (jdk != null) {
							row.add(jdk);
						}
					} else if (tmp.length == 6) {
						row.add(tmp[0]);
						row.add(tmp[1]);
						row.add(tmp[2]);
						row.add(tmp[3]);
						row.add(tmp[4]);
						row.add(tmp[5]);
					} else {
						JOptionPane.showMessageDialog(win, "Invalid information: " + read);
					}
					mm.addRow(row);
				}
				br.close();
				fr.close();
			} catch (Exception e) {
				JOptionPane.showMessageDialog(win, "ERROR! Fail to read!");
			}
	}

	public void setHadoop() {
		int index = 0;
		for (; index < this.mm.getColumnCount(); index++) {
			if (this.mm.getValueAt(index, 0).equals(this.nameNode.getText())) {
				break;
			}
		}
		
		@SuppressWarnings("unused")
		HadoopConfiguration conf = new HadoopConfiguration(this, this.nameNode.getText().toString(),
				this.mm.getValueAt(index, 1).toString(), this.mm.getValueAt(index, 2).toString(),
				this.HadoopHomePath.getText().toString(), this.rootPath, this.mm);
	}
	
	public void setBCBSP() {
		int index = 0;
		for (; index < this.mm.getColumnCount(); index++) {
			if (this.mm.getValueAt(index, 0).equals(this.bspController.getText())) {
				break;
			}
		}
		
		@SuppressWarnings("unused")
		BCBSPConfiguration conf = new BCBSPConfiguration(this, this.bspController.getText().toString(),
				this.mm.getValueAt(index, 1).toString(), this.mm.getValueAt(index, 2).toString(),
				this.BCBSPHomePath.getText().toString(), this.rootPath, this.mm);
	}
	
	private void setOSProfile(String hostName, String ipAddress, String userName) throws Exception {
		String command = "scp " + userName + "@" + ipAddress + ":/etc/"
				+ Util.SystemConf.SYSTEM_CHECK_FILE + " " + rootPath + "/"
				+ Util.SystemConf.DEPLOY_TEMP_DIR + "/";
		String[] getCmd = { "/bin/bash", "-c", command };
		Process getP = Runtime.getRuntime().exec(getCmd);
		getP.waitFor();
		File initFile = new File(rootPath + "/"
				+ Util.SystemConf.DEPLOY_TEMP_DIR + "/"
				+ Util.SystemConf.SYSTEM_CHECK_FILE);
		File sourceFile = new File(rootPath + "/"
				+ Util.SystemConf.DEPLOY_TEMP_DIR + "/"
				+ Util.SystemConf.SYSTEM_CHECK_FILE + ".tmp");
		initFile.renameTo(sourceFile);
		FileReader fr = new FileReader(sourceFile);
		BufferedReader br = new BufferedReader(fr, 65536);
		File dstFile = new File(rootPath + "/"
				+ Util.SystemConf.DEPLOY_TEMP_DIR + "/"
				+ Util.SystemConf.SYSTEM_CHECK_FILE);
		FileWriter fw = new FileWriter(dstFile);
		BufferedWriter bw = new BufferedWriter(fw);
		String read;
		while ((read = br.readLine()) != null) {
			int index = read.indexOf("BCBSP");
			if (index != -1) {
				continue;
			}
			index = read.indexOf("HADOOP");
			if (index != -1) {
				continue;
			}
			bw.write(read);
			bw.newLine();
		}
		
		bw.write("export " + Util.SystemConf.HADOOP_HOME_PATH_HEADER + this.HadoopHomePath.getText());
		bw.newLine();
		bw.write("export HADOOP_CONF_DIR=" + this.HadoopHomePath.getText() + "/conf");
		bw.newLine();
		bw.write("export PATH=$HADOOP_HOME/bin:$PATH");
		bw.newLine();
		
		bw.write("export " + Util.SystemConf.BCBSP_HOME_PATH_HEADER + this.BCBSPHomePath.getText());
		bw.newLine();
		bw.write("export BCBSP_CONF_DIR=" + this.BCBSPHomePath.getText() + "/conf");
		bw.newLine();
		bw.write("export PATH=$PATH:$BCBSP_HOME/bin");
		
		br.close();
		fr.close();
		bw.close();
		fw.close();
		sourceFile.delete();
		
		command = "scp " + dstFile.toString() + " "
				+ userName + "@" + ipAddress + ":/etc/";
		String[] backCmd = {"/bin/bash","-c",command};
		Process backP = Runtime.getRuntime().exec(backCmd);
		backP.waitFor();
		dstFile.delete();
	}
	
	public void setOS() {
		int a = JOptionPane.showConfirmDialog(null,
				"Are you sure to set the system profile?",
				"Note", JOptionPane.YES_NO_OPTION);
		if (a != 0) {
			return;
		}
		
		try {
			for(int i = 0; i < mm.getRowCount(); i++) {
				setOSProfile(mm.getValueAt(i, 0).toString(), mm.getValueAt(i, 1).toString(), mm.getValueAt(i, 2).toString());
			}
			JOptionPane.showMessageDialog(win,
					"SUCCESS!\nSuccess to setup the profile.");
		} catch (Exception e) {
			JOptionPane.showMessageDialog(win,
				"ERROR!\nFail to setup profile!");
		}
	}
	
	public void addWorker() {
		int a = JOptionPane.showConfirmDialog(null,
				"Are you sure to add this worker?",
				"Note", JOptionPane.YES_NO_OPTION);
		if (a != 0) {
			return;
		}
		
		Vector<String> row = new Vector<String>();
		row.add(hostName.getText());
		row.add(hostIP.getText());
		row.add(hostAccount.getText());
		row.add(hostPassWord.getText());
		row.add(staffNum.getText());
		if (jdkLocation.getText().toString().length() == 0) {
			String location = searchJDKLocation(hostName.getText().toString(), hostIP.getText().toString(), hostAccount.getText().toString());
			jdkLocation.setText(location);
		}
		row.add(jdkLocation.getText());
		try {
			AddWorker newWorker = new AddWorker(hostAccount.getText().toString(), hostIP.getText().toString(), hostName.getText().toString(),
					this.sourcePathHadoop.getText().toString(), this.HadoopHomePath.getText().toString(),
					this.sourcePathBCBSP.getText().toString(), this.BCBSPHomePath.getText().toString());
			setOSProfile(hostName.getText().toString(), hostIP.getText().toString(), hostAccount.getText().toString());
			newWorker.deployHadoopBCBSP();
			
			int index = 0, hadoopIndex = 0, bcbspIndex = 0;
			for (; index < this.mm.getRowCount(); index++) {
				if (this.mm.getValueAt(index, 0).equals(this.nameNode.getText())) {
					hadoopIndex = index;
				}
				if (this.mm.getValueAt(index, 0).equals(this.bspController.getText())) {
					bcbspIndex = index;
				}
			}
			newWorker.changeWorkermanager(this.rootPath, this.mm.getValueAt(hadoopIndex, 1).toString(), this.mm.getValueAt(hadoopIndex, 2).toString(),
					this.mm.getValueAt(bcbspIndex, 1).toString(), this.mm.getValueAt(bcbspIndex, 2).toString());
			mm.addRow(row);
			save();
			JOptionPane.showMessageDialog(win, "Add " + hostName.getText()
					+ " successfully!");
		} catch (Exception e) {
			JOptionPane.showMessageDialog(win, "ERROR!\nFail to add the new worker:" + hostName.getText() + "!");
			//e.printStackTrace();
		}
	}

	public void removeWorker() {
		int a = JOptionPane.showConfirmDialog(null,
				"Are you sure to remove this worker?",
				"Note", JOptionPane.YES_NO_OPTION);
		if (a != 0) {
			return;
		}
		
		String deleteHost = hostName.getText();
		int flag = 0;
		if ((deleteHost != null) && (!deleteHost.equals(""))) {
			for (flag = 0; flag < mm.getRowCount(); flag++) {
				if (mm.getValueAt(flag, 0).equals(deleteHost)) {
					break;
				}
			}
		} else {
			JOptionPane.showMessageDialog(win, "ERROR! Please input the HostName!");
			return;
		}
		
		try {
			RemoveWorker removeWorker = new RemoveWorker(this.mm.getValueAt(flag, 2).toString(), this.mm.getValueAt(flag, 1).toString(), this.mm.getValueAt(flag, 0).toString(),
					this.sourcePathHadoop.getText().toString(), this.HadoopHomePath.getText().toString(),
					this.sourcePathBCBSP.getText().toString(), this.BCBSPHomePath.getText().toString());
			removeWorker.closeDaemon();
			
			int index, hadoopIndex = 0, bcbspIndex = 0;
			for (index = 0; index < this.mm.getRowCount(); index++) {
				if (this.mm.getValueAt(index, 0).equals(this.nameNode.getText())) {
					hadoopIndex = index;
				}
				if (this.mm.getValueAt(index, 0).equals(this.bspController.getText())) {
					bcbspIndex = index;
				}
			}
			removeWorker.changeWorkermanager(this.rootPath, this.mm.getValueAt(hadoopIndex, 1).toString(), this.mm.getValueAt(hadoopIndex, 2).toString(),
					this.mm.getValueAt(bcbspIndex, 1).toString(), this.mm.getValueAt(bcbspIndex, 2).toString());
			mm.removeRow(flag);
			save();
			JOptionPane.showMessageDialog(win, "Remove " + hostName.getText()
					+ " successfully!");
		} catch (Exception e) {
			JOptionPane.showMessageDialog(win, "ERROR!\nFail to remove the new worker:" + hostName.getText() + "!");
			//e.printStackTrace();
		}
	}

	public void copyExe() {
		int a = JOptionPane.showConfirmDialog(null,
				"Are you sure to copy?",
				"Note", JOptionPane.YES_NO_OPTION);
		if (a != 0) {
			return;
		}
		
		String input = ta_command.getText();
		for (int i = 0; i < mm.getRowCount(); i++) {
			String command = input.replaceFirst("tihuan",
					mm.getValueAt(i, 2) + "@" + mm.getValueAt(i, 1));
			String[] cmd = { "/bin/bash", "-c", command };
			try {
				Runtime.getRuntime().exec(cmd);
			} catch (Exception e) {
				e.printStackTrace();
				JOptionPane.showMessageDialog(win,
						"ERROR! Fail to execute the command on " + mm.getValueAt(i, 0) + "!");
			}
		}
		JOptionPane.showMessageDialog(win,
				"Excute the command successfully!");
	}

	public void sshExe() {
		int a = JOptionPane.showConfirmDialog(null,
				"Are you sure to execute these commands?",
				"Note", JOptionPane.YES_NO_OPTION);
		if (a != 0) {
			return;
		}
		
		String[] commands = ta_command.getText().split("\n");
			try {
				for (int i = 0; i < mm.getRowCount(); i++) {
					Connection conn = new Connection((String) mm.getValueAt(i, 1));
					conn.connect();
					boolean isAuthenticated = conn.authenticateWithPassword(
							(String) mm.getValueAt(i, 2),
							(String) mm.getValueAt(i, 3));
					if (isAuthenticated) {
						Session sess;
						for (int j = 0; j < commands.length; j++) {
							sess = conn.openSession();
							sess.execCommand(commands[j]);
							sess.close();
						}
					}
					conn.close();
				}
				JOptionPane.showMessageDialog(win, "Excute the command successfully!");
			} catch (Exception e) {
				JOptionPane.showMessageDialog(win, "ERROR!\nFail to execute the command!");
			}	
	}
	
	public void clearExe() {
		ta_command.setText("");
	}
	
	public void browseHadoop() {
		String dir = "/home"; //缺省路径
		Component parent = null; //Dialog上级组件
		JFileChooser chooser = new JFileChooser(dir);
		javax.swing.filechooser.FileFilter dirFilter = new javax.swing.filechooser.FileFilter() {
			public boolean accept(File f) {
				return f.isDirectory();
			}

			public String getDescription() {
				return "";
			}
		};
		
		chooser.setFileFilter(dirFilter);
		chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		
		if (chooser.showOpenDialog(parent) == JFileChooser.APPROVE_OPTION) {
			dir = chooser.getSelectedFile().getAbsolutePath();
		}
		sourcePathHadoop.setText(dir);
	}
	
	public void browseBCBSP() {
		String dir = "/home"; //缺省路径
		Component parent = null; //Dialog上级组件
		JFileChooser chooser = new JFileChooser(dir);
		javax.swing.filechooser.FileFilter dirFilter = new javax.swing.filechooser.FileFilter() {
			public boolean accept(File f) {
				return f.isDirectory();
			}

			public String getDescription() {
				return "";
			}
		};
		
		chooser.setFileFilter(dirFilter);
		chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		
		if (chooser.showOpenDialog(parent) == JFileChooser.APPROVE_OPTION) {
			dir = chooser.getSelectedFile().getAbsolutePath();
		}
		sourcePathBCBSP.setText(dir);
	}

	public void deployHadoop() {
		int a = JOptionPane.showConfirmDialog(null,
				"Are you sure to deploy Hadoop?",
				"Note", JOptionPane.YES_NO_OPTION);
		if (a != 0) {
			return;
		}
		
		if (sourcePathHadoop.getText().toString().length() == 0 || this.HadoopHomePath.getText().toString().length() == 0) {
			JOptionPane.showMessageDialog(win,"Sorry!\nPlease set the source path & output path!");
			return;
		}
		
		boolean success = true;
		for (int i=0; i<mm.getRowCount(); i++) {
			String command = ("scp -r" + " " + sourcePathHadoop.getText().toString() 
					+ " " + mm.getValueAt(i, 2) + "@" + mm.getValueAt(i, 1) + ":" + this.HadoopHomePath.getText().toString());
				String[] cmd = { "/bin/bash", "-c", command };
				try {
					Process pro = Runtime.getRuntime().exec(cmd);
					if(pro.waitFor() == 0){
						ta_result.append("Deploy Hadoop on " + mm.getValueAt(i, 1) + " successfully!\n");
					}else{
						ta_result.append("Deploy Hadoop on " + mm.getValueAt(i, 1) + " fail!\n");
					}
				} catch (Exception e) {
					success =  false;
				}
		}
		if (success) {
			JOptionPane.showMessageDialog(win,"SUCCESS!\n Hadoop has been deployed on all workers!");
		} else {
			JOptionPane.showMessageDialog(win,"ERROR!\n Fail to deploy Hadoop on all workers!");
		}
	}
	
	public void deployBCBSP() {
		int a = JOptionPane.showConfirmDialog(null,
				"Are you sure to deploy BCBSP?",
				"Note", JOptionPane.YES_NO_OPTION);
		if (a != 0) {
			return;
		}
		
		if (sourcePathBCBSP.getText().toString().length() == 0 || this.BCBSPHomePath.getText().toString().length() == 0) {
			JOptionPane.showMessageDialog(win,"Sorry!\nPlease set the source path & output path!");
			return;
		}
		
		boolean success = true;
		for (int i=0; i<mm.getRowCount(); i++) {
			String command = ("scp -r" + " " + sourcePathBCBSP.getText().toString() 
					+ " " + mm.getValueAt(i, 2) + "@" + mm.getValueAt(i, 1) + ":" + this.BCBSPHomePath.getText().toString());
				String[] cmd = { "/bin/bash", "-c", command };
				try {
					Process pro = Runtime.getRuntime().exec(cmd);
					if(pro.waitFor() == 0){
						ta_result.append("Deploy BCBSP on " + mm.getValueAt(i, 1) + " successfully!\n");
					}else{
						ta_result.append("Deploy BCBSP on " + mm.getValueAt(i, 1) + " fail!\n");
					}
				} catch (Exception e) {
					success =  false;
				}
		}
		if (success) {
			JOptionPane.showMessageDialog(win,"SUCCESS!\n BCBSP has been deployed on all workers!");
		} else {
			JOptionPane.showMessageDialog(win,"ERROR!\n Fail to deploy BCBSP on all workers!");
		}
	}
	
	@SuppressWarnings("unchecked")
	public String getParentPath(Class cls){
		ClassLoader loader=cls.getClassLoader();
		String clsName=cls.getName()+".class";    
	    Package pack=cls.getPackage();   
	    String path="";     
	    if(pack!=null){   
	        String packName=pack.getName();   
	        clsName=clsName.substring(packName.length()+1);  
	        if(packName.indexOf(".")<0){
	        	path=packName+"/";   
	        }else{
	        	int start=0,end=0;   
	            end=packName.indexOf(".");   
	            while(end!=-1){   
	                path=path+packName.substring(start,end)+"/";   
	                start=end+1;   
	                end=packName.indexOf(".",start);   
	            }   
	            path=path+packName.substring(start)+"/";   
	        }   
	    }   
	    java.net.URL url =loader.getResource(path+clsName);     
	    String realPath=url.getPath();     
	    int pos=realPath.indexOf("file:");   
	    if(pos>-1) realPath=realPath.substring(pos+5);      
	    pos=realPath.indexOf(path+clsName);   
	    realPath=realPath.substring(0,pos-1);     
	    if(realPath.endsWith("!")){
	    	realPath=realPath.substring(0,realPath.lastIndexOf("/"));
	    }
	    try{
	    	realPath=java.net.URLDecoder.decode(realPath,"utf-8");   
	    }catch(Exception e){
	    	throw new RuntimeException(e);
	    }   
	   return realPath;
	}
	
	public static void main(String[] args) {

		EventQueue.invokeLater(new Runnable() {
			public void run() {
				win = new DeployGUI();
				win.setVisible(true);
			}
		});
	}
}
