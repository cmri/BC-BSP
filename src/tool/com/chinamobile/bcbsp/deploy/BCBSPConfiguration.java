/**
 * CopyRight by Chinamobile
 * 
 * BCBSPConfiguration.java
 */
package com.chinamobile.bcbsp.deploy;
import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;


public class BCBSPConfiguration {
	
	public class ButtonAction implements ActionListener {
		public void actionPerformed(ActionEvent e) {
			if (e.getSource() == update) {
				update();
			} else if (e.getSource() == cancel) {
				cancel();
			}
		}
	}
	
	public class WindowAction implements WindowListener {
		
		public void windowClosing(WindowEvent e) {
			 clean();
		}

		public void windowActivated(WindowEvent e) {};
		public void windowClosed(WindowEvent e) {};
		public void windowDeactivated(WindowEvent e) {};
		public void windowDeiconified(WindowEvent e) {};
		public void windowIconified(WindowEvent e) {};
		public void windowOpened(WindowEvent e) {};
	}
	
	private JFrame frame;
	private Container c;
	
	private JPanel top = new JPanel();
	private JPanel bottom = new JPanel();
	
	private String[] col = {"Name", "Value"};
	private DefaultTableModel mm = new DefaultTableModel(col, 0);
	private JTable table = new JTable(mm);
	private JScrollPane paramList = new JScrollPane();
	private JDesktopPane desktopPane_Param = new JDesktopPane();
	
	private JButton update = new JButton("Update");
	private JButton cancel = new JButton("Cancel");
	
	private String path = null;
	private String rootPath = null;
	private DefaultTableModel workers;
	private static String SKIP = "bcbsp.workermanager.staff.max";
	
	public BCBSPConfiguration(DeployGUI father, String masterName, String ipAddress, String userName,
			String path, String rootPath, DefaultTableModel workers) {
		this.path = path;
		this.rootPath = rootPath;
		this.workers = workers;
		this.frame = new JFrame("Configurate BCBSP Cluster");
		this.frame.setBounds(new Rectangle(
						(int) father.getBounds().getX() + 50,
						(int) father.getBounds().getY() + 50, 
						(int) father.getBounds().getWidth(), 
						(int) father.getBounds().getHeight()));
		
		this.c = this.frame.getContentPane();
		
		this.frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		this.c.setLayout(null);
		
		this.top.setLayout(null);
		this.bottom.setLayout(null);
		
		initialize(masterName, ipAddress, userName, path, rootPath);
		
		this.c.add(this.top);
		this.c.add(this.desktopPane_Param);
		this.c.add(this.bottom);
		this.frame.addWindowListener(new WindowAction());
		this.frame.setVisible(true);
	}
	
	private void initialize(String masterName, String ipAddress, String userName, String path, String rootPath) {
		this.top.setBounds(10, 10, 750, 20);
		JLabel topJL = new JLabel("Read the current configuration on BSPController: " + masterName + ":" + path);
		topJL.setBounds(0, 0, 750, 20);
		this.top.add(topJL);
		
		this.paramList.setViewportView(table);
		this.desktopPane_Param.setBounds(10, 35, 750, 400);
		JInternalFrame interFram_Param = new JInternalFrame("Advanced parameters",
				false, false, false, false);
		interFram_Param.setBounds(0, 0, 750, 400);
		interFram_Param.setVisible(true);
		interFram_Param.add(this.paramList, BorderLayout.CENTER);
		this.desktopPane_Param.add(interFram_Param);
		readDefaultConf(masterName, ipAddress, userName, path, rootPath);
		
		this.bottom.setBounds(10, 440, 750, 70);
		JLabel total = new JLabel("Total " + this.mm.getRowCount() + " configuration items");
		JLabel note_one = new JLabel("Note: the following parameters will be set according to WorkerServer List");
		JLabel note_two = new JLabel("        *the path of JDK in bcbsp-env.sh");
		JLabel note_three = new JLabel("        *the bcbsp.workermanager.staff.max per worker in bcbsp-site.xml");
		total.setBounds(0, 0, 550, 20);
		note_one.setBounds(0, 25, 550, 13);
		note_two.setBounds(0, 40, 550, 13);
		note_three.setBounds(0, 55, 550, 13);
		this.bottom.add(total);
		this.bottom.add(note_one);
		this.bottom.add(note_two);
		this.bottom.add(note_three);
		this.update.setBounds(560, 25, 80, 20);
		this.update.addActionListener(new ButtonAction());
		this.cancel.setBounds(670, 25, 80, 20);
		this.cancel.addActionListener(new ButtonAction());
		this.bottom.add(this.update);
		this.bottom.add(this.cancel);
	}
	
	/**
	 * Read the configuration file(bcbsp-site.xml) from the BSPController.
	 */
	private void readDefaultConf(String masterName, String ipAddress, String userName, String path, String rootPath) {
		String command = "scp " + userName + "@" + ipAddress + ":" + path + "/" + Util.BCBSPConf.BCBSP_CONF_DIR + "/"
				+ Util.BCBSPConf.BCBSP_CONF_SITE_FILE + " " + rootPath + "/"
				+ Util.SystemConf.DEPLOY_TEMP_DIR + "/";
		String[] getCmd = { "/bin/bash", "-c", command };
		try {
			Process getP = Runtime.getRuntime().exec(getCmd);
			getP.waitFor();
			File initFile = new File(rootPath + "/"
					+ Util.SystemConf.DEPLOY_TEMP_DIR + "/"
					+ Util.BCBSPConf.BCBSP_CONF_SITE_FILE);
			File sourceFile = new File(rootPath + "/"
					+ Util.SystemConf.DEPLOY_TEMP_DIR + "/"
					+ Util.BCBSPConf.BCBSP_CONF_SITE_FILE + ".tmp");
			initFile.renameTo(sourceFile);
			FileReader fr = new FileReader(sourceFile);
			BufferedReader br = new BufferedReader(fr, 65536);
			
			String read, content;
			while ((read = br.readLine()) != null) {
				content = Util.XML.filter(read, Util.XML.PROPERTY_NAME_START, Util.XML.PROPERTY_NAME_END);
				if (content != null) {
					if (content.equals(SKIP)) {
						continue;
					}
					Vector<String> row = new Vector<String>();
					row.add(content);
					read = br.readLine();
					content = Util.XML.filter(read, Util.XML.PROPERTY_VALUE_START, Util.XML.PROPERTY_VALUE_END);
					row.add(content);
					this.mm.addRow(row);
				}
			}

			br.close();
			fr.close();
			sourceFile.delete();
		} catch (Exception e) {
			JOptionPane.showMessageDialog(this.frame,
					"ERROR!\nFail to get bcbsp-site.xml file at master: " + masterName + " !");
			//e.printStackTrace();
		}
	}
	
	/**
	 * Clean up resources before quit the configuration window.
	 */
	private void clean() {
		
	}
	
	public void update() {
		int a = JOptionPane.showConfirmDialog(null, "Are you sure to update configuration info on every worker?", "Note",
			      JOptionPane.YES_NO_OPTION);
		if (a == 0) {
			 try {
				for (int index = 0; index < this.workers.getRowCount(); index++) {
					File dstFile = new File(rootPath + "/"
							 + Util.SystemConf.DEPLOY_TEMP_DIR + "/"
							 + Util.BCBSPConf.BCBSP_CONF_SITE_FILE);
					FileWriter fw = new FileWriter(dstFile);
					BufferedWriter bw = new BufferedWriter(fw);
					
					Util.XML.writeHeader(bw);
					for (int i = 0; i < this.mm.getRowCount(); i++) {
						Util.XML.writeRecord(bw, this.mm.getValueAt(i, 0).toString(), this.mm.getValueAt(i, 1).toString());
					}
					Util.XML.writeRecord(bw, SKIP, this.workers.getValueAt(1, 4).toString());
					Util.XML.writeEnd(bw);
						
					bw.close();
					fw.close();
					String command = "scp " + dstFile.toString() + " " + this.workers.getValueAt(index, 2).toString()
							+ "@" + this.workers.getValueAt(index, 1) + ":" + path + "/" + Util.BCBSPConf.BCBSP_CONF_DIR + "/";
					//System.out.println(command);
					String[] backCmd = {"/bin/bash","-c",command};
					Process backP = Runtime.getRuntime().exec(backCmd);
					backP.waitFor();
					dstFile.delete();
						
					dstFile = new File(rootPath + "/"
							+ Util.SystemConf.DEPLOY_TEMP_DIR + "/"
							+ Util.BCBSPConf.BCBSP_CONF_ENV_FILE);
					fw = new FileWriter(dstFile);
					fw.write("export JAVA_HOME=" + this.workers.getValueAt(index, 5));
					fw.close();
					command = "scp " + dstFile.toString() + " " + this.workers.getValueAt(index, 2).toString()
							+ "@" + this.workers.getValueAt(index, 1) + ":" + path + "/" + Util.BCBSPConf.BCBSP_CONF_DIR + "/";
					//System.out.println(command);
					backCmd[2] = command;
					backP = Runtime.getRuntime().exec(backCmd);
					backP.waitFor();
					dstFile.delete();
				}
				JOptionPane.showMessageDialog(this.frame, "Update successfully!");
			 } catch (Exception e) {
				 JOptionPane.showMessageDialog(this.frame, "ERROR!\nOperation failed!"); 
			 }
		 } 
	}
	
	public void cancel() {
		clean();
		this.frame.dispose();
	}
}
