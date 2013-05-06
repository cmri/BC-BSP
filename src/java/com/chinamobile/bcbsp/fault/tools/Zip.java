/**
 * CopyRight by Chinamobile
 * 
 * Zip.java
 */
package com.chinamobile.bcbsp.fault.tools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Zip {
    
    public static final Log LOG = LogFactory.getLog(Zip.class);
	private static final int BUFFER = 1024;

	/**
	 * Compress files to *.zip.
	 * 
	 * @param fileName
	 *            : file to be compress
	 */
	public static String compress(File file){
		return compress(file.getAbsolutePath());
	}
	public static String compress(String fileName) {
		String targetFile = null;
		File sourceFile = new File(fileName);
		Vector<File> vector = getAllFiles(sourceFile);
		try {
			if (sourceFile.isDirectory()) {
				targetFile = fileName + ".zip";
			} else {
				char ch = '.';
				targetFile = fileName.substring(0,
						fileName.lastIndexOf((int) ch))
						+ ".zip";
			}
			BufferedInputStream bis = null;
			BufferedOutputStream bos = new BufferedOutputStream(
					new FileOutputStream(targetFile));
			ZipOutputStream zipos = new ZipOutputStream(bos);
			byte data[] = new byte[BUFFER];
			if(vector.size()>0){
			for (int i = 0; i < vector.size(); i++) {
				File file = vector.get(i);
				zipos.putNextEntry(new ZipEntry(getEntryName(fileName, file))); 
																			
				bis = new BufferedInputStream(new FileInputStream(file));

				int count;
				while ((count = bis.read(data, 0, BUFFER)) != -1) {
					zipos.write(data, 0, count);
				}
				bis.close();
				zipos.closeEntry();
			}
			zipos.close();
			bos.close();
			return targetFile;
			}else{
			File zipNullfile=	new File(targetFile);
			zipNullfile.getParentFile().mkdirs();
			zipNullfile.mkdir();
			return zipNullfile.getAbsolutePath();
			}
		} catch (IOException e) {
			LOG.error("[compress]", e);
			return "error";
		}
	}

	/**
	 * Uncompress *.zip files.
	 * 
	 * @param fileName
	 *            : file to be uncompress
	 */
	@SuppressWarnings("unchecked")
    public static void decompress(String fileName) {
		File sourceFile = new File(fileName);
		String filePath = sourceFile.getParent() + "/";
		try {
			BufferedInputStream bis = null;
			BufferedOutputStream bos = null;
			ZipFile zipFile = new ZipFile(fileName);
			Enumeration en = zipFile.entries();
			byte data[] = new byte[BUFFER];
			while (en.hasMoreElements()) {
				ZipEntry entry = (ZipEntry) en.nextElement();
				if (entry.isDirectory()) {
					new File(filePath + entry.getName()).mkdirs();
					continue;
				}
				bis = new BufferedInputStream(zipFile.getInputStream(entry));
				File file = new File(filePath + entry.getName());
				File parent = file.getParentFile();
				if (parent != null && (!parent.exists())) {
					parent.mkdirs();
				}
				bos = new BufferedOutputStream(new FileOutputStream(file));
				int count;
				while ((count = bis.read(data, 0, BUFFER)) != -1) {
					bos.write(data, 0, count);
				}
				bis.close();
				bos.close();
			}
			zipFile.close();
			
		} catch (IOException e) {
		    LOG.error("[compress]", e);
		}
	}

	/**
	 * To get a directory's all files.
	 * 
	 * @param sourceFile
	 *            : the source directory
	 * @return the files' collection
	 */
	private static Vector<File> getAllFiles(File sourceFile) {
		Vector<File> fileVector = new Vector<File>();
		if (sourceFile.isDirectory()) {
			File[] files = sourceFile.listFiles();
			for (int i = 0; i < files.length; i++) {
				fileVector.addAll(getAllFiles(files[i]));
			}
		} else {
			fileVector.add(sourceFile);
		}
		return fileVector;
	}

	private static String getEntryName(String base, File file) {
		File baseFile = new File(base);
		String filename = file.getPath();
		if (baseFile.getParentFile().getParentFile() == null)
			return filename.substring(baseFile.getParent().length());  // d:/file1  /   file2/text.txt  
		return filename.substring(baseFile.getParent().length()+1);    //d:/  file1/text.txt
	}
}
