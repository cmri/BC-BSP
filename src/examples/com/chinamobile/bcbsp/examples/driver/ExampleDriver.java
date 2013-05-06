/**
 * ExampleDriver.java
 */
package com.chinamobile.bcbsp.examples.driver;

import org.apache.hadoop.util.ProgramDriver;

import com.chinamobile.bcbsp.examples.kmeans.KMeansDriver;
import com.chinamobile.bcbsp.examples.pagerank.PageRankDriver;
import com.chinamobile.bcbsp.examples.sssp.SSPDriver;

/**
 * ExampleDriver
 * An entry to drive examples of BC-BSP.
 * Now, only implement three examples.
 * User can input "pagerank", "sssp" and "kmeans" to choose one example to run.
 * 
 * @author Zhigang Wang
 * @version 1.0
 */

public class ExampleDriver {
	
	public static void main(String argv[]){
	    int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    
	    try {
	    	pgd.addClass("pagerank", PageRankDriver.class, 
        		"An example program that computes the pagerank value of web pages.");
	    	pgd.addClass("sssp", SSPDriver.class, 
        		"An example program that computes the single source shortest path problem.");
	    	pgd.addClass("kmeans", KMeansDriver.class, 
            	"An example program that computes the k-means algorithm.");
	    	
	    	pgd.driver(argv);
	    	exitCode = 0;
	    } catch(Throwable e) {
	    	e.printStackTrace();
	    }
	    
	    System.exit(exitCode);
	}
}
