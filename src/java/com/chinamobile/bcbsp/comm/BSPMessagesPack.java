/**
 * CopyRight by Chinamobile
 * 
 * BSPMessagesPack.java
 */
package com.chinamobile.bcbsp.comm;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * BSPMessagesPack.
 * To pack the messages into array list for sending
 * and receiving all together.
 * 
 * @author
 * @version
 */
public class BSPMessagesPack implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private ArrayList<BSPMessage> pack;
    
    public BSPMessagesPack() {
        
    }
    
    public void setPack(ArrayList<BSPMessage> pack) {
        this.pack = pack;
    }
    
    public ArrayList<BSPMessage> getPack() {
        return this.pack;
    }

}
