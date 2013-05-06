/**
 * CopyRight by Chinamobile
 * 
 * Combiner.java
 */
package com.chinamobile.bcbsp.api;

import java.util.Iterator;

import com.chinamobile.bcbsp.comm.BSPMessage;

/**
 * Combiner
 * Abstract class to be extended by user to implement 
 * combine operation to the messages sent to the same
 * vertex.
 * 
 * @author
 * @version
 */
public abstract class Combiner {

    /**
     * Combine the messages to generate a new message,
     * and return it.
     * 
     * @param messages
     *              Iterator<BSPMessage>
     * @return message
     *              BSPMessage
     */
    abstract public BSPMessage combine(Iterator<BSPMessage> messages);

}
