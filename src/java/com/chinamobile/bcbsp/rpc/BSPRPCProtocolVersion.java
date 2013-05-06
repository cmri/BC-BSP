/**
 * CopyRight by Chinamobile
 * 
 * BSPRPCProtocolVersion.java
 */
package com.chinamobile.bcbsp.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * BSPRPCProtovolVersion
 * 
 * There is one version id for all the RPC interfaces. If any interface is
 * changed, the versionID must be changed here.
 * 
 * @author
 * @version
 */
public interface BSPRPCProtocolVersion extends VersionedProtocol {
    public static final long versionID = 0L;
}
