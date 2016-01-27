package org.apache.storm.container;

import java.util.Map;

/**
 * A plugin to support resource isolation and limitation within Storm
 */
public interface ResourceIsolationInterface {

    /**
     * @param workerId worker id of the worker to start
     * @param resources set of resources to limit
     * @return a String that includes to command on how to start the worker.  The string returned from this function
     * will be concatenated to the front of the command to launch logwriter/worker in supervisor.clj
     */
    public String startNewWorker(String workerId, Map resources);

    /**
     * This function will be called when the worker needs to shutdown.  This function should include logic to clean up after a worker is shutdown
     * @param workerId worker id to shutdown and clean up after
     * @param isKilled whether to actually kill worker
     */
    public void shutDownWorker(String workerId, boolean isKilled);

}
