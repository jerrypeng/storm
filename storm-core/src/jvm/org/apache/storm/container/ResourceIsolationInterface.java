package org.apache.storm.container;

import java.io.IOException;
import java.util.Map;

/**
 * Created by jerrypeng on 1/19/16.
 */
public interface ResourceIsolationInterface {


    public String startNewWorker(Map conf, Map resources, String workerId);

    public void shutDownWorker(String workerId, boolean isKilled);

}
