package org.apache.storm;

import org.apache.storm.container.cgroup.CgroupManager;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by jerrypeng on 1/19/16.
 */
public class TestCGroup {
    private static final Logger LOG = LoggerFactory.getLogger(TestCGroup.class);

    @Test
    public void test() throws IOException {
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        //config.put(Config.CGROUP_SUPERVISOR_ROOTDIR, "storm");

        CgroupManager manager = new CgroupManager(config);

        Map<String, Integer> resourcesMap = new HashMap<String, Integer>();
        resourcesMap.put("cpu", 200);
        resourcesMap.put("memory", 1024);
        String workerId = UUID.randomUUID().toString();
        LOG.info("Starting worker {} Commandline: {}", workerId, manager.startNewWorker(config, resourcesMap, workerId));

        //manager.shutDownWorker(workerId, true);

    }
}

