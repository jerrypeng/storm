package org.apache.storm;

import org.apache.storm.container.CgroupManager;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by jerrypeng on 1/19/16.
 */
public class TestCGroup {
    private static final Logger LOG = LoggerFactory.getLogger(TestCGroup.class);

    @Test
    public void test() throws IOException {
        Config config = new Config();
        config.put(Config.SUPERVISOR_CGROUP_ROOTDIR, "storm");
        config.putAll(Utils.readDefaultConfig());

        CgroupManager manager = new CgroupManager(config);

        LOG.info("Commandline: {}", manager.startNewWorker(config, 2 , UUID.randomUUID().toString()));

    }
}

