package org.apache.storm;

import org.junit.Assert;
import org.junit.Assume;
import org.apache.storm.container.cgroup.CgroupManager;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by jerrypeng on 1/19/16.
 */
public class TestCGroup {
    private static final Logger LOG = LoggerFactory.getLogger(TestCGroup.class);

//    @Test
//    public void test() throws IOException {
//        Config config = new Config();
//        config.putAll(Utils.readDefaultConfig());
//        //config.put(Config.CGROUP_SUPERVISOR_ROOTDIR, "storm");
//
//        CgroupManager manager = new CgroupManager();
//        manager.prepare(config);
//
//        Map<String, Integer> resourcesMap = new HashMap<String, Integer>();
//        resourcesMap.put("cpu", 200);
//        resourcesMap.put("memory", 1024);
//        String workerId = UUID.randomUUID().toString();
//        LOG.info("Starting worker {} Commandline: {}", workerId, manager.startNewWorker(workerId, resourcesMap));
//
//        //manager.shutDownWorker(workerId, true);
//
//    }



    @Test
    public void testSetupAndTearDown() throws IOException {
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        //We don't want to run the test is CGroups are not setup
        Assume.assumeTrue("Check if CGroups are setup", ((boolean) config.get(Config.CGROUP_ENABLE)) == true);

        Assert.assertTrue("Check if CGROUP_STORM_HIERARCHY_DIR exists", stormCGroupHierarchyExists(config));
        Assert.assertTrue("Check if CGROUP_SUPERVISOR_ROOTDIR exists", stormCGroupSupervisorRootDirExists(config));

        CgroupManager manager = new CgroupManager();
        manager.prepare(config);

        Map<String, Object> resourcesMap = new HashMap<String, Object>();
        resourcesMap.put("cpu", 200);
        resourcesMap.put("memory", 1024);
        String workerId = UUID.randomUUID().toString();
        String command = manager.startNewWorker(workerId, resourcesMap);

        String correctCommand = config.get(Config.CGROUP_CGEXEC_CMD) + " -g memory,cpu:/"
                + config.get(Config.CGROUP_SUPERVISOR_ROOTDIR) + "/" + workerId;
        Assert.assertEquals("Check if cgroup launch command is correct", correctCommand, command);

        LOG.info("Starting worker {} Commandline: {}", workerId, command);

        String pathToWorkerCGroupDir = ((String) config.get(Config.CGROUP_STORM_HIERARCHY_DIR))
                + "/" + ((String) config.get(Config.CGROUP_SUPERVISOR_ROOTDIR)) + "/" + workerId;

        Assert.assertTrue("Check if cgroup directory exists for worker", dirExists(pathToWorkerCGroupDir));

        /* validate cpu settings */

        String pathToCpuShares = pathToWorkerCGroupDir + "/cpu.shares";
        Assert.assertTrue("Check if cpu.shares file exists", fileExists(pathToCpuShares));
        Assert.assertEquals("Check if the correct value is written into cpu.shares", "200", readFileAll(pathToCpuShares));

        /* validate memory settings */

        String pathTomemoryLimitInBytes = pathToWorkerCGroupDir + "/memory.limit_in_bytes";

        Assert.assertTrue("Check if memory.limit_in_bytes file exists", fileExists(pathTomemoryLimitInBytes));
        Assert.assertEquals("Check if the correct value is written into memory.limit_in_bytes", String.valueOf(1024 * 1024 * 1024), readFileAll(pathTomemoryLimitInBytes));

        manager.shutDownWorker(workerId, true);

        Assert.assertFalse("Make sure cgroup was removed properly", dirExists(pathToWorkerCGroupDir));
    }

    public boolean stormCGroupHierarchyExists(Map config) {
        String pathToStormCGroupHierarchy = (String) config.get(Config.CGROUP_STORM_HIERARCHY_DIR);
        return dirExists(pathToStormCGroupHierarchy);
    }

    public boolean stormCGroupSupervisorRootDirExists(Map config) {
        String pathTostormCGroupSupervisorRootDir = ((String) config.get(Config.CGROUP_STORM_HIERARCHY_DIR))
                + "/" + ((String) config.get(Config.CGROUP_SUPERVISOR_ROOTDIR));

        return dirExists(pathTostormCGroupSupervisorRootDir);
    }

    public boolean dirExists(String rawPath) {
        File path = new File(rawPath);
        return path.exists() && path.isDirectory();
    }

    public boolean fileExists(String rawPath) {
        File path = new File(rawPath);
        return path.exists() && !path.isDirectory();
    }

    public String readFileAll(String filePath) throws IOException {
        byte[] data = Files.readAllBytes(Paths.get(filePath));
        LOG.info("data: {}", data);
        return new String(data).trim();
    }
}