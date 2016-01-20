package org.apache.storm.container.cgroup;

import org.apache.storm.Config;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.container.cgroup.core.CgroupCore;
import org.apache.storm.container.cgroup.core.CpuCore;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CgroupManager implements ResourceIsolationInterface {

    public static final Logger LOG = LoggerFactory.getLogger(CgroupManager.class);

    public static final String JSTORM_HIERARCHY_NAME = "jstorm_cpu";

    //public static final int ONE_CPU_SLOT = 1024;

    private CgroupCenter center;

    private Hierarchy h;

    private CgroupCommon rootCgroup;

    private static final String JSTORM_CPU_HIERARCHY_DIR = "/cgroup/cpu";
    private static String rootDir;

    public CgroupManager(Map conf) {
        LOG.info("running on cgroup mode");

        // Cgconfig service is used to create the corresponding cpu hierarchy
        // "/cgroup/cpu"
        rootDir = Config.getCgroupRootDir(conf);
        if (rootDir == null)
            throw new RuntimeException("Check configuration file. The supervisor.cgroup.rootdir is missing.");

        File file = new File(JSTORM_CPU_HIERARCHY_DIR + "/" + rootDir);
        if (!file.exists()) {
            LOG.error(JSTORM_CPU_HIERARCHY_DIR + "/" + rootDir + " is not existing.");
            throw new RuntimeException("Check if cgconfig service starts or /etc/cgconfig.conf is consistent with configuration file.");
        }
        center = CgroupCenter.getInstance();
        if (center == null)
            throw new RuntimeException("Cgroup error, please check /proc/cgroups");
        this.prepareSubSystem();
    }

    private int validateCpuUpperLimitValue(int value) {
        /*
         * Valid value is -1 or 1~10 -1 means no control
         */
        if (value > 10)
            value = 10;
        else if (value < 1 && value != -1)
            value = 1;

        return value;
    }

    private void setCpuUsageUpperLimit(CpuCore cpuCore, int cpuCoreUpperLimit) throws IOException {
        /*
         * User cfs_period & cfs_quota to control the upper limit use of cpu core e.g. If making a process to fully use two cpu cores, set cfs_period_us to
         * 100000 and set cfs_quota_us to 200000 The highest value of "cpu core upper limit" is 10
         */
        cpuCoreUpperLimit = validateCpuUpperLimitValue(cpuCoreUpperLimit);

        if (cpuCoreUpperLimit == -1) {
            // No control of cpu usage
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit);
        } else {
            cpuCore.setCpuCfsPeriodUs(100000);
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit * 100000);
        }
    }

    public String startNewWorker(Map conf, Map resourcesMap, String workerId) throws SecurityException {
        LOG.info("resourcesMap: {}", resourcesMap);
        int cpuNum = (int) resourcesMap.get("cpu");
        int onHeapMem = (int) resourcesMap.get("mem-onheap");
        int offHeapMem = (int) resourcesMap.get("mem-offheap");
        int totalMem = onHeapMem + offHeapMem;

        CgroupCommon workerGroup = new CgroupCommon(workerId, h, this.rootCgroup);
        this.center.create(workerGroup);
        CgroupCore cpu = workerGroup.getCores().get(SubSystemType.cpu);
        CpuCore cpuCore = (CpuCore) cpu;
        MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);

        try {
            cpuCore.setCpuShares(cpuNum);
            memCore.setPhysicalUsageLimit(totalMem * 1024 * 1024);
        } catch (IOException e) {
            throw new RuntimeException("Cannot set cpu shares!");
        }
        //setCpuUsageUpperLimit(cpuCore, ConfigExtension.getWorkerCpuCoreUpperLimit(conf));

        StringBuilder sb = new StringBuilder();
        sb.append("cgexec -g cpu:").append(workerGroup.getName()).append(" ");
        return sb.toString();
    }

    public void shutDownWorker(String workerId, boolean isKilled) {
        CgroupCommon workerGroup = new CgroupCommon(workerId, h, this.rootCgroup);
        try {
            if (isKilled == false) {
                for (Integer pid : workerGroup.getTasks()) {
                    Utils.kill(pid);
                }
                Utils.sleepMs(1500);
            }
            center.delete(workerGroup);
        } catch (Exception e) {
            LOG.info("No task of " + workerId);
        }

    }

    public void close() throws IOException {
        this.center.delete(this.rootCgroup);
    }

    private void prepareSubSystem() {
        h = center.busy(SubSystemType.cpu);
        if (h == null) {
            Set<SubSystemType> types = new HashSet<SubSystemType>();
            types.add(SubSystemType.cpu);
            h = new Hierarchy(JSTORM_HIERARCHY_NAME, types, JSTORM_CPU_HIERARCHY_DIR);
        }
        rootCgroup = new CgroupCommon(rootDir, h, h.getRootCgroups());
    }
}
