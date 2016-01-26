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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CgroupManager implements ResourceIsolationInterface {

    public static final Logger LOG = LoggerFactory.getLogger(CgroupManager.class);

    private CgroupCenter center;

    private Hierarchy h;

    private CgroupCommon rootCgroup;

    private static String rootDir;

    public CgroupManager(Map conf) {
        LOG.info("running on cgroup mode");

        // Cgconfig service is used to create the corresponding cpu hierarchy
        // "/cgroup/cpu"
        rootDir = Config.getCgroupRootDir(conf);
        if (rootDir == null)
            throw new RuntimeException("Check configuration file. The supervisor.cgroup.rootdir is missing.");

        File file = new File(Config.getCGroupStormHierarchyDir(conf) + "/" + rootDir);
        if (!file.exists()) {
            LOG.error(Config.getCGroupStormHierarchyDir(conf) + "/" + rootDir + " is not existing.");
            throw new RuntimeException("Check if cgconfig service starts or /etc/cgconfig.conf is consistent with configuration file.");
        }
        center = CgroupCenter.getInstance();
        if (center == null)
            throw new RuntimeException("Cgroup error, please check /proc/cgroups");
        this.prepareSubSystem(conf);
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
        Integer cpuNum = (Integer) resourcesMap.get("cpu");
        Long totalMem = null;
        if (resourcesMap.get("memory") != null) {
            LOG.info("memory class: {}", resourcesMap.get("memory").getClass());
            totalMem = new Long ((Long) resourcesMap.get("memory"));

        }
        LOG.info("cpuNum {} totalMem {}", cpuNum, totalMem);

        CgroupCommon workerGroup = new CgroupCommon(workerId, h, this.rootCgroup);
        this.center.create(workerGroup);

        if (cpuNum != null) {
            CpuCore cpuCore = (CpuCore) workerGroup.getCores().get(SubSystemType.cpu);;

            try {
                cpuCore.setCpuShares(cpuNum);
            } catch (IOException e) {
                LOG.info("Exception thown: {}", e);
                throw new RuntimeException("Cannot set cpu shares!");
            }
        }

        if (totalMem != null) {
            MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
            try {
                LOG.info("mem: {} - {}", Long.valueOf(totalMem * 1024 * 1024), (totalMem * 1024 * 1024));
                memCore.setPhysicalUsageLimit(Long.valueOf(totalMem * 1024 * 1024));
            } catch (IOException e) {
                LOG.info("Exception thown: {}", e);
                throw new RuntimeException("Cannot set MEMORY_LIMIT_IN_BYTES !");
            }
        }
        //setCpuUsageUpperLimit(cpuCore, ConfigExtension.getWorkerCpuCoreUpperLimit(conf));

        StringBuilder sb = new StringBuilder();

        //sb.append("/bin/cgexec -g ");

        //todo need to modify command line to include memory

        Iterator<SubSystemType> it = h.getSubSystems().iterator();
        while(it.hasNext()) {
            sb.append(it.next().toString());
            if(it.hasNext()) {
                sb.append(",");
            } else {
                sb.append(":");
            }
        }

        //sb.append(workerGroup.getName()).append(" ");
        sb.append(workerGroup.getName());

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
            LOG.info("deleting cgroup: {} dir {}", workerGroup.getName(), workerGroup.getDir());
            center.delete(workerGroup);
        } catch (Exception e) {
            LOG.info("No task of " + workerId);
        }

    }

    public void close() throws IOException {
        this.center.delete(this.rootCgroup);
    }

    private void prepareSubSystem(Map conf) {
        List<SubSystemType> subSystemTypes = new LinkedList<>();
        for (String resource : Config.getCGroupStormResources(conf)) {
            subSystemTypes.add(SubSystemType.getSubSystem(resource));
        }

        LOG.info("subSystemTypes: {}", subSystemTypes);
        h = center.busy(subSystemTypes);

        LOG.info("Heirarchy name {} dir {} subsystems {} RootCgroups {} type {}", h.getName(), h.getDir(), h.getSubSystems(), h.getRootCgroups(), h.getType());

        if (h == null) {
            Set<SubSystemType> types = new HashSet<SubSystemType>();
            types.add(SubSystemType.cpu);
            h = new Hierarchy(Config.getCGroupStormHierarchyName(conf), types, Config.getCGroupStormHierarchyDir(conf));
        }
        rootCgroup = new CgroupCommon(rootDir, h, h.getRootCgroups());
    }
}
