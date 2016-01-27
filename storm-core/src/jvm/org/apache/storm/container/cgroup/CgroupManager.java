package org.apache.storm.container.cgroup;

import org.apache.storm.Config;
import org.apache.storm.container.ResourceIsolationInterface;
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

    private Map conf;

    public void prepare(Map conf) throws IOException {
        this.conf = conf;
        this.rootDir = Config.getCgroupRootDir(this.conf);
        if (this.rootDir == null)
            throw new RuntimeException("Check configuration file. The supervisor.cgroup.rootdir is missing.");

        File file = new File(Config.getCGroupStormHierarchyDir(conf) + "/" + this.rootDir);
        if (!file.exists()) {
            LOG.error(Config.getCGroupStormHierarchyDir(conf) + "/" + this.rootDir + " is not existing.");
            throw new RuntimeException("Check if cgconfig service starts or /etc/cgconfig.conf is consistent with configuration file.");
        }
        this.center = CgroupCenter.getInstance();
        if (this.center == null)
            throw new RuntimeException("Cgroup error, please check /proc/cgroups");
        this.prepareSubSystem(this.conf);
    }

    /**
     * User cfs_period & cfs_quota to control the upper limit use of cpu core e.g. If making a process to fully use two cpu cores, set cfs_period_us to
     * 100000 and set cfs_quota_us to 200000
     */
    private void setCpuUsageUpperLimit(CpuCore cpuCore, int cpuCoreUpperLimit) throws IOException {

        if (cpuCoreUpperLimit == -1) {
            // No control of cpu usage
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit);
        } else {
            cpuCore.setCpuCfsPeriodUs(100000);
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit * 1000);
        }
    }

    public String startNewWorker(String workerId, Map resourcesMap) throws SecurityException {
        Number cpuNum = (Number) resourcesMap.get("cpu");
        Number totalMem = null;
        if (resourcesMap.get("memory") != null) {
            totalMem = (Number) resourcesMap.get("memory");

        }

        CgroupCommon workerGroup = new CgroupCommon(workerId, h, this.rootCgroup);
        this.center.create(workerGroup);

        if (cpuNum != null) {
            CpuCore cpuCore = (CpuCore) workerGroup.getCores().get(SubSystemType.cpu);
            try {
                cpuCore.setCpuShares(cpuNum.intValue());
            } catch (IOException e) {
                throw new RuntimeException("Cannot set cpu.shares! Exception: " + e);
            }
        }

        if (totalMem != null) {
            MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
            try {
                memCore.setPhysicalUsageLimit(Long.valueOf(totalMem.longValue() * 1024 * 1024));
            } catch (IOException e) {
                throw new RuntimeException("Cannot set memory.limit_in_bytes! Exception: " + e);
            }
        }

        StringBuilder sb = new StringBuilder();

        sb.append(this.conf.get(Config.CGROUP_CGEXEC_CMD)).append(" -g ");

        Iterator<SubSystemType> it = h.getSubSystems().iterator();
        while(it.hasNext()) {
            sb.append(it.next().toString());
            if(it.hasNext()) {
                sb.append(",");
            } else {
                sb.append(":");
            }
        }

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
            Set<Integer> tasks = workerGroup.getTasks();
            if (isKilled == true && !tasks.isEmpty()) {
                throw new Exception("Cannot correctly showdown worker CGroup " + workerId + "tasks " + tasks.toString() + " still running!");
            }
            this.center.delete(workerGroup);
        } catch (Exception e) {
            LOG.info("Exception thrown when shutting worker " + workerId + " Exception: " + e);
        }
    }

    public void close() throws IOException {
        this.center.delete(this.rootCgroup);
    }

    private void prepareSubSystem(Map conf) throws IOException {
        List<SubSystemType> subSystemTypes = new LinkedList<>();
        for (String resource : Config.getCGroupStormResources(conf)) {
            subSystemTypes.add(SubSystemType.getSubSystem(resource));
        }

        this.h = center.busy(subSystemTypes);

        if (this.h == null) {
            Set<SubSystemType> types = new HashSet<SubSystemType>();
            types.add(SubSystemType.cpu);
            this.h = new Hierarchy(Config.getCGroupStormHierarchyName(conf), types, Config.getCGroupStormHierarchyDir(conf));
        }
        this.rootCgroup = new CgroupCommon(this.rootDir, this.h, this.h.getRootCgroups());

        // set upper limit to how much cpu can be used by all workers running on supervisor node.
        // This is done so that some cpu cycles will remain free to run the daemons and other miscellaneous OS operations.
        CpuCore supervisorRootCPU = (CpuCore)  this.rootCgroup.getCores().get(SubSystemType.cpu);
        setCpuUsageUpperLimit(supervisorRootCPU, ((Number) this.conf.get(Config.SUPERVISOR_CPU_CAPACITY)).intValue());
    }
}
