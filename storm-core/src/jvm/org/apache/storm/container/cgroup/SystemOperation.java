package org.apache.storm.container.cgroup;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by jerrypeng on 1/19/16.
 */
public class SystemOperation {

    public static final Logger LOG = LoggerFactory.getLogger(SystemOperation.class);

    public static boolean isRoot() throws IOException {
        String result = SystemOperation.exec("echo $EUID").substring(0, 1);
        return Integer.valueOf(result.substring(0, result.length())).intValue() == 0 ? true : false;
    };

    public static void mount(String name, String target, String type, String data) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("mount -t ").append(type).append(" -o ").append(data).append(" ").append(name).append(" ").append(target);
        SystemOperation.exec(sb.toString());
    }

    public static void umount(String name) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("umount ").append(name);
        SystemOperation.exec(sb.toString());
    }

    public static String exec(String cmd) throws IOException {
        LOG.debug("Shell cmd: " + cmd);
        Process process = new ProcessBuilder(new String[] { "/bin/bash", "-c", cmd }).start();
        try {
            process.waitFor();
            String output = IOUtils.toString(process.getInputStream());
            String errorOutput = IOUtils.toString(process.getErrorStream());
            LOG.debug("Shell Output: " + output);
            if (errorOutput.length() != 0) {
                LOG.error("Shell Error Output: " + errorOutput);
                throw new IOException(errorOutput);
            }
            return output;
        } catch (InterruptedException ie) {
            throw new IOException(ie.toString());
        }

    }

    public static void main(String[] args) throws IOException {
        SystemOperation.mount("test", "/cgroup/cpu", "cgroup", "cpu");
    }
}