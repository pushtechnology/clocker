/*
 * Copyright 2014-2016 by Cloudsoft Corporation Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package clocker.docker.entity;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.apache.brooklyn.util.ssh.BashCommands.INSTALL_CURL;
import static org.apache.brooklyn.util.ssh.BashCommands.alternatives;
import static org.apache.brooklyn.util.ssh.BashCommands.chain;
import static org.apache.brooklyn.util.ssh.BashCommands.chainGroup;
import static org.apache.brooklyn.util.ssh.BashCommands.ifExecutableElse0;
import static org.apache.brooklyn.util.ssh.BashCommands.ifExecutableElse1;
import static org.apache.brooklyn.util.ssh.BashCommands.installPackage;
import static org.apache.brooklyn.util.ssh.BashCommands.sudo;
import io.brooklyn.entity.nosql.etcd.EtcdCluster;
import io.brooklyn.entity.nosql.etcd.EtcdNode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import clocker.docker.entity.util.DockerUtils;
import clocker.docker.entity.util.JcloudsHostnameCustomizer;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;

import org.jclouds.compute.ComputeService;

import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.OsDetails;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.entity.group.AbstractGroup;
import org.apache.brooklyn.entity.group.DynamicGroup;
import org.apache.brooklyn.entity.software.base.AbstractSoftwareProcessSshDriver;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.lifecycle.ScriptHelper;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.file.ArchiveUtils;
import org.apache.brooklyn.util.core.file.ArchiveUtils.ArchiveType;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.ssh.BashCommands;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.VersionComparator;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;

public class DockerHostSshDriver extends AbstractSoftwareProcessSshDriver implements DockerHostDriver {

    public DockerHostSshDriver(DockerHostImpl entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    @Override
    public Integer getDockerPort() {
        return getEntity().sensors().get(DockerHost.DOCKER_SSL_PORT);
    }

    /** {@inheritDoc} */
    @Override
    public String buildImage(String dockerfile, Optional<String> entrypoint, Optional<String> contextArchive, String name, boolean useSsh, Map<String, Object> substitutions) {
        String imageId;
        String imageDir =  Os.mergePaths(getRunDir(), name);
        if (!ArchiveType.UNKNOWN.equals(ArchiveType.of(dockerfile)) || Urls.isDirectory(dockerfile)) {
            ArchiveUtils.deploy(dockerfile, getMachine(), imageDir);
            imageId = buildDockerfileDirectory(name);
            log.info("Created base Dockerfile image with ID {}", imageId);
        } else {
            if (contextArchive.isPresent()) {
                ArchiveUtils.deploy(contextArchive.get(), getMachine(), imageDir);
            } else {
                ProcessTaskWrapper<Integer> task = SshEffectorTasks.ssh(format("mkdir -p %s", imageDir))
                        .machine(getMachine())
                        .newTask();
                DynamicTasks.queueIfPossible(task).executionContext(getEntity()).orSubmitAndBlock();
                int result = task.get();
                if (result != 0) throw new IllegalStateException("Error creating image directory: " + name);
            }
            copyTemplate(dockerfile, Os.mergePaths(name, DockerUtils.DOCKERFILE), false, substitutions);
            if (entrypoint.isPresent()) {
                copyResource(entrypoint.get(), Os.mergePaths(name, DockerUtils.ENTRYPOINT));
            }

            // Build the image using the files in the image directory
            imageId = buildDockerfileDirectory(name);
            log.info("Created Dockerfile image with ID {}", imageId);
        }

        if (useSsh) {
            // Update the image with the Clocker sshd Dockerfile
            copyTemplate(DockerUtils.SSHD_DOCKERFILE, Os.mergePaths(name, "Sshd" + DockerUtils.DOCKERFILE), false, substitutions);
            imageId = buildDockerfile("Sshd" + DockerUtils.DOCKERFILE, name);
            log.info("Created SSHable Dockerfile image with ID {}", imageId);
        }

        return imageId;
    }

    /** {@inheritDoc} */
    @Override
    public String layerSshableImageOn(String fullyQualifiedImageName) {
        checkNotNull(fullyQualifiedImageName, "fullyQualifiedImageName");
        copyTemplate(DockerUtils.SSHD_DOCKERFILE, Os.mergePaths(fullyQualifiedImageName, "Sshd" + DockerUtils.DOCKERFILE),
                true, ImmutableMap.<String, Object>of("fullyQualifiedImageName", fullyQualifiedImageName));
        String sshdImageId = buildDockerfile("Sshd" + DockerUtils.DOCKERFILE, fullyQualifiedImageName);
        log.info("Created SSH-based image from {} with ID {}", fullyQualifiedImageName, sshdImageId);

        return sshdImageId;
    }

    private String buildDockerfileDirectory(String name) {
        String build = format("build --rm -t %s %s",
                name, Os.mergePaths(getRunDir(), name));
        String stdout = ((DockerHost) getEntity()).runDockerCommandTimeout(build, Duration.minutes(20));
        String prefix = Strings.getFirstWordAfter(stdout, "Successfully built");

        return getImageId(prefix, name);
    }

    private String buildDockerfile(String dockerfile, String name) {
        String build = format("build --rm -t %s - < %s",
                name, Os.mergePaths(getRunDir(), name, dockerfile));
        String stdout = ((DockerHost) getEntity()).runDockerCommandTimeout(build, Duration.minutes(20));
        String prefix = Strings.getFirstWordAfter(stdout, "Successfully built");

        return getImageId(prefix, name);
    }

    // Inspect the Docker image with this prefix
    private String getImageId(String prefix, String name) {
        String inspect = format("inspect --format={{.Id}} %s", prefix);
        String imageId = ((DockerHost) getEntity()).runDockerCommand(inspect);
        return DockerUtils.checkId(imageId);
    }

    public String getEpelRelease() {
        return getEntity().config().get(DockerHost.EPEL_RELEASE);
    }

    @Override
    public String deployArchive(String url) {
        String volumeId = Identifiers.makeIdFromHash(url.hashCode());
        String path = Os.mergePaths(getRunDir(), volumeId);
        ArchiveUtils.deploy(url, getMachine(), path);
        return path;
    }

    @Override
    public void install() {
        OsDetails osDetails = getMachine().getMachineDetails().getOsDetails();
        String osVersion = osDetails.getVersion();
        String arch = osDetails.getArch();
        if (!osDetails.is64bit()) throw new IllegalStateException("Docker supports only 64bit OS");
        if (osDetails.isWindows() || osDetails.isMac()) {
            throw new IllegalStateException("Clocker does not support Windows or OSX currently");
        }
        log.debug("Installing Docker on {} version {}", osDetails.getName(), osVersion);

        // Generate Linux kernel upgrade commands
        if (osDetails.isLinux()) {
            String storage = Strings.toLowerCase(entity.config().get(DockerHost.DOCKER_STORAGE_DRIVER));
            if (!"devicemapper".equals(storage)) { // No kernel changes needed for devicemapper sadness as a servive
                int present = ((DockerHost) getEntity()).execCommandStatus(BashCommands.sudo("modprobe " + storage));

                ScriptHelper uname = newScript("check-kernel-version")
                        .body.append("uname -r")
                        .failOnNonZeroResultCode()
                        .uniqueSshConnection()
                        .gatherOutput()
                        .noExtraOutput();
                uname.execute();
                String kernelVersion = Strings.getFirstWord(uname.getResultStdout());

                if (VersionComparator.getInstance().compare("3.19", kernelVersion) > 0 || present != 0) {
                    List<String> commands = MutableList.of();
                    if ("ubuntu".equalsIgnoreCase(osDetails.getName())) {
                        commands.add(installPackage("software-properties-common linux-generic-lts-vivid"));
                        executeKernelInstallation(commands);
                    }
                    if ("centos".equalsIgnoreCase(osDetails.getName())) {
                        // TODO differentiate between CentOS 6 and 7 and RHEL
                        commands.add(sudo("yum -y --nogpgcheck upgrade kernel"));
                        executeKernelInstallation(commands);
                    }
                }
            }

            final DockerInfrastructure infrastructure = ((DockerHost) getEntity()).getInfrastructure();
            if (Boolean.TRUE.equals(infrastructure.config().get(DockerInfrastructure.ENABLE_ETCD_CLUSTER))) {
                // Create EtcdNode for this host
                EtcdCluster etcd = infrastructure.sensors().get(DockerInfrastructure.ETCD_CLUSTER);
                EtcdNode node = (EtcdNode) etcd.addNode(getMachine(), Maps.newHashMap());
                node.start(ImmutableList.of(getMachine()));
                getEntity().sensors().set(DockerHost.ETCD_NODE, node);
                Entities.waitForServiceUp(node);
            }
        }

        // Generate Docker install commands
        List<String> commands = Lists.newArrayList();
        if (osDetails.isLinux()) {
            commands.add(INSTALL_CURL);
            if ("ubuntu".equalsIgnoreCase(osDetails.getName())) {
                commands.add(installDockerOnUbuntu());
            } else if ("centos".equalsIgnoreCase(osDetails.getName())) { // should work for RHEL also?
                commands.add(installPackage(ImmutableMap.of("yum", "docker-" + super.getVersion()), null));
            } else {
                commands.add(installDockerFallback());
            }
        }
        newScript(INSTALLING)
                .body.append(commands)
                .failOnNonZeroResultCode()
                .execute();
    }

    private void executeKernelInstallation(List<String> commands) {
        newScript(INSTALLING + "-kernel")
                .body.append(commands)
                .body.append(sudo("reboot"))
                .execute();

        // Wait until the Docker host is SSHable after the reboot
        // Don't check immediately; it could take a few seconds for rebooting to make the machine not ssh'able;
        // must not accidentally think it's rebooted before we've actually rebooted!
        Stopwatch stopwatchForReboot = Stopwatch.createStarted();
        Time.sleep(Duration.seconds(30));

        Task<Boolean> sshable = TaskBuilder.<Boolean> builder()
                .displayName("Waiting until host is SSHable")
                .body(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        return Repeater.create()
                                .every(Duration.TEN_SECONDS)
                                .until(new Callable<Boolean>() {
                                    public Boolean call() {
                                        return getLocation().isSshable();
                                    }
                                })
                                .limitTimeTo(Duration.minutes(15)) // Because of the reboot
                                .run();
                    }
                })
                .build();
        Boolean result = DynamicTasks.queueIfPossible(sshable)
                .orSubmitAndBlock()
                .andWaitForSuccess();
        if (!result) {
            throw new IllegalStateException(format("The entity %s is not sshable after reboot (waited %s)",
                    entity, Time.makeTimeStringRounded(stopwatchForReboot)));
        }

        if (entity.config().get(JcloudsLocationConfig.MAP_DEV_RANDOM_TO_DEV_URANDOM)) {
            newScript(INSTALLING + "-urandom")
                    .body.append(
                            sudo("mv /dev/random /dev/random-real"),
                            sudo("ln -s /dev/urandom /dev/random"))
                    .execute();
        }

        // Setup SoftLayer hostname after reboot
        MachineProvisioningLocation<?> provisioner = getEntity().sensors().get(SoftwareProcess.PROVISIONING_LOCATION);
        if (getEntity().config().get(DockerInfrastructure.USE_JCLOUDS_HOSTNAME_CUSTOMIZER)) {
            JcloudsHostnameCustomizer.instanceOf().customize((JcloudsLocation) provisioner, (ComputeService) null, (JcloudsMachineLocation) location);
        }
    }

    @Override
    public String getVersion() {
        String version = super.getVersion();
        if (version.matches("^[0-9]+\\.[0-9]+$")) {
            version += ".0"; // Append minor version
        }
        else if (version.matches("[0-9]\\.[0-9]\\.[0-9]-[0-9]+\\.el7\\.centos")) {
            return version.substring(0, 5);
        }
        return version;
    }

    private String installDockerOnUbuntu() {
        String dockerVersion = getVersion();
        String ubuntuVersion = getMachine().getMachineDetails().getOsDetails().getVersion();

        String dockerRepoName;
        String repositoryVersionName;
        switch (ubuntuVersion) {
            case "12.04":
                dockerRepoName = "ubuntu-precise";
                repositoryVersionName = dockerVersion + "-0~precise";
                break;
            case "14.04":
                dockerRepoName = "ubuntu-trusty";
                repositoryVersionName = dockerVersion + "-0~trusty";
                break;
            case "15.04":
            case "15.10":
                dockerRepoName = "ubuntu-wily";
                repositoryVersionName = dockerVersion + "-0~wily";
                break;
            case "16.04":
                dockerRepoName = "ubuntu-xenial";
                repositoryVersionName = dockerVersion + "-0~xenial";
                break;
            default:
                throw new IllegalArgumentException("No docker repo found for ubuntu version: " + ubuntuVersion);
        }

        log.debug("Installing Docker version {} on Ubuntu {} with docker repo name {} and repository version {}",
                new Object[] { dockerVersion, ubuntuVersion, dockerRepoName, repositoryVersionName });
        return chainGroup(
                installPackage("apt-transport-https"),
                "echo 'deb https://apt.dockerproject.org/repo " + dockerRepoName + " main' | " + sudo("tee -a /etc/apt/sources.list.d/docker.list"),
                sudo("apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 58118E89F3A912897C070ADBF76221572C52609D"),
                installPackage("docker-engine=" + repositoryVersionName));
    }

    /**
     * Uses the curl-able install.sh script provided at {@code get.docker.com}.
     * This will install the latest version, which may be incompatible with the
     * jclouds driver.
     */
    private String installDockerFallback() {
        return "curl -s https://get.docker.com/ | " + sudo("sh");
    }

    // TODO --registry-mirror
    private String getDockerRegistryOpts() {
        String registryUrl = entity.config().get(DockerInfrastructure.DOCKER_IMAGE_REGISTRY_URL);
        if ("https://index.docker.io/v1/".equals(registryUrl)) {
            // If using dockerhub do not configure anything
            return null;
        }

        if (Strings.isNonBlank(registryUrl)) {
            return format("--insecure-registry %s", registryUrl);
        }

        if (entity.config().get(DockerInfrastructure.DOCKER_SHOULD_START_REGISTRY)) {
            String firstHostname = entity.sensors().get(DynamicGroup.FIRST).sensors().get(Attributes.HOSTNAME);
            Integer registryPort = entity.config().get(DockerInfrastructure.DOCKER_REGISTRY_PORT);
            return format("--insecure-registry %s:%d", firstHostname, registryPort);
        }

        return null;
    }

    private String getStorageOpts() {
        String driver = getEntity().config().get(DockerHost.DOCKER_STORAGE_DRIVER);
        List<String> options = getEntity().config().get(DockerHost.DOCKER_STORAGE_OPTIONS);
        if (Strings.isBlank(driver)) {
            return null;
        } else if (options.size() > 0) {
            return "-s " + Strings.toLowerCase(driver) + " --storage-opt " + Strings.join(options, " --storage-opt ");
        } else {
            return "-s " + Strings.toLowerCase(driver);
        }
    }

    @Override
    public void customize() {
        if (isRunning()) {
            log.info("Stopping running Docker instance at {} before customising", getMachine());
            stop();
        }

        // Determine OS
        String os = getMachine().getMachineDetails().getOsDetails().getName();
        boolean centos = "centos".equalsIgnoreCase(os);
        boolean ubuntu = "ubuntu".equalsIgnoreCase(os);

        // Copy certs from install directory to run directory
        if (entity.config().get(DockerInfrastructure.DOCKER_GENERATE_TLS_CERTIFICATES)) {
            newScript(ImmutableMap.of(NON_STANDARD_LAYOUT, "true"), CUSTOMIZING)
                    .body.append(
                            format("cp ca-cert.pem %s/ca.pem", getRunDir()),
                            format("cp server-cert.pem %s/cert.pem", getRunDir()),
                            format("cp server-key.pem %s/key.pem", getRunDir()))
                .failOnNonZeroResultCode()
                .execute();
        }
        else {
            newScript(ImmutableMap.of(NON_STANDARD_LAYOUT, "true"), CUSTOMIZING)
                    .body.append(
                            format("cp ca.pem %s/ca.pem", getRunDir()),
                            format("cp cert.pem %s/cert.pem", getRunDir()),
                            format("cp key.pem %s/key.pem", getRunDir()))
                    .failOnNonZeroResultCode()
                    .execute();
        }

        // Add the CA cert as an authorised docker CA for the first host.
        // This will be used for docker registry etc.
        String firstHost = entity.sensors().get(AbstractGroup.FIRST).sensors().get(Attributes.HOSTNAME);
        String certsPath = "/etc/docker/certs.d/" + firstHost + ":"+ entity.config().get(DockerInfrastructure.DOCKER_REGISTRY_PORT);

        newScript(CUSTOMIZING)
                .body.append(
                        chainGroup(
                                sudo("mkdir -p " + certsPath),
                                sudo("cp ca.pem " + certsPath + "/ca.crt")))
                .failOnNonZeroResultCode()
                .execute();

        // Docker daemon startup arguments

        List<String> args = MutableList.of(
                centos ? "--selinux-enabled" : null,
                "--userland-proxy=false",
                format("-H tcp://0.0.0.0:%d", getDockerPort()),
                "-H unix:///var/run/docker.sock",
                getStorageOpts(),
                getDockerRegistryOpts(),
                "--tls",
                format("--tlscert=%s/cert.pem", getRunDir()),
                format("--tlskey=%s/key.pem", getRunDir()),
                format("--tlscacert=%s/ca.pem", getRunDir()));

        final DockerInfrastructure infrastructure = ((DockerHost) getEntity()).getInfrastructure();
        if (Boolean.TRUE.equals(infrastructure.config().get(DockerInfrastructure.ENABLE_ETCD_CLUSTER))) {
            EtcdNode etcdNode = getEntity().sensors().get(DockerHost.ETCD_NODE);
            HostAndPort etcdAuthority = HostAndPort.fromParts(etcdNode.sensors().get(Attributes.SUBNET_ADDRESS), etcdNode.sensors().get(EtcdNode.ETCD_CLIENT_PORT));

            args.add(format("--cluster-store=etcd://%s", etcdAuthority.toString()));
            args.add(format("--cluster-advertise=%s:%d", getEntity().sensors().get(Attributes.SUBNET_ADDRESS), getDockerPort()));
        }

        String argv = Joiner.on(" ").skipNulls().join(args);
        log.debug("Docker daemon args: {}", argv);

        // Upstart
        if (ubuntu) {
            newScript(CUSTOMIZING + "-upstart")
                    .body.append(
                            chain(
                                    sudo("mkdir -p /etc/default"),
                                    format("echo 'DOCKER_OPTS=\"%s\"' | ", argv) + sudo("tee -a /etc/default/docker")),
                                    sudo("groupadd -f docker"),
                                    sudo(format("gpasswd -a %s docker", getMachine().getUser())),
                                    sudo("newgrp docker"))
                    .failOnNonZeroResultCode()
                    .execute();
        }

        // CentOS
        if (centos) {
            newScript(CUSTOMIZING + "-sysconfig")
                    .body.append(
                            chain(
                                    sudo("mkdir -p /etc/sysconfig"),
                                    format("echo 'other_args=\"%s\"' | ", argv) + sudo("tee -a /etc/sysconfig/docker")))
                    .failOnNonZeroResultCode()
                    .execute();
        }

        // SystemD
        boolean dockerTen = VersionComparator.getInstance().compare(getVersion(), "1.10") >= 0;
        String service = Os.mergePaths(getInstallDir(), "docker.service");
        copyTemplate("classpath://clocker/docker/entity/docker.service", service, true, ImmutableMap.of("args", argv, "daemon", dockerTen ? "daemon" : "-d"));
        newScript(CUSTOMIZING + "-systemd")
                .body.append(
                        chain(
                                sudo("mkdir -p /etc/systemd/system"),
                                sudo(format("cp %s %s", service, "/etc/systemd/system/docker.service")),
                                ifExecutableElse0("systemctl", sudo("systemctl daemon-reload"))))
                .failOnNonZeroResultCode()
                .execute();

        // Configure volume mappings for the host
        Map<String, String> mapping = MutableMap.of();
        Map<String, String> volumes = getEntity().config().get(DockerHost.DOCKER_HOST_VOLUME_MAPPING);
        if (volumes != null) {
            for (String source : volumes.keySet()) {
                if (Urls.isUrlWithProtocol(source)) {
                    String path = deployArchive(source);
                    mapping.put(path, volumes.get(source));
                } else {
                    mapping.put(source, volumes.get(source));
                }
            }
        }
        getEntity().sensors().set(DockerHost.DOCKER_HOST_VOLUME_MAPPING, mapping);
    }

    @Override
    public boolean isRunning() {
        return newScript(CHECK_RUNNING)
                .body.append(sudo("docker version"))
                .uniqueSshConnection()
                .execute() == 0;
    }

    @Override
    public void stop() {
        newScript(STOPPING)
                .body.append(sudo("service docker stop"))
                .failOnNonZeroResultCode()
                .uniqueSshConnection()
                .execute();
    }

    @Override
    public void launch() {
        newScript(LAUNCHING)
                .body.append(sudo("service docker start"))
                .failOnNonZeroResultCode()
                .uniqueSshConnection()
                .execute();
    }

}
