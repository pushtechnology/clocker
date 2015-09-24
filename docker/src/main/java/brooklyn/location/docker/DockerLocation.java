/*
 * Copyright 2014-2015 by Cloudsoft Corporation Limited
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
package brooklyn.location.docker;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.api.mgmt.rebind.RebindContext;
import org.apache.brooklyn.api.mgmt.rebind.RebindSupport;
import org.apache.brooklyn.api.mgmt.rebind.mementos.LocationMemento;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityFunctions;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.dynamic.DynamicLocation;
import org.apache.brooklyn.core.mgmt.rebind.BasicLocationRebindSupport;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.container.DockerAttributes;
import brooklyn.entity.container.docker.DockerHost;
import brooklyn.entity.container.docker.DockerInfrastructure;
import brooklyn.location.docker.strategy.DockerAwarePlacementStrategy;
import brooklyn.location.docker.strategy.NoAvailableHostStrategy;
import brooklyn.networking.location.NetworkProvisioningExtension;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

public class DockerLocation extends AbstractLocation implements DockerVirtualLocation, MachineProvisioningLocation<MachineLocation>,
        DynamicLocation<DockerInfrastructure, DockerLocation>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DockerLocation.class);

    @SetFromFlag("owner")
    private DockerInfrastructure infrastructure;

    @SetFromFlag("strategies")
    private List<DockerAwarePlacementStrategy> strategies;

    @SetFromFlag("provisioner")
    private MachineProvisioningLocation<SshMachineLocation> provisioner;

    @SetFromFlag("machines")
    private final SetMultimap<DockerHostLocation, String> containers = Multimaps.synchronizedSetMultimap(HashMultimap.<DockerHostLocation, String>create());

    public DockerLocation() {
        this(Maps.newLinkedHashMap());
    }

    public DockerLocation(Map properties) {
        super(properties);

        if (isLegacyConstruction()) {
            init();
        }
    }

    public MachineProvisioningLocation<SshMachineLocation> getProvisioner() {
        return provisioner;
    }

    protected List<DockerHostLocation> getDockerHostLocations() {
        List<Optional<DockerHostLocation>> result = Lists.newArrayList();
        for (Entity entity : getDockerHostList()) {
            DockerHost host = (DockerHost) entity;
            DockerHostLocation machine = host.getDynamicLocation();
            result.add(Optional.<DockerHostLocation>fromNullable(machine));
        }
        return ImmutableList.copyOf(Optional.presentInstances(result));
    }

    public MachineLocation obtain() throws NoMachinesAvailableException {
        return obtain(Maps.<String, Object>newLinkedHashMap());
    }

    @Override
    public MachineLocation obtain(Map<?,?> flags) throws NoMachinesAvailableException {
        // Check context for entity being deployed
        Object context = flags.get(LocationConfigKeys.CALLER_CONTEXT.getName());
        if (context != null && !(context instanceof Entity)) {
            throw new IllegalStateException("Invalid location context: " + context);
        }
        Entity entity = (Entity) context;

        synchronized (this) {
            final DockerHostLocation machine = obtainHostLocation(entity);
            final DockerHost dockerHost = machine.getOwner();

            // Now wait until the host has started up
            Entities.waitForServiceUp(dockerHost);

            // Obtain a new Docker container location, save and return it
            if (LOG.isDebugEnabled()) {
                LOG.debug("Obtain a new container from {} for {}", machine, entity);
            }
            Map<?, ?> hostFlags = MutableMap.copyOf(flags);
            DockerContainerLocation container = machine.obtain(hostFlags);
            containers.put(machine, container.getId());
            return container;
        }
    }

    /**
     * Obtain a host location to provision a container on.
     * @param newEntity The entity the container is being provisioned for
     * @return The DockerHostLocation to use
     * @throws NoMachinesAvailableException If there are no hosts that can be used to provision a container
     */
    private DockerHostLocation obtainHostLocation(Entity newEntity) throws NoMachinesAvailableException {
        final NoAvailableHostStrategy noAvailableHostStrategy = infrastructure.getConfig(DockerInfrastructure.NO_HOST_STRATEGY);

        List<DockerHostLocation> available;
        boolean keepGoing;
        do {
            available = tryObtainHostLocation(newEntity);
            if (available.size() == 0) {
                keepGoing = noAvailableHostStrategy.handleNoHosts(this);
            }
            else {
                return available.get(0);
            }
        }
        while (keepGoing);
        throw new NoMachinesAvailableException("No hosts found");
    }

    /**
     * Attempt to obtain a docker host location.
     * @param newEntity The entity the container is being provisioned for
     * @return An ordered list of the preferred DockerHostLocations
     */
    private List<DockerHostLocation> tryObtainHostLocation(Entity newEntity) {
        // Get the available hosts based on placement strategies
        // First get the locations of running hosts
        List<DockerHostLocation> available = new ArrayList<DockerHostLocation>();
        Iterables.addAll(available, Iterables.filter(getDockerHostLocations(), new Predicate<DockerHostLocation>() {
            @Override
            public boolean apply(DockerHostLocation location) {
                final DockerHost host = location.getOwner();
                return Lifecycle.RUNNING.equals(host.getAttribute(Attributes.SERVICE_STATE_ACTUAL));
            }
        }));

        if (LOG.isDebugEnabled()) {
            LOG.debug("Placement for: {}", Iterables.toString(Iterables.transform(available, EntityFunctions.id())));
        }

        // Apply infrastructure strategies
        for (DockerAwarePlacementStrategy strategy : strategies) {
            available = strategy.filterLocations(available, newEntity);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Placement after {}: {}", strategy, Iterables.toString(Iterables.transform(available, EntityFunctions.id())));
            }
        }

        // Apply entity specific strategies
        List<DockerAwarePlacementStrategy> entityStrategies = newEntity.config().get(DockerAttributes.PLACEMENT_STRATEGIES);
        if (entityStrategies != null && entityStrategies.size() > 0) {
            for (DockerAwarePlacementStrategy strategy : entityStrategies) {
                available = strategy.filterLocations(available, newEntity);
            }
        }
        return available;
    }

    @Override
    public MachineProvisioningLocation<MachineLocation> newSubLocation(Map<?, ?> newFlags) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void release(MachineLocation machine) {
        if (provisioner == null) {
            throw new IllegalStateException("No provisioner available to release "+machine);
        }
        String id = machine.getId();
        Set<DockerHostLocation> set = Multimaps.filterValues(containers, Predicates.equalTo(id)).keySet();
        if (set.isEmpty()) {
            throw new IllegalArgumentException("Request to release "+machine+", but this machine is not currently allocated");
        }
        DockerHostLocation host = Iterables.getOnlyElement(set);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Request to remove container mapping {} to {}", host, id);
        }
        host.release((DockerContainerLocation) machine);
        if (containers.remove(host, id)) {
            if (containers.get(host).isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Empty Docker host: {}", host);
                }

                // Remove hosts when it has no containers, except for the last one
                if (getOwner().config().get(DockerInfrastructure.REMOVE_EMPTY_DOCKER_HOSTS) && set.size() > 1) {
                    LOG.info("Removing empty Docker host: {}", host);
                    remove(host);
                }
            }
        } else {
            throw new IllegalArgumentException("Request to release "+machine+", but container mapping not found");
        }
    }

    protected void remove(DockerHostLocation machine) {
        LOG.info("Releasing {}", machine);
        DynamicCluster cluster = infrastructure.getDockerHostCluster();
        DockerHost host = machine.getOwner();
        if (cluster.removeMember(host)) {
            LOG.info("Docker Host {} released", host.getDockerHostName());
        } else {
            LOG.warn("Docker Host {} not found for release", host.getDockerHostName());
        }

        // Now close and unmange the host
        try {
            machine.close();
            host.stop();
        } catch (Exception e) {
            LOG.warn("Error stopping host: " + host, e);
            Exceptions.propagateIfFatal(e);
        } finally {
            Entities.unmanage(host);
        }
    }

    @Override
    public Map<String,Object> getProvisioningFlags(Collection<String> tags) {
        return Maps.newLinkedHashMap();
    }

    @Override
    public DockerInfrastructure getOwner() {
        return infrastructure;
    }

    public List<Entity> getDockerContainerList() {
        return infrastructure.getDockerContainerList();
    }

    public List<Entity> getDockerHostList() {
        return infrastructure.getDockerHostList();
    }

    public DockerInfrastructure getDockerInfrastructure() {
        return infrastructure;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Close called on Docker infrastructure: {}", this);
    }

    // FIXME this should be supported in core Brooklyn for all extension tyoes
    @Override
    public RebindSupport<LocationMemento> getRebindSupport() {
        NetworkProvisioningExtension networkProvisioningExtension = null;
        if (hasExtension(NetworkProvisioningExtension.class)) {
            networkProvisioningExtension = getExtension(NetworkProvisioningExtension.class);
        }
        final Optional<NetworkProvisioningExtension> extension = Optional.fromNullable(networkProvisioningExtension);
        return new BasicLocationRebindSupport(this) {
            @Override public LocationMemento getMemento() {
                return getMementoWithProperties(MutableMap.<String, Object>of("networkProvisioningExtension", extension));
            }
            @Override
            protected void doReconstruct(RebindContext rebindContext, LocationMemento memento) {
                super.doReconstruct(rebindContext, memento);
                Optional<NetworkProvisioningExtension> extension = (Optional<NetworkProvisioningExtension>) memento.getCustomField("networkProvisioningExtension");
                if (extension.isPresent()) {
                    addExtension(NetworkProvisioningExtension.class, extension.get());
                }
            }
        };
    }

    @Override
    public ToStringHelper string() {
        return super.string()
                .omitNullValues()
                .add("provisioner", provisioner)
                .add("infrastructure", infrastructure)
                .add("strategies", strategies);
    }

}
