package brooklyn.location.docker.strategy;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.EntityFunctions;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.Lifecycle;
import brooklyn.entity.container.DockerAttributes;
import brooklyn.entity.container.docker.DockerHost;
import brooklyn.entity.container.docker.DockerInfrastructure;
import brooklyn.location.NoMachinesAvailableException;
import brooklyn.location.docker.DockerHostLocation;
import brooklyn.location.docker.DockerLocation;
import brooklyn.policy.basic.AbstractPolicy;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Block until a host is available to provision on.
 * @author Matt Champion on 29/04/15
 */
public final class WaitForHostStrategy extends AbstractPolicy implements NoAvailableHostStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DockerLocation.class);
    private volatile List<DockerAwarePlacementStrategy> strategies;

    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);

        strategies = entity.config().get(DockerInfrastructure.PLACEMENT_STRATEGIES);
    }

    @Override
    public DockerHost handleNoHosts(DockerLocation location, Entity newEntity, Map<?, ?> flags) throws NoMachinesAvailableException {
        LOG.info("Waiting for host to become available");
        List<DockerHostLocation> available;
        do {
            available = getAvailable(newEntity);
            if (available.size() == 0) {
                try {
                    Thread.sleep(5000L);
                }
                catch (InterruptedException e) {
                    throw new NoMachinesAvailableException("Was interrupted while waiting for machine", e);
                }
            }
            else {
                final DockerHostLocation availableLocation = Iterables.getFirst(available, null);
                return availableLocation.getOwner();
            }
        }
        while (true); // TODO: Timeout
    }

    private final List<DockerHostLocation> getAvailable(Entity newEntity) {
        // Get the available hosts based on placement strategies
        List<DockerHostLocation> available = getDockerHostLocations();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Placement for: {}", Iterables.toString(Iterables.transform(available, EntityFunctions.id())));
        }
        for (DockerAwarePlacementStrategy strategy : strategies) {
            available = strategy.filterLocations(available, newEntity);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Placement after {}: {}", strategy, Iterables.toString(Iterables.transform(available, EntityFunctions.id())));
            }
        }
        List<DockerAwarePlacementStrategy> entityStrategies = newEntity.config().get(DockerAttributes.PLACEMENT_STRATEGIES);
        if (entityStrategies != null && entityStrategies.size() > 0) {
            for (DockerAwarePlacementStrategy strategy : entityStrategies) {
                available = strategy.filterLocations(available, newEntity);
            }
        }
        return available;
    }

    private List<DockerHostLocation> getDockerHostLocations() {
        final DockerInfrastructure infrastructure = (DockerInfrastructure)entity;
        final List<Entity> hosts = infrastructure.getDockerHostList();
        List<Optional<DockerHostLocation>> result = Lists.newArrayList();
        for (Entity entity : hosts) {
            if (Lifecycle.RUNNING.equals(entity.getAttribute(Attributes.SERVICE_STATE_ACTUAL))) {
                DockerHost host = (DockerHost) entity;
                DockerHostLocation machine = host.getDynamicLocation();
                result.add(Optional.fromNullable(machine));
            }
        }
        return ImmutableList.copyOf(Optional.presentInstances(result));
    }
}
