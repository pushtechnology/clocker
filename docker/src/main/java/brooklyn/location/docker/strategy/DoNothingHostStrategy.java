package brooklyn.location.docker.strategy;

import java.util.Map;

import brooklyn.entity.Entity;
import brooklyn.entity.container.docker.DockerHost;
import brooklyn.location.NoMachinesAvailableException;
import brooklyn.location.docker.DockerLocation;
import brooklyn.policy.basic.AbstractPolicy;

/**
 * @author Matt Champion on 26/04/15
 */
public final class DoNothingHostStrategy extends AbstractPolicy implements NoAvailableHostStrategy {
    @Override
    public DockerHost handleNoHosts(DockerLocation location, Entity newEntity, Map<?, ?> flags) throws NoMachinesAvailableException {
        throw new NoMachinesAvailableException("No machines available, taking no action");
    }
}
