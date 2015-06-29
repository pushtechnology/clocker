package brooklyn.location.docker.strategy;

import java.util.Map;

import brooklyn.entity.Entity;
import brooklyn.location.docker.DockerLocation;

/**
 * This will result in the failure to provision a container.
 * @author Matt Champion on 26/04/15
 */
public final class DoNothingHostStrategy implements NoAvailableHostStrategy {
    @Override
    public boolean handleNoHosts(DockerLocation location) {
        return false;
    }
}
