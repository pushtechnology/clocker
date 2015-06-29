package brooklyn.location.docker.strategy;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.Entity;
import brooklyn.location.docker.DockerLocation;

/**
 * Increase the size of the Docker cloud if there is not free host. This is suitable when no autoscaler is being used.
 * @author Matt Champion on 24/04/15
 */
public final class ExpandOnNoHostStrategy implements NoAvailableHostStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ExpandOnNoHostStrategy.class);

    @Override
    public boolean handleNoHosts(DockerLocation location) {
        final int size = location.getOwner().getCurrentSize();
        location.getOwner().resize(size + 1);
        return true;
    }
}
