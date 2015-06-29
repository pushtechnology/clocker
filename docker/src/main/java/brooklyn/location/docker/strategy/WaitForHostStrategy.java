package brooklyn.location.docker.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.location.docker.DockerLocation;

/**
 * Block until a host is available to provision on. This is suitable when scaling is being handled by an autoscaler policy.
 * @author Matt Champion on 29/04/15
 */
public final class WaitForHostStrategy implements NoAvailableHostStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(WaitForHostStrategy.class);
    private final long backOff;
    private final int maxAttempts;
    private int attempts = 0;

    /**
     * Constructor. Creates a strategy with no timeout.
     * @param backOff The amount of time to wait between attempts
     */
    public WaitForHostStrategy(long backOff) {
        this(backOff, -1);
    }

    /**
     * Constructor. Creates a strategy with no timeout.
     * @param backOff The amount of time to wait between attempts
     * @param maxAttempts The maximum number of attempts to provision a container
     */
    public WaitForHostStrategy(long backOff, int maxAttempts) {
        this.backOff = backOff;
        this.maxAttempts = maxAttempts;
    }

    @Override
    public boolean handleNoHosts(DockerLocation location) {
        LOG.debug("Back off and wait for host");
        try {
            Thread.sleep(backOff);
        }
        catch (InterruptedException e) {
            LOG.info("Was interrupted while waiting for machine", e);
            return false;
        }

        if (attempts == maxAttempts) {
            return false;
        }
        else if (maxAttempts != -1) {
            attempts++;
        }

        return true;
    }
}
