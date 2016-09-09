package clocker.docker.location.strategy.no.host;

import clocker.docker.entity.DockerHost;
import clocker.docker.entity.DockerInfrastructure;

/**
 * A {@link NoHostStrategy} that waits for a new host.
 */
public final class WaitForHostStrategy implements NoHostStrategy {
    private final long backoff;

    /**
     * Constructor.
     * @param backoff the number of milliseconds to wait for a host
     */
    public WaitForHostStrategy(long backoff) {
        this.backoff = backoff;
    }

    @Override
    public DockerHost noHosts(DockerInfrastructure dockerInfrastructure) {
        try {
            Thread.sleep(backoff);
        }
        catch (InterruptedException e) {
            return null;
        }

        return null;
    }
}
