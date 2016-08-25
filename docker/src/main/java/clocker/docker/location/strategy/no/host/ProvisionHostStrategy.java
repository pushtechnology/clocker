package clocker.docker.location.strategy.no.host;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.policy.autoscaling.AutoScalerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import clocker.docker.entity.DockerHost;
import clocker.docker.entity.DockerInfrastructure;
import clocker.docker.policy.ContainerHeadroomEnricher;

/**
 * A {@link NoHostStrategy} that provisions a new host.
 */
public final class ProvisionHostStrategy implements NoHostStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ProvisionHostStrategy.class);

    @Override
    public DockerHost noHosts(DockerInfrastructure dockerInfrastructure) {
        // Determine if headroom scaling policy is being used and suspend
        Integer headroom = dockerInfrastructure.config().get(ContainerHeadroomEnricher.CONTAINER_HEADROOM);
        Double headroomPercent = dockerInfrastructure.config().get(ContainerHeadroomEnricher.CONTAINER_HEADROOM_PERCENTAGE);
        boolean headroomSet = (headroom != null && headroom > 0) || (headroomPercent != null && headroomPercent > 0d);
        Optional<Policy> policy = Iterables.tryFind(dockerInfrastructure.getDockerHostCluster().policies(), Predicates.instanceOf(AutoScalerPolicy.class));
        if (headroomSet && policy.isPresent()) policy.get().suspend();

        DockerHost dockerHost;
        try {
            // Resize the host cluster
            LOG.info("Provisioning new host");
            Entity added = Iterables.getOnlyElement(dockerInfrastructure.getDockerHostCluster().resizeByDelta(1));
            dockerHost = (DockerHost) added;

            // Update autoscaler policy with new minimum size and resume
            if (headroomSet && policy.isPresent()) {
                int currentMin = policy.get().config().get(AutoScalerPolicy.MIN_POOL_SIZE);
                LOG.info("Updating autoscaler policy ({}) setting {} to {}",
                    new Object[] { policy.get(), AutoScalerPolicy.MIN_POOL_SIZE.getName(), currentMin + 1 });
                policy.get().config().set(AutoScalerPolicy.MIN_POOL_SIZE, currentMin + 1);
            }
        } finally {
            if (policy.isPresent()) policy.get().resume();
        }

        return dockerHost;
    }
}
