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
package clocker.docker.test;

import org.apache.brooklyn.api.mgmt.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clocker.docker.entity.DockerInfrastructure;
import clocker.docker.location.DockerLocation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.util.time.Duration;

/**
 * Contains static tests that can be run by any test class.
 */
public class DockerInfrastructureTests {

    private static final Logger LOG = LoggerFactory.getLogger(DockerInfrastructureTests.class);

    private DockerInfrastructureTests() {}

    public static DockerInfrastructure deployAndWaitForDockerInfrastructure(TestApplication app, Location location) {
        DockerInfrastructure dockerInfrastructure = app.createAndManageChild(EntitySpec.create(DockerInfrastructure.class)
                .configure(DockerInfrastructure.DOCKER_HOST_CLUSTER_MIN_SIZE, 1)
                .configure(DockerInfrastructure.SDN_ENABLE, false)
                .displayName("Docker Infrastructure"));
        LOG.info("Starting {} in {}", dockerInfrastructure, location);
        app.start(ImmutableList.of(location));

        DockerLocation dockerLocation = dockerInfrastructure.getDynamicLocation();

        LOG.info("Waiting {} for {} to have started", Duration.TWO_MINUTES, dockerInfrastructure);
        EntityAsserts.assertAttributeEqualsEventually(ImmutableMap.of("timeout", Duration.FIVE_MINUTES),
                dockerInfrastructure, Attributes.SERVICE_UP, true);
        return dockerInfrastructure;
    }

    public static void testDeploysTrivialApplication(TestApplication app, Location location) {
        DockerInfrastructure dockerInfrastructure = deployAndWaitForDockerInfrastructure(app, location);
        int existingCount = dockerInfrastructure.sensors().get(DockerInfrastructure.DOCKER_CONTAINER_COUNT);

        EntityManager emgr = app.getManagementContext().getEntityManager();
        EntitySpec<TestApplication> appSpec = EntitySpec.create(TestApplication.class);
        TestApplication deployment = emgr.createEntity(appSpec);
        deployment.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class));
        deployment.start(ImmutableList.of(dockerInfrastructure.getDynamicLocation()));

        EntityAsserts.assertAttributeEqualsEventually(deployment, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        EntityAsserts.assertAttributeEqualsEventually(dockerInfrastructure, DockerInfrastructure.DOCKER_CONTAINER_COUNT,
                existingCount + 1);

        deployment.stop();
    }
}
