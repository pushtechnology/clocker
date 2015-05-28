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
package brooklyn.entity.container.docker;

import brooklyn.entity.basic.SoftwareProcessDriver;

/**
 * The {@link SoftwareProcessDriver driver} for Docker.
 */
public interface DockerHostDriver extends SoftwareProcessDriver {

    Integer getDockerPort();

    /**
     * Login to a Docker repository
     * @param username The username to login with
     * @param email The email to login with
     * @param password The password to login with
     */
    void loginToRepository(String username, String email, String password);

    /**
     * Build a Docker image from the given Dockerfile.
     * <p>
     * Copies the Dockerfile to the host in the {@code name} folder, and
     * uses {@code brooklyn/name} for the repository. The returned ID is
     * required to start a container using the jclouds API.
     *
     * @return the 64 character Image ID
     * @see DockerHost#createSshableImage(String, String)
     */
    String buildImage(String dockerFile, String name);

    /**
     * Build an SSHable Docker image that is based from an image with the given name.
     *
     * @param name e.g. tomcat:8.0
     * @return the new image's ID
     */
    String layerSshableImageOn(String name, String tag);

    String deployArchive(String url);

    void configureSecurityGroups();

}
