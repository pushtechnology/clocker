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
package brooklyn.networking.sdn;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.reflect.TypeToken;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;

/**
 * SDN attributes and configuration keys.
 */
public class SdnAttributes {

    public static final ConfigKey<Collection<String>> NETWORK_LIST = ConfigKeys.newConfigKey(
            new TypeToken<Collection<String>>() { }, "network.list", "Collection of extra networks to create for an entity", Collections.<String>emptyList());

    public static final ConfigKey<Boolean> SDN_ENABLE = ConfigKeys.newBooleanConfigKey("sdn.enable", "Enable Sofware-Defined Networking", Boolean.FALSE);
    public static final ConfigKey<Boolean> SDN_DEBUG = ConfigKeys.newBooleanConfigKey("sdn.debug", "Enable SDN debugging utility installation", Boolean.FALSE);

    public static final ConfigKey<EntitySpec<SdnProvider>> SDN_PROVIDER_SPEC = ConfigKeys.newConfigKey(new TypeToken<EntitySpec<SdnProvider>>() { }, "sdn.provider.spec", "SDN provider entity specification");

    public static final AttributeSensorAndConfigKey<SdnProvider, SdnProvider> SDN_PROVIDER = ConfigKeys.newSensorAndConfigKey(SdnProvider.class, "sdn.provider.network", "SDN provider network entity");

    public static final AttributeSensor<List<String>> ATTACHED_NETWORKS = Sensors.newSensor(new TypeToken<List<String>>() { },
            "sdn.networks.attached", "The list of networks that an entity is attached to");

}
