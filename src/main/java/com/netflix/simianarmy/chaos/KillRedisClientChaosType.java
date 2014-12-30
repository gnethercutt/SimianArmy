/*
 *
 *  Copyright 2014 Glenn Nethercutt
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.simianarmy.chaos;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.netflix.simianarmy.CloudClient;
import com.netflix.simianarmy.MonkeyConfiguration;
import com.netflix.simianarmy.basic.chaos.BasicChaosMonkey;
import com.netflix.simianarmy.client.aws.chaos.ElastiCacheRedisChaosCrawler;

/**
 * Simulate client disconnects from a Redis database.
 * 
 * @author glenn
 *
 */
public class KillRedisClientChaosType extends ChaosType {
    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicChaosMonkey.class);
    
    /** Constant regex pattern for Redis client list output parsing */
    private static final Pattern p = Pattern.compile("id=(\\d+) addr=([^ ]*) .*");
    
    /**
     * Constructor.
     *
     * @param config
     *            Configuration to use
     */
    protected KillRedisClientChaosType(MonkeyConfiguration config) {
        super(config, "KillRedisClient");
    }

    /**
     * We can apply the strategy iff the instance is an ElastiCache node.
     */
    @Override
    public boolean canApply(ChaosInstance instance) {
        if (!instance.getInstanceGroup().type().equals(ElastiCacheRedisChaosCrawler.Types.ElastiCache)) {
            return false;
        }

        return super.canApply(instance);
    }

    @Override
    public void apply(ChaosInstance instance) {
        LOGGER.info("apply KillRedisClient to {}", instance);
        try (Jedis jedis = new Jedis(instance.getInstanceId())) {
            String[] clients = jedis.clientList().split("\n");
            LOGGER.info("{} clients connected to Redis cluster {}", clients.length, instance.getInstanceId());
            for (String client : clients) {                
                Matcher m = p.matcher(client);
                if (m.matches()) {
                    //TODO: use a probability calculation rather than killing all connections
                    // Figure out how to appropriately report this so service owners for the client-side are aware of the chaos
                    jedis.clientKill(m.group(2));
                }
            }
        }
    }
}
