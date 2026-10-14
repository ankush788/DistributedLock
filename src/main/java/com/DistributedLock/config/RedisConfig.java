package com.DistributedLock.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

@Configuration
public class RedisConfig {

    /**
     * Configures the StringRedisTemplate for fast key-value operations.
     */
    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(connectionFactory);
        return template;
    }

    /**
     * Pre-loads and compiles the unified Lua script for atomic lock acquisition (direct or from queue head).
     */
    @Bean
    public RedisScript<Long> acquireLockScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/acquire_lock.lua"));
        script.setResultType(Long.class);
        return script;
    }

    /**
     * Pre-loads and compiles the Lua script for owner-verified lock release.
     */
    @Bean
    public RedisScript<Long> releaseLockScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/release_lock.lua"));
        script.setResultType(Long.class);
        return script;
    }
}
