package com.DistributedLock.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
public class RedisConfig {

    /**
     * Configures the StringRedisTemplate for fast key-value operations.
     */
    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }

    /**
     * Configures the RedisMessageListenerContainer for asynchronous Pub/Sub and keyspace expiration messaging.
     */
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory connectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        return container;
    }

    /**
     * 1. Script for direct lock acquisition.
     */
    @Bean
    public RedisScript<Long> acquireLockScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/acquire_lock.lua"));
        script.setResultType(Long.class);
        return script;
    }

    /**
     * 2. Script for putting waiting requests in FIFO queue.
     */
    @Bean
    public RedisScript<Long> enqueueLockScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/enqueue_lock.lua"));
        script.setResultType(Long.class);
        return script;
    }

    /**
     * 3. Script for owner-verified lock release.
     */
    @Bean
    public RedisScript<Long> releaseLockScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/release_lock.lua"));
        script.setResultType(Long.class);
        return script;
    }

    /**
     * 4. Script for atomic lock/queue revocation.
     */
    @Bean
    public RedisScript<Long> revokeLockScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/revoke_lock.lua"));
        script.setResultType(Long.class);
        return script;
    }

    /**
     * 5. Script for queue lock acquisition and handover.
     * Returns the winning tokenId as a String.
     */
    @Bean
    public RedisScript<String> queueLockAcquireScript() {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/queue_lock_acquire.lua"));
        script.setResultType(String.class);
        return script;
    }
}
