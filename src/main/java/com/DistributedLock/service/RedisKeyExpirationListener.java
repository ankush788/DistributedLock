package com.DistributedLock.service;

import com.DistributedLock.repository.RedisLockStore;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Listens for Redis key expiration events (__keyevent@*__:expired).
 * When a lock key expires naturally (TTL timeout without explicit release),
 * this listener intercepts the expiration and triggers lockService.triggerQueueHandover(lockName)
 * to hand over the lock to the next waiting request in line.
 */
@Slf4j
@Component
public class RedisKeyExpirationListener implements MessageListener {

    private static final String EXPIRED_EVENT_TOPIC = "__keyevent@*__:expired";

    private final RedisMessageListenerContainer redisMessageListenerContainer;
    private final LockService lockService;
    private final StringRedisTemplate redisTemplate;

    public RedisKeyExpirationListener(RedisMessageListenerContainer redisMessageListenerContainer,
                                      @Lazy LockService lockService,
                                      StringRedisTemplate redisTemplate) {
        this.redisMessageListenerContainer = redisMessageListenerContainer;
        this.lockService = lockService;
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void init() {
        redisMessageListenerContainer.addMessageListener(this, new PatternTopic(EXPIRED_EVENT_TOPIC));
        log.info("RedisKeyExpirationListener subscribed to '{}'", EXPIRED_EVENT_TOPIC);

        try {
            var connection = redisTemplate.getConnectionFactory() != null
                    ? redisTemplate.getConnectionFactory().getConnection()
                    : null;
            if (connection != null) {
                connection.serverCommands().setConfig("notify-keyspace-events", "Ex");
                log.info("Configured Redis notify-keyspace-events='Ex'");
            }
        } catch (Exception ex) {
            log.info("Redis CONFIG SET not permitted or pre-set in container flags: {}", ex.getMessage());
        }
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        if (message == null || message.getBody() == null) {
            return;
        }

        String expiredKey = new String(message.getBody(), StandardCharsets.UTF_8);

        // Filter: We only react to actual lock keys (e.g. "lock:order-123"), NOT queue keys or notify channels
        if (expiredKey.startsWith(RedisLockStore.LOCK_KEY_PREFIX)
                && !expiredKey.startsWith(RedisLockStore.QUEUE_KEY_PREFIX)) {

            String lockName = expiredKey.substring(RedisLockStore.LOCK_KEY_PREFIX.length());
            log.warn("Redis TTL EXPIRED: Lock key '{}' expired naturally. Triggering queue handover.", expiredKey);

            // Hand over to the next request in line for this lock!
            lockService.triggerQueueHandover(lockName);
        }
    }
}
