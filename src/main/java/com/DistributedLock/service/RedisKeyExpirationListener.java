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
 *
 * Intercepts two types of TTL expirations:
 * 1. Lock Lease Expiry (lock:<lockName>):
 *    Triggers lockService.triggerQueueHandover(lockName) to pass lock to next waiter.
 * 2. Waiter Alarm Expiry (lock:waiter:<lockName>:<clientId>):
 *    Triggers lockService.handleWaiterTimeout(lockName, clientId) to clean up graph,
 *    queue, and notify the waiting SSE connection.
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

        // Case 1: Active lock key expired naturally (TTL timeout)
        if (expiredKey.startsWith(RedisLockStore.LOCK_KEY_PREFIX)
                && !expiredKey.startsWith(RedisLockStore.QUEUE_KEY_PREFIX)
                && !expiredKey.startsWith(RedisLockStore.WAITER_KEY_PREFIX)) {

            String lockName = expiredKey.substring(RedisLockStore.LOCK_KEY_PREFIX.length());
            log.warn("Redis TTL EXPIRED: Lock key '{}' expired naturally. Cleaning deadlock graph and triggering handover.", expiredKey);

            // Clean deadlock detector resources: release old holder from dependency graph
            String oldHolder = lockService.getDependencyGraph().getLockHolder(lockName);
            if (oldHolder != null) {
                log.info("Deadlock Graph Cleanup: Releasing expired lock holder '{}' for '{}'", oldHolder, lockName);
                lockService.getDependencyGraph().recordReleased(lockName, oldHolder);
            }

            // Hand over freed lock to the next waiting client in FIFO line
            lockService.triggerQueueHandover(lockName);
        }
        // Case 2: Individual waiter alarm key expired (60s queue wait timeout)
        else if (expiredKey.startsWith(RedisLockStore.WAITER_KEY_PREFIX)) {
            // Key format: "lock:waiter:<lockName>:<clientId>"
            String remainder = expiredKey.substring(RedisLockStore.WAITER_KEY_PREFIX.length());
            int separatorIndex = remainder.indexOf(':');
            if (separatorIndex > 0) {
                String lockName = remainder.substring(0, separatorIndex);
                String clientId = remainder.substring(separatorIndex + 1);

                log.warn("Redis WAITER ALARM EXPIRED: Client '{}' timed out waiting in queue for '{}'. Cleaning deadlock graph and state.", clientId, lockName);

                // Clean deadlock detector resources: remove waiting edges for this client
                lockService.getDependencyGraph().removeClient(clientId);

                // Notify SSE stream with TIMEOUT and purge from Redis queue list
                lockService.handleWaiterTimeout(lockName, clientId);
            }
        }
    }
}
