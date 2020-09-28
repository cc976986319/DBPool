package com.sensitiveidentifyapi.comm.JdbcConPool;

import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class DBHelperPool {
    private Long _idleTimeout;
    private Integer _maxPoolSize;
    private Long _waitTimeout;

    private Long _pruningInterval;
    // 连接池队列，key: dbType|ip|port
    private ConcurrentHashMap<String, DBConnectionPoolItem> _poolMap = new ConcurrentHashMap<String, DBConnectionPoolItem>();
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private ReentrantLock hashlock = new ReentrantLock();
    private Timer _timer;

    public DBHelperPool(@Value("${DBHelperPool.idleTimeout}") Long idleTimeout,
            // @Value("${DBHelperPool.pruningInterval}") Long pruningInterval,
            @Value("${DBHelperPool.maxPoolSize}") Integer maxPoolSize,
            @Value("${DBHelperPool.waitTimeout}") Long waitTimeout) {
        this._idleTimeout = idleTimeout;
        this._maxPoolSize = maxPoolSize;
        this._waitTimeout = waitTimeout;
        Long pruningInterval = idleTimeout * 1000 / 2;
        if (pruningInterval > 10000L) {
            pruningInterval = 10000L;
        }
        this._pruningInterval = pruningInterval;
        _timer = new Timer();
        _timer.schedule(new TimerTask() {
            public void run() {
                CheckObjectIdleTime();
            }
        }, 2000, _pruningInterval);
        // Timeout.Infinite);
    }

    public DBHelperPooledObject Get(Integer dbType, String ip, Integer port, String userName, String password,
            String database, String serviceName) throws Exception {
        String key = dbType + "|" + ip + "|" + port;
        DBConnectionPoolItem pool = null;
        readWriteLock.readLock().lock();
        try {
            if (!_poolMap.containsKey(key)) {
                hashlock.lock();
                try {
                    if (!_poolMap.containsKey(key)) {
                        pool = new DBConnectionPoolItem(_maxPoolSize, _waitTimeout, _idleTimeout);
                        _poolMap.put(key, pool);
                    } else {
                        pool = _poolMap.get(key);
                    }
                } catch (Exception e) {
                    throw e;
                } finally {
                    hashlock.unlock();
                }
            } else {
                pool = _poolMap.get(key);
                if (pool.getIsNull()) {
                    _poolMap.remove(key);
                    pool =new DBConnectionPoolItem(_maxPoolSize, _waitTimeout, _idleTimeout);
                    _poolMap.put(key, pool);
                }
            }

            DBHelperPooledObject pooledObject = pool.getResource(dbType, ip, port, userName, password, database,
                    serviceName);
            return pooledObject;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /// <summary>
    /// 检查连接池中对象闲置时间是否超时，若是则销毁该对象
    /// </summary>
    private void CheckObjectIdleTime() {
        if (readWriteLock.hasQueuedThreads()) {
            return;
        }
        readWriteLock.writeLock().lock();
        try {
            if (readWriteLock.getReadLockCount() > 0) {
                return;
            }
            for (Entry<String, DBConnectionPoolItem> entry : _poolMap.entrySet()) {
                DBConnectionPoolItem pool = entry.getValue();
                pool.clearIdleTime();
            }
        } catch (Exception e) {
            throw e;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
