import java.sql.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBConnectionPoolItem {
    // 最大活动连接数
    private Integer _maxPoolSize;
    // 最大等待时间
    private Long _waitTimeout;
    private Long _idleTimeout;

    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private ReentrantLock hashlock = new ReentrantLock();
    // 空闲队列
    private ConcurrentHashMap<String, LinkedBlockingQueue<DBHelperPooledObject>> idle = null;
    // 繁忙队列
    private ConcurrentHashMap<String, LinkedBlockingQueue<DBHelperPooledObject>> busy = null;

    LinkedBlockingQueue<DBHelperPooledObject> globalQueue = new LinkedBlockingQueue<DBHelperPooledObject>();
    // 等待队列数量
    private AtomicInteger awitSize = new AtomicInteger(0);

    // 连接池活动连接数
    private AtomicInteger activeSize = new AtomicInteger(0);
    // 总共获取的连接记数
    private AtomicInteger createCount = new AtomicInteger(0);

    // 已全部清空
    private Boolean isNull = false;

    public Boolean getIsNull() {
        return isNull;
    }

    /**
     * @param maxPoolSize
     * @param waitTimeout
     * @param idleTimeout
     */
    public DBConnectionPoolItem(Integer maxPoolSize, Long waitTimeout, Long idleTimeout) {
        this._maxPoolSize = maxPoolSize;
        this._waitTimeout = waitTimeout;
        this._idleTimeout = idleTimeout;
        idle = new ConcurrentHashMap<String, LinkedBlockingQueue<DBHelperPooledObject>>();
        busy = new ConcurrentHashMap<String, LinkedBlockingQueue<DBHelperPooledObject>>();
    }

    /**
     * 获取连接对象
     *
     * @param dbType
     * @param ip
     * @param port
     * @param userName
     * @param password
     * @param database
     * @param serviceName
     * @return
     * @throws Exception
     */
    public DBHelperPooledObject getResource(Integer dbType, String ip, Integer port, String userName, String password,
                                            String database, String serviceName) throws Exception {
        Long nowTime = System.currentTimeMillis();
        String key = getKey(dbType, ip, port, database);
        LinkedBlockingQueue<DBHelperPooledObject> keyIdle = null;
        LinkedBlockingQueue<DBHelperPooledObject> keyBusy = null;
        DBHelperPooledObject pooledObject = null;

        if (!idle.containsKey(key) || !busy.containsKey(key)) {
            hashlock.lock();
            try {
                if (!idle.containsKey(key)) {
                    keyIdle = new LinkedBlockingQueue<DBHelperPooledObject>(_maxPoolSize * 4);
                    idle.put(key, keyIdle);
                } else {
                    keyIdle = idle.get(key);
                }
                if (!busy.containsKey(key)) {
                    keyBusy = new LinkedBlockingQueue<DBHelperPooledObject>(_maxPoolSize * 4);
                    busy.put(key, keyBusy);
                } else {
                    keyBusy = busy.get(key);
                }
            } catch (Exception e) {
                throw e;
            } finally {
                hashlock.unlock();
            }
        } else {
            keyIdle = idle.get(key);
            keyBusy = busy.get(key);
        }
        pooledObject = keyIdle.poll();
        // 空闲队列idle是否有连接
        if (pooledObject == null) {
            readWriteLock.readLock().lock();// 上读锁
            try {
                // 判断池中连接数是否小于maxActive
                if (activeSize.get() < _maxPoolSize) {
                    // 先增加池中连接数后判断是否小于等于maxActive
                    if (activeSize.incrementAndGet() <= _maxPoolSize) {
                        try {
                            // 创建连接
                            pooledObject = getPooledObject(dbType, ip, port, userName, password, database, serviceName,
                                    key);
                            keyBusy.offer(pooledObject);
                            System.out.println("Thread:" + Thread.currentThread().getId() + "创建连接成功" + idle.size() + "|"
                                    + busy.size());
                            return pooledObject;
                        } catch (Exception e) {
                            activeSize.decrementAndGet();
                            e.printStackTrace();
                            throw e;
                        }
                    } else {
                        // 如增加后发现大于maxActive则减去增加的
                        activeSize.decrementAndGet();
                    }
                }
                // 若活动线程已满则等待busy队列释放连接
                try {
                    System.out.println("Thread:" + Thread.currentThread().getId() + "等待获取空闲资源");
                    awitSize.incrementAndGet();
                    for (Entry<String, LinkedBlockingQueue<DBHelperPooledObject>> tmpidle : idle.entrySet()) {
                        pooledObject = tmpidle.getValue().poll();
                        if (pooledObject != null) {
                            if (!isSameDB(pooledObject, dbType, database, userName)) {
                                if (!pooledObject.getConnection().isClosed())
                                    pooledObject.getConnection().close();
                                pooledObject = getPooledObject(dbType, ip, port, userName, password, database,
                                        serviceName, key);
                            }
                            break;
                        }
                    }
                    Long waitTime = _waitTimeout - (System.currentTimeMillis() - nowTime);
                    if (pooledObject == null) {
                        pooledObject = globalQueue.poll(waitTime, TimeUnit.MILLISECONDS);
                        if (!isSameDB(pooledObject, dbType, database, userName)) {
                            if (!pooledObject.getConnection().isClosed()) {
                                pooledObject.getConnection().close();
                            }
                            pooledObject = getPooledObject(dbType, ip, port, userName, password, database, serviceName,
                                    key);
                        }
                    }

                } catch (InterruptedException e) {
                    throw new Exception("等待异常");
                } finally {
                    awitSize.decrementAndGet();
                }
                // 判断是否超时
                if (pooledObject != null) {
                    System.out.println(
                            "Thread:" + Thread.currentThread().getId() + "获取连接：" + createCount.incrementAndGet() + "条");
                    keyBusy.offer(pooledObject);
                    return pooledObject;
                } else {
                    throw new Exception("Thread:" + Thread.currentThread().getId() + "获取连接超时！");

                }
            } finally {
                readWriteLock.readLock().unlock();
            }

        }
        // 空闲队列有连接，直接返回
        keyBusy.offer(pooledObject);
        return pooledObject;
    }

    /**
     * 将获取到的连接放入空闲队列
     *
     * @param pooledObject
     * @throws Exception
     */
    public void release(DBHelperPooledObject pooledObject) throws Exception {
        System.out.println(
                "Thread:" + Thread.currentThread().getId() + "connection释放前，" + idle.size() + "|" + busy.size());
        try {
            if (pooledObject == null) {
                System.out.println("connection 为空");
                return;
            }
            if (awitSize.get() > 0) {
                LinkedBlockingQueue<DBHelperPooledObject> keyBusy = busy.get(pooledObject.key);
                if (keyBusy.remove(pooledObject)) {
                    System.out.println("Thread:" + Thread.currentThread().getId() + "放入全局等待");
                    globalQueue.offer(pooledObject);
                } else {
                    activeSize.decrementAndGet();
                    throw new Exception("释放失败");
                }
            } else {
                LinkedBlockingQueue<DBHelperPooledObject> keyIdle = idle.get(pooledObject.key);
                LinkedBlockingQueue<DBHelperPooledObject> keyBusy = busy.get(pooledObject.key);
                if (keyBusy.remove(pooledObject)) {
                    System.out.println("Thread:" + Thread.currentThread().getId() + "connection正在释放");
                    keyIdle.offer(pooledObject);
                    System.out.println("Thread:" + Thread.currentThread().getId() + "connection已释放，" + idle.size() + "|"
                            + busy.size());
                } else {
                    activeSize.decrementAndGet();
                    throw new Exception("释放失败");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clearIdle(DBHelperPooledObject pooledObject) {
        try {
            if (pooledObject == null) {
                System.out.println("connection 为空");
                return;
            }
            if (!pooledObject.getConnection().isClosed())
                pooledObject.getConnection().close();
            LinkedBlockingQueue<DBHelperPooledObject> keyIdle = idle.get(pooledObject.key);
            if (keyIdle.remove(pooledObject)) {
                activeSize.decrementAndGet();
                System.out.println("自动释放成功");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clearIdleTime() {
        if (readWriteLock != null) {
            return;
        }
        if (readWriteLock.hasQueuedThreads()) {
            return;
        }
        readWriteLock.writeLock().lock();
        try {
            if (readWriteLock.getReadLockCount() > 0) {
                return;
            }
            for (Entry<String, LinkedBlockingQueue<DBHelperPooledObject>> tmpidle : idle.entrySet()) {
                tmpidle.getValue().removeIf((e) -> {
                    Boolean bool = false;
                    try {
                        if (e.IsClosed()) {
                            return true;
                        } else {
                            if (e.getIdleTime() > _idleTimeout) {
                                e.clearIdle();
                                return true;
                            }
                        }
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                    return bool;
                });
            }

            if (activeSize.get() < 1)
                isNull = true;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
    // @Override
    // public void close() {
    // if (isClosed.compareAndSet(false, true)) {
    // idle.forEach((pooledObject) -> {
    // try {
    // pooledObject.getConnection().close();
    // } catch (SQLException e) {

    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // });
    // busy.forEach((pooledObject) -> {
    // try {
    // pooledObject.getConnection().close();
    // } catch (SQLException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // });
    // }
    // }

    /**
     * 获取connection
     *
     * @param dbType
     * @param ip
     * @param port
     * @param userName
     * @param password
     * @param database
     * @param serviceName
     * @param key
     * @return
     * @throws Exception
     */
    private DBHelperPooledObject getPooledObject(Integer dbType, String ip, Integer port, String userName,
                                                 String password, String database, String serviceName, String key) throws Exception {
        try {
            String connectionUrl = "";
            String jdbcName = "";
            switch (dbType) {
                case 1:
                    jdbcName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                    connectionUrl = "jdbc:sqlserver://" + ip + ":" + port.toString() + ";DatabaseName=" + database
                            + ";pooling=false;";
                    break;
                case 8:
                    jdbcName = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
                    connectionUrl = "jdbc:microsoft:sqlserver://" + ip + ":" + port.toString() + ";DatabaseName="
                            + database + ";pooling=false;";
                    break;
                case 9:
//                    jdbcName = "com.alipay.oceanbase.obproxy.mysql.jdbc.Driver";
                    jdbcName = "com.alipay.oceanbase.jdbc.Driver";
                    connectionUrl = FormatFunc.formatDatabaseConnUrl("jdbc:oceanbase://", ip, port.toString(), database)
                            + "?useUnicode=true&characterEncoding=utf-8&autoReconnectForPools=false&pooling=false";
                    break;
                default:
                    throw new Exception("数据库类型异常！");

            }
            Class.forName(jdbcName);
            DriverManager.setLoginTimeout((int) (_waitTimeout / 1000));
            Connection connection = DriverManager.getConnection(connectionUrl, userName, password);

            DBHelperPooledObject result = new DBHelperPooledObject(key, database, connection, this);
            result.setConnection(connection);
            if (dbType == 9) {
                result.currentMode = getCurrentMode(connection);
                result.account = userName;
            }
            return result;
        } catch (Exception e) {
            throw e;
        }
    }

    private String getKey(Integer dbType, String ip, Integer port, String database) {
        String key = dbType + "|" + ip + "|" + port + "|" + database;
        return key;
    }

    /**
     * 获取当前连接租户模式
     *
     * @param connection
     * @return
     * @throws Exception
     */
    private String getCurrentMode(Connection connection)
            throws Exception {
        String resultStr = "";
        Statement stmt = null;
        ResultSet rs = null;
        try {
            // getConnection(bigDataApiEmployEntity);
            stmt = connection.createStatement();
            rs = stmt.executeQuery("show variables like '%compat%' ");
            while (rs.next()) {
                resultStr = rs.getString("VALUE");
            }

        } catch (Exception e) {
            throw e;
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (rs != null) {
                rs.close();
            }
        }
        resultStr = resultStr.toUpperCase();
        return resultStr;
    }


    private Boolean isSameDB(DBHelperPooledObject pooledObject, Integer dbtype, String dbName, String account) {
        if (pooledObject == null)
            return true;
        if (dbtype == 9) {
            return pooledObject.account.equals(account) && pooledObject.dbname.equals(dbName);
        } else {
            return pooledObject.dbname.equals(dbName);
        }
    }
}
