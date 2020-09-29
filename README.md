# DBPool
java 实现连接池，平台连接客户数据库，实现同ip端口连接数可控

注：有个设计缺陷，频繁切换同ip端口的不同库时会globalQueue获取空闲连接，但大概率获取到不是需要使用的库的连接，此时会关闭连接重新打开新的连接。
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

使用方式
    @Autowired
    DBHelperPool dbHelperPool;
    
     /**
     * 获取数据库连接
     * 
     * @param dataBaseAPIEmployEntity
     * @return
     * @throws Exception
     */
    private DBHelperPooledObject getConnection(DataBaseAPIEmployEntity dataBaseAPIEmployEntity) throws Exception {
        try {
            DBHelperPooledObject pooledObject = dbHelperPool.Get(dataBaseAPIEmployEntity.getDbtype(),
                    dataBaseAPIEmployEntity.getDbip(), dataBaseAPIEmployEntity.getDbport(),
                    dataBaseAPIEmployEntity.getDbaccount(), dataBaseAPIEmployEntity.getDbpwd(),
                    dataBaseAPIEmployEntity.getDbname(), dataBaseAPIEmployEntity.getServicename());
            return pooledObject;
        } catch (Exception e) {
            throw e;
        }
    }


    /**
     * 获取数据库版本
     * 
     * @param dataBaseAPIEmployEntity
     * @return
     * @throws Exception
     */
    public String getDbVersion(DataBaseAPIEmployEntity dataBaseAPIEmployEntity) throws Exception {
        String resultStr = "";
        Statement stmt = null;
        ResultSet rs = null;
        try (DBHelperPooledObject pooledObject = getConnection(dataBaseAPIEmployEntity)) {
            Connection con = pooledObject.getConnection();
            stmt = con.createStatement();
            Integer timeout = dataBaseAPIEmployEntity.getTimeOut();
            if (timeout != null && timeout > 0) {
                stmt.setQueryTimeout(timeout);
            }
            rs = stmt.executeQuery("SELECT @@VERSION");
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    resultStr = rs.getString(columnName);
                }
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
        return resultStr;
    }


