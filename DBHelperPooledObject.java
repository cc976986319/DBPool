import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

public class DBHelperPooledObject implements AutoCloseable {

    public DBHelperPooledObject(String key,String dbname, Connection connection,DBConnectionPoolItem dbHelperPool) {
        this.key=key;
        this.dbname=dbname;
        this.connection = connection;
        this.dbHelperPool = dbHelperPool;
        this.lastReturnTime = new Date();
    }

    private Connection connection;
    public DBConnectionPoolItem dbHelperPool;
    public String dbname;
    public  String account;
    public String key;
    public String currentMode;
    /// <summary>
    /// 最后归还时间
    /// </summary>
    private Date lastReturnTime;
    /// <summary>
    /// 空闲时间
    /// </summary>
    private Long idleTime;

    public Connection getConnection() {
        return this.connection;
    }

    void setConnection(Connection connection) {
        this.connection = connection;
    }

    public DBConnectionPoolItem getDbHelperPool() {
        return this.dbHelperPool;
    }

    public Date getLastReturnTime() {
        return lastReturnTime;
    }

    public Long getIdleTime() {
        idleTime = new Date().getTime() - this.lastReturnTime.getTime();
        return idleTime;
    }

    public Boolean IsClosed() throws SQLException {
        if (connection == null || connection.isClosed()) {
            return true;
        } else {
            return false;
        }
    }
    
    public void clearIdle(){
        try {
            dbHelperPool.clearIdle(this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() throws Exception {
        System.out.println("close 调用"+ Thread.currentThread().getId());
        try {
            this.lastReturnTime=new Date();
            dbHelperPool.release(this);
        } catch (Exception e) {
            e.printStackTrace();
        }
       
    }
}
