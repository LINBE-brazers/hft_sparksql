package Extract_Transform_Load;

public class ParameterDTO {

    private String hivedatabase;
    private String isDropTable;
    private String db_name;
    private String db_ip;
    private String db_port;
    private String user_name;
    private String user_password;
    private String isNeedPartition;
    private String hiveTableName;
    private String dbsource_type;
    private String real_tableName;
    private String numBuckets;


    public String getNumBuckets() {
        return numBuckets;
    }

    public void setNumBuckets(String numBuckets) {
        this.numBuckets = numBuckets;
    }

    public String getReal_tableName() {
        return real_tableName;
    }

    public void setReal_tableName(String real_tableName) {
        this.real_tableName = real_tableName;
    }

    public String getDbsource_type() {
        return dbsource_type;
    }

    public void setDbsource_type(String dbsource_type) {
        this.dbsource_type = dbsource_type;
    }

    public ParameterDTO() {
    }

    public String getHivedatabase() {
        return hivedatabase;
    }

    public void setHivedatabase(String hivedatabase) {
        this.hivedatabase = hivedatabase;
    }

    public String getIsDropTable() {
        return isDropTable;
    }

    public void setIsDropTable(String isDropTable) {
        this.isDropTable = isDropTable;
    }

    public String getDb_name() {
        return db_name;
    }

    public void setDb_name(String db_name) {
        this.db_name = db_name;
    }

    public String getDb_ip() {
        return db_ip;
    }

    public void setDb_ip(String db_ip) {
        this.db_ip = db_ip;
    }

    public String getDb_port() {
        return db_port;
    }

    public void setDb_port(String db_port) {
        this.db_port = db_port;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getUser_password() {
        return user_password;
    }

    public void setUser_password(String user_password) {
        this.user_password = user_password;
    }

    public String getIsNeedPartition() {
        return isNeedPartition;
    }

    public void setIsNeedPartition(String isNeedPartition) {
        this.isNeedPartition = isNeedPartition;
    }

    public String getHiveTableName() {
        return hiveTableName;
    }

    public void setHiveTableName(String hiveTableName) {
        this.hiveTableName = hiveTableName;
    }

    @Override
    public String toString() {
        return "ParameterDTO{" +
                "hivedatabase='" + hivedatabase + '\'' +
                ", isDropTable='" + isDropTable + '\'' +
                ", db_name='" + db_name + '\'' +
                ", db_ip='" + db_ip + '\'' +
                ", db_port='" + db_port + '\'' +
                ", user_name='" + user_name + '\'' +
                ", user_password='" + user_password + '\'' +
                ", isNeedPartition='" + isNeedPartition + '\'' +
                ", hiveTableName='" + hiveTableName + '\'' +
                ", dbsource_type='" + dbsource_type + '\'' +
                ", real_tableName='" + real_tableName + '\'' +
                '}';
    }
}
