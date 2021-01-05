package Extract_Transform_Load.multithread;

public class ParameterDTONew implements Cloneable {

    private String hivedatabase;//hive数据库名称
    private String isDropTable;//是否删除hive表，当关系型数据库表结构变动时
    private String db_name;//关系型数据库名称
    private String db_ip;//关系型数据库IP
    private String db_port;//关系型数据库端口
    private String user_name;//关系型数据库用户名
    private String user_password;//关系型数据库密码
    private String isNeedPartition;//同步关系型数据库时是否需要分区同步，并行同步意思
    private String hiveTableName;//hive表名称
    private String dbsource_type;//关系型数据库类型：mysql和sqlserver
    private String real_tableName;//关系型数据库表名称
    private String numBuckets;//同步数据至kudu时的分桶数
    private String select;//同步关系表时的select
    private String create;//创建hive表的语句
    private String truncate_table;//是否需要清空hive表
    private String rsa_columns;//需要进行RSA加密的字段，多个字段间使用','逗号分割
    private String dataframe_columns; //需要单独配置sparkSql的字段
    private String df_repartitions = "1"; //dataframe需要重分区数, 默认值为1，不重分区
    private String start_time; //增量同步开始时间
    private String end_time; //增量同步结束时间
    private String idld_sql; //增量sql

    public ParameterDTONew clone() {
        ParameterDTONew o = null;
        try {
            o = (ParameterDTONew) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return o;
    }

    public String getIdld_sql() {
        return idld_sql;
    }

    public void setIdld_sql(String idld_sql) {
        this.idld_sql = idld_sql;
    }


    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public String getDf_repartitions() {
        return df_repartitions;
    }

    public void setDf_repartitions(String df_repartitions) {
        this.df_repartitions = df_repartitions;
    }

    public String getDataframe_columns() {
        return dataframe_columns;
    }

    public void setDataframe_columns(String dataframe_columns) {
        this.dataframe_columns = dataframe_columns;
    }

    public String getRsa_columns() {
        return rsa_columns;
    }

    public void setRsa_columns(String rsa_columns) {
        this.rsa_columns = rsa_columns;
    }

    public String getTruncate_table() {
        return truncate_table;
    }

    public void setTruncate_table(String truncate_table) {
        this.truncate_table = truncate_table;
    }

    public String getSelect() {
        return select;
    }

    public void setSelect(String select) {
        this.select = select;
    }

    public String getCreate() {
        return create;
    }

    public void setCreate(String create) {
        this.create = create;
    }

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

    public ParameterDTONew() {
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
        return "ParameterDTONew{" +
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
                ", numBuckets='" + numBuckets + '\'' +
                ", select='" + select + '\'' +
                ", create='" + create + '\'' +
                ", truncate_table='" + truncate_table + '\'' +
                '}';
    }
}
