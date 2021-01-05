package Extract_Transform_Load;

import com.alibaba.fastjson.JSON;

public class testjavaDemo {

    public static void main(String[] args) {
        ParameterDTO parameterDTO = new ParameterDTO();
        parameterDTO.setHivedatabase("ods_hft_database_info1");
        parameterDTO.setIsDropTable("Y");
        parameterDTO.setDb_name("newtest");
        parameterDTO.setDb_ip("10.101.40.211");
        parameterDTO.setDb_port("3306");
        parameterDTO.setUser_name("root");
        parameterDTO.setUser_password("123456");
        parameterDTO.setIsNeedPartition("Y");
        parameterDTO.setHiveTableName("ods_wei_yong");
        parameterDTO.setDbsource_type("mysql");
        parameterDTO.setReal_tableName("wei_yong");

        String json = JSON.toJSONString(parameterDTO);
        System.out.println(parameterDTO.toString());
    }
}
