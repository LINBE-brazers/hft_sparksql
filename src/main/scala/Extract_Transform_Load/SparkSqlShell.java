package Extract_Transform_Load;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SparkSqlShell {

    /**
     * 获取创建hive表的相关信息，
     * 包括创建表字段和从关系型数据的查询的字段
     * XML文件前缀就是就是同步的数据库的名称
     *
     * @param tableName
     * @return
     */
    public Map initSparkSqlShellFromXml(String tableName, String dbname) {
        Map<String, String> hashMap = null;
        try {
            //1. 创建DOM4J解析器对象
            SAXReader reader = new SAXReader();
            String xmlFilePath = "sql/Extract_Transform_Load/" + dbname + "/" + tableName + ".xml";
            InputStream is = SparkSqlShell.class.getClassLoader().getResourceAsStream(xmlFilePath);
            Document doc = reader.read(is);
            Element rootElement = doc.getRootElement();
            Iterator<Element> iter_root = rootElement.elementIterator();
            String create_tag = "create";
            String select_tag = "select";
            String keys = "keys";
            String rsa_columns = "rsa_columns";
            String dataframe_columns = "dataframe_columns";
            while (iter_root.hasNext()) {
                Element element = iter_root.next();
                String attribute_value = element.attributeValue("table_id");
                Iterator<Element> iter_element = element.elementIterator();
                hashMap = new HashMap<String, String>();
                //获取当前表的创建语句和查询语句
                if (StringUtils.equals(attribute_value, tableName)) {
                    while (iter_element.hasNext()) {
                        Element next = iter_element.next();
                        String tag = next.getName();
                        String text = next.getText();
                        /*
                        if (StringUtils.endsWith(tag, create_tag)) {
                            hashMap.put(create_tag, text);
                        } else if (StringUtils.endsWith(tag, select_tag)) {
                            hashMap.put(select_tag, text);
                        } else if (StringUtils.endsWith(tag, keys)) {
                            hashMap.put(keys, text);
                        } else if (StringUtils.endsWith(tag, rsa_columns)) {
                            hashMap.put(rsa_columns, text);
                        } else if (StringUtils.endsWith(tag, dataframe_columns)) {
                            hashMap.put(dataframe_columns, text);
                        }
                         */
                        hashMap.put(tag, text);
                    }
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hashMap;
    }

    /**
     * 获取身份证号码相关字段名
     *
     * @param xmlFilePath
     * @return
     */
    public static Map<String, String> getIdCardNumFromXml(String xmlFilePath) {
        Map<String, String> hashMap = null;
        String elementText = "";
        try {
            //1. 创建DOM4J解析器对象
            SAXReader reader = new SAXReader();
            InputStream is = SparkSqlShell.class.getClassLoader().getResourceAsStream(xmlFilePath);
            Document doc = reader.read(is);
            Element rootElement = doc.getRootElement();
            elementText = rootElement.getText().trim();

            hashMap = new HashMap<String, String>();
            for (String str : elementText.split(",")) {
                String column = str.trim().toLowerCase();
                if (column.length() > 0) {
                    hashMap.put(column, column);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return hashMap;
    }

}
