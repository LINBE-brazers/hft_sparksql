package broker_attribute_GIO;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

public class broker_attribute_POJO {

    public String userId;
    public Map<String,String> attrs = new HashMap<String,String>();

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Map<String, String> getAttrs() {
        return attrs;
    }

    public void setAttrs(Map<String, String> attrs) {
        this.attrs = attrs;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
