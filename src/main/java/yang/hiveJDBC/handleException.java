package yang.hiveJDBC;

import java.util.HashMap;
import java.util.Map;

public class handleException {
	public static Object getException(Exception e) {
		Object exceptionType = e.getCause().getClass().toString();
		Object exceptionDetail = e.getCause().getMessage().toString();
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("异常类型", exceptionType);
		map.put("异常点信息", exceptionDetail);
		return map.toString();
	}

}
