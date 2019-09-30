package yang.hiveJDBC;



import org.json.Test;

import java.sql.*;



public class HiveJDBC {



	private static String DRIVER = "org.apache.hive.jdbc.HiveDriver";
	private static String URL = "jdbc:hive2://al-node2:10000/test";
	private static String USERNAME = "hdfs";
	private static String PASSWORD = "hdfs";
	private static Connection connection;
	private static Statement statement;


	static {
		try {
			// 加载hive jdbc驱动
			Class.forName(DRIVER);
			// 获取连接
			connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
			// 获取statement
			statement = connection.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
		//insert();
		//select();
		//createDataBases("create databas test");
		//select("select * from ");

		//desc("test.student");
		show("show databases");
		use("test");
		show("show tables");
		System.out.println("========================");
		set("set hive.exec.dynamic.partition.mode") ;
		desc("desc formatted student");
	}

	/**
	 * insert
	 *
	 * @param sql
	 * @throws SQLException
	 */
	public static void insert(String sql) throws SQLException {
		try {
			statement.execute(sql);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	public static void show(String sql) throws SQLException {

		ResultSet resultSet= null;
		try {
			resultSet = statement.executeQuery(sql);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		if (resultSet!=null){

			while (resultSet.next()) {
				System.out.println(resultSet.getString(1));
			}
		}

	}


	/**
	 * select
	 *
	 * @throws SQLException
	 */
	public static void select(String sql) throws SQLException {

		ResultSet resultSet = null;
		try {
			resultSet = statement.executeQuery(sql);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		if (resultSet!=null){
		while (resultSet.next()) {
			System.out.println(
					resultSet.getLong("id") +","+
							resultSet.getString("name")+","+
							resultSet.getString("class"));;

		}}
	}

	/**
	 *
	 * 创建数据库
	 *
	 * @param sql
	 * @throws SQLException
	 */
	public static void createDataBases(String sql) throws SQLException {


		boolean execute;

		try {
			execute = statement.execute(sql);
		} catch (SQLException e) {

			System.out.println(e.getMessage());

		/*	System.out.println("getSQLState  "+e.getSQLState());
			System.out.println("getErrorCode  " + e.getErrorCode());
			System.out.println("getCause  " +e.getCause());
			System.out.println("getClass  " +e.getClass());
			System.out.println("getLocalizedMessage  "+e.getLocalizedMessage());
			System.out.println("getNextException  "+ e.getNextException());*/


		}


	}



	public static void desc(String sql) throws SQLException {

		ResultSet resultSet = statement.executeQuery(sql);
		if (resultSet!=null){
			System.out.println("1");
			while (resultSet.next()) {
				System.out.println(resultSet.getString(1) + "|||||"
						+ resultSet.getString(2));

			}
		}else
		{
			System.out.println("0"
			);
		}
	}



	public static void set(String sql) throws SQLException {
		boolean execute = statement.execute(sql);

	}
	public static void drop(String tableName) throws SQLException {


	}

	public static void use(String databaseName) throws SQLException {

		boolean execute = statement.execute("use " + databaseName);


	}
	public static void load(String fileUrl) throws SQLException {

		boolean execute = statement.execute("load data local inpath '\" + filePath + \"' overwrite into table emp\"");


	}
}