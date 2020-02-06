import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class TEST {
	
	Connection brs_conn;
	Connection sys_conn;
	
	// jdbc 드라이버 정보
	private final String DRIVER_NAME = "com.tmax.tibero.jdbc.TbDriver";
	
	// db 접속 유저
	private final String TIBERO_JDBC_BRS_URL = "jdbc:tibero:thin:@x.x.x.x:x:x";
	private final String TIBERO_JDBC_SYS_URL = "jdbc:tibero:thin:@x.x.x.x:x:x";
	
	private List<String> tableList = new ArrayList<>();
	
	 public static void main( String[] args ) throws Exception {
		
		 TEST test = new TEST();
		 
		 test.BRS_connect();
		 test.SYS_connect();
		 test.executePreparedStatement();
		 test.disconnect();
	}
	
	private void BRS_connect() {
		
		try {
			Class.forName(DRIVER_NAME);	
		}
		catch (ClassNotFoundException e) {
			System.err.println("Class Not Found");
		}
		
		try {
			brs_conn = DriverManager.getConnection(TIBERO_JDBC_BRS_URL, "x", "x");
			
			// 테이블 목록 조회
			String select = "SELECT * FROM ALL_TABLES WHERE OWNER = ?";
			
			PreparedStatement pstmt = 
		   		 brs_conn.prepareStatement(select);
			pstmt.setString(1, "AIRBRS");
			ResultSet rs = pstmt.executeQuery();

			while(rs.next()) {
				tableList.add(rs.getString("TABLE_NAME"));
			}
		} catch (SQLException e) {
			System.out.println("connection failure!");
			System.exit(-1);
		}
		System.out.println("Connection success!");
	}
	
	private void SYS_connect() throws ClassNotFoundException {
		
		try {
			Class.forName(DRIVER_NAME);	
		}
		catch (ClassNotFoundException e) {
			System.err.println("Class Not Found");
		}
		
		try {
			sys_conn = DriverManager.getConnection(TIBERO_JDBC_SYS_URL, "x", "x");
		} catch (SQLException e) {
			System.out.println("connection failure!");
			System.exit(-1);
		}
		System.out.println("Connection success!");
	}
	
	
	private void executePreparedStatement() throws SQLException
	{
		// 테이블 목록 만큼 반복해서 데이터 마이그레이션 진행
		for(String tableName : tableList) {
			
			String select = String.format("select * from ", tableName);
			
			PreparedStatement pstmt = 
		   		 brs_conn.prepareStatement(select);
			
			ResultSet rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			
			int nColumn = rsmd.getColumnCount();
			String insert = String.format("INSERT INTO %s VALUES(", tableName);
			String values = "";
			
			int j = 0;
			int k = 0;
			
			//현재 작업중인 테이블 명 출력
			System.out.println("WORKING TABLE ::: " + tableName);
			
			// 테이블의 컬럼수 만큼 반복해서 insert 문장 생성 로직
			while (rs.next()) {
		   	 
				j++;
				
				for(int i = 1; i <= nColumn; i++) {
					
					switch (rsmd.getColumnType(i)) {
					case Types.NUMERIC:
						if(String.valueOf(rs.getInt(i)) == null) {
							values += "NULL";
						} else {
							values += rs.getInt(i);
						}
					break;
					case Types.INTEGER:
						if(String.valueOf(rs.getInt(i)) == null) {
							values += "NULL";
						} else {
							values += rs.getInt(i);
						}
					break;
					case Types.VARCHAR:
						if(rs.getString(i) == null) {
							values += "NULL";
						} else {
							values += String.format("'%s'", rs.getString(i));
						}
					break;
					case Types.TIMESTAMP:
						SimpleDateFormat sdf = new SimpleDateFormat("YYYY/MM/d hh:mm:ss");
						
						if(rs.getTimestamp(i) == null) {
							values += "NULL";
						} else {
							values += String.format("TO_DATE('%s', 'YYYY/MM/DD HH24:MI:SS')",  sdf.format(rs.getTimestamp(i)));
						}
					break;
					case Types.CHAR:
						if(rs.getString(i) == null) {
							values += "NULL";
						} else {
							values += String.format("'%s'", rs.getString(i));
						}
					break;
					default:
						values += "NULL";
					break;
					}
					
					if(i == nColumn) {
						values += ");";
					} else {
						values += ",";
					}
				}
				
				insert += values;
				PreparedStatement insert_pre = sys_conn.prepareStatement(insert);
				insert = String.format("INSERT INTO %s VALUES(", tableName);
				values = "";
				try {
					if(j % 10000 == 0) {
						System.out.println("WORKING :::" + j);
					}
					insert_pre.executeUpdate();
					k++;
					if(k % 5000 == 0) {
						sys_conn.commit();
						System.out.println("COMMIT ::: TABLE --- " + tableName + " --- COUNT ---" + j);
					}
				}
				catch (SQLException e) {
					continue;
				}
		    }
			
			sys_conn.commit();
			System.out.println("COMMIT ::: TABLE --- " + tableName);
		}
	}
	
	private void disconnect() throws SQLException
	{
	    if (brs_conn != null) {
	   	 brs_conn.close();
	   	 System.out.println("brs_conn Connection close");
	    }
	    if(sys_conn != null) {
	   	 sys_conn.close();
	   	 System.out.println("sys_conn Connection close");
	    }
	    if(pstmt != null) {
		   pstmt.close();
		   System.out.println("pstmt close");
	    }
	    if(insert_pre != null) {
		   insert_pre.close();
		   System.out.println("insert_pre close");
	    }
	}
	
}



