import java.sql.Connection;
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
	
	private final String DRIVER_NAME = "com.tmax.tibero.jdbc.TbDriver";
	private final String TIBERO_JDBC_BRS_URL = "jdbc:tibero:thin:@10.240.8.101:8629:BRS";
	private final String TIBERO_JDBC_SYS_URL = "jdbc:tibero:thin:@10.240.8.110:8629:BRS";
	
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
			brs_conn = DriverManager.getConnection(TIBERO_JDBC_BRS_URL, "brs", "brs2098");
			
			String[] tableArray = {
//		   		 "INBOUND_BAG_ADDITION_INFO",
//		   		 "INBOUND_BAG_AIRLINE_DATA",
//		   		 "INBOUND_BAG_EXCEPTION_DATA",
//		   		 "INBOUND_BAG_HIST",
//		   		 "INBOUND_BAG_PROCESS_INFO",
//		   		 "INBOUND_BAG_UNIQUE_IDENTIFIER",
//		   		 "INBOUND_BAG_UNLOAD_HIST",
//		   		 "INBOUND_BPM_SEND",
//		   		 "INBOUND_BPM_SEND_APPLY_HIST",
//		   		 "INBOUND_FILE_MESSAGE_HIST",
//		   		 "INBOUND_MANUAL_FLIGHT_HIST",
//		   		 "INBOUND_MANUAL_FLIGHT_INFO",
//		   		 "INBOUND_MESSAGE_APPLY_HIST",
//		   		 "INBOUND_MESSAGE_FAIL_HIST",
//		   		 "INBOUND_TRANSACTION_BAG_HIST",
//		   		 "LOGBAG_HIST",
//		   		 "LOGIN_HIST",
//		   		 "MENU_AUTH",
//		   		 "MENU_INFO",
//		   		 "MENU_INFO_BAK",
//		   		 "MESSAGE_CODE_INFO",
//		   		 "OPENINGBAG_HIST",
//		   		 "OUTBOUND_ALERT_HIST",
//		   		 "OUTBOUND_BAG_ADDITION_INFO",
//		   		 "OUTBOUND_BAG_AIRLINE_DATA",
//		   		 "OUTBOUND_BAG_EXCEPTION_DATA",
//		   		 "OUTBOUND_BAG_HIST",
//		   		 "OUTBOUND_BAG_INFO",
//		   		 "OUTBOUND_BAG_PROCESS_INFO",
//		   		 "OUTBOUND_BAG_UNIQUE_IDENTIFIER",
//		   		 "OUTBOUND_BAG_UNLOAD_HIST",
//		   		 "OUTBOUND_BPM_SEND",
//		   		 "OUTBOUND_BPM_SEND_APPLY_HIST",
//		   		 "OUTBOUND_FLIGHT_SUMMARY",
//		   		 "OUTBOUND_MANUAL_FLIGHT_HIST",
//		   		 "OUTBOUND_MANUAL_FLIGHT_INFO",
//		   		 "OUTBOUND_MESSAGE_HIST",
//		   		 "OUTBOUND_MESSAGE_SEND_HIST",
//		   		 "OUTBOUND_MQ_MESSAGE_HIST",
//		   		 "OUTBOUND_MQ_MESSAGE_SEND_HIST",
//		   		 "OUTBOUND_TRANSACTION_BAG_HIST",
//		   		 "OUTBOUND_TRANSACTION_FAIL_HIST",
//		   		 "OUTBOUND_TRANSACTION_HIST",
//		   		 "SCN_PROGRAM_BOARD",
//		   		 "SCN_PROGRAM_UPDATE_HIST",
//		   		 "SECURITY_BAG_HIST",
//		   		 "SMTP_INFO",
//		   		 "STOP_BAG_HIST",
//		   		 "SYS_BPM_LOG",
//		   		 "SYS_MQ_FAIL_LOG",
//		   		 "SYS_SYNCHRONIZER_LOG",
//		   		 "SYS_ZZ_ELAPSED_LOG",
					
//		   		 "TRANSACTION_INFO",
//		   		 "TRANSACTION_MESSAGE_INFO",
//		   		 "TROUBLE_BAG_INFO",
//		   		 "ULD_BAG_MAPPING_INFO",
//		   		 "ULD_BARCODE_REPORT",
//		   		 "ULD_INFO",
//		   		 "USER_ALLOW_IP_INFO",
//		   		 "USER_GUIDE_INFO",
//		   		 "USER_INFO",
//		   		 "WK_CL_R",
//		   		 "WK_CP_R",
//		   		 "WK_PG_R",
//		   		 "WK_TC_P",
//		   		 "WK_TC_R",
//		   		 "WK_UC_R",
//		   		 "WK_UI_R"
		    };
		    
		    
		    for(String table : tableArray) {
		   	 tableList.add(table);
		    }
			
			System.out.println(tableList.toString());
			
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
			sys_conn = DriverManager.getConnection(TIBERO_JDBC_SYS_URL, "SYS", "tibero");
		} catch (SQLException e) {
			System.out.println("connection failure!");
			System.exit(-1);
		}
		System.out.println("Connection success!");
	}
	
	
	private void executePreparedStatement() throws SQLException
	{
		for(String tableName : tableList) {
			
			String select = String.format("select * from AIRBRS.%s", tableName);
//			String select = String.format("select * from AIRBRS.%s", "AIRCRAFT_INFO");
			PreparedStatement pstmt = 
		   		 brs_conn.prepareStatement(select);
			
			ResultSet rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			
			int nColumn = rsmd.getColumnCount();
			String insert = String.format("INSERT INTO AIRBRS.%s VALUES(", tableName);
//			String insert = String.format("INSERT INTO AIRBRS.%s VALUES(", "AIRCRAFT_INFO");
			String values = "";
			
			int j = 0;
			int k = 0;
			
			System.out.println("WORKING TABLE ::: " + tableName);
			
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
				insert = String.format("INSERT INTO AIRBRS.%s VALUES(", tableName);
//				insert = String.format("INSERT INTO AIRBRS.%s VALUES(", "AIRCRAFT_INFO");
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
	   	 
	}
	
}



