package com.simple;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
 
import com.ibm.is.cc.javastage.api.Capabilities;
import com.ibm.is.cc.javastage.api.Configuration;
import com.ibm.is.cc.javastage.api.OutputLink;
import com.ibm.is.cc.javastage.api.OutputRecord;
import com.ibm.is.cc.javastage.api.Processor;
 
public class App extends Processor {
 
	private String urljdbc = null;
	private String username = null;
	private String password = null;
	 
	
	private OutputLink outputLink;
	 
	
	private Connection connection = null ;
	private PreparedStatement st = null ;
	private ResultSet rs = null;
 

	public Capabilities getCapabilities() {
	    Capabilities capabilities = new Capabilities();
	    // Set minimum number of input links to 1
	    capabilities.setMinimumInputLinkCount(0);
	    // Set maximum number of input links to 1
	    capabilities.setMaximumInputLinkCount(0);
	    // Set minimum number of output stream links to 1
	    capabilities.setMinimumOutputStreamLinkCount(1);
	    // Set maximum number of output stream links to 1
	    capabilities.setMaximumOutputStreamLinkCount(1);
	    // Set maximum number of reject links to 1
	    capabilities.setMaximumRejectLinkCount(0);
	    return capabilities;
	}
 

	@Override
	public void process() throws Exception {
		OutputRecord outputRecord = this.outputLink.getOutputRecord();
	    HashMap<String, int[]> alamatTotal = new HashMap<String, int[]>();
	 
		try {
		this.st = this.connection.prepareStatement(
		  "select 'Jaya' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya2' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur2', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman2', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa2', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya3' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur23', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman3', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa3', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya4' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur4', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman4', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa4', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya2' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur2', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman2', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa2', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya3' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur23', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman3', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa3', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya4' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur4', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman4', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa4', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya2' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur2', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman2', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa2', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya3' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur23', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman3', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa3', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya4' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur4', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman4', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa4', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya2' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur2', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman2', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa2', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw2', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya3' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur23', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman3', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa3', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw3', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Jaya4' as Toko, 'GANG MANGGA|1,GANG JERUK|1,GANG GANGAN|1,JALAN GANGGU|1' as Alamat union all "
		+ "select 'Makmur4', 'Jalan Soedirman Pokoke|1,GANG JERUK|2,Jalan Jalan wkwkwkwk|2,Jalan santai|2' union all "
		+ "select 'Soedirman4', 'Jalan santai|1,GANG JERUK|1' union all "
		+ "select 'Sentosa4', 'Jalan Soedirman Pokoke|2,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|2' union all "
		+ "select 'Hehehe4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|1,Jalan santai|1' union all "
		+ "select 'Wkwkwkk4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' union all "
		+ "select 'Wkw4', 'Jalan Soedirman Pokoke|1,GANG JERUK|1,Jalan Jalan wkwkwkwk|2,Jalan santai|1' ; "
		);
		this.rs = st.executeQuery();
		while ( this.rs.next() ) {
		    String[] alamat = rs.getString(2).split(",");
		    for( int i=0; i < alamat.length; i++ ) {
		    	String[] tmp = alamat[i].split("\\|");
		    	if( alamatTotal.size() == 0 ) {
		    		alamatTotal.put( tmp[0], new int[] { 1, Integer.parseInt(tmp[1]) } );
		    	} else if( alamatTotal.containsKey(tmp[0]) ) {
		    		int[] data = alamatTotal.get(tmp[0]);
		    		data[0] = data[0] + 1;
		    		data[1] = data[1] + Integer.parseInt(tmp[1]);
		    		alamatTotal.put( tmp[0], data );
		    	} else {
		    		alamatTotal.put( tmp[0], new int[] { 1, Integer.parseInt(tmp[1]) } );
		    	}
		    }
		}
	 
	
		for( String key: alamatTotal.keySet() ) {
			outputRecord.setValue(0, key );
			int[] tmp1 = alamatTotal.get(key);
			System.out.println( key + " "+ String.valueOf(tmp1[0]) +" "+ String.valueOf(tmp1[1]) );
			outputRecord.setValue(1, String.valueOf(tmp1[0]) );
			outputRecord.setValue(2, String.valueOf(tmp1[1]) );
			outputLink.writeRecord(outputRecord);
		}
	 
	
		} catch ( Exception ex ) {
			ex.printStackTrace();
			System.out.println( ex.getMessage() );
		} finally {
			if( this.st == null ) {
				this.st.close();
			}
			if( this.rs == null ) {
				this.rs.close();
			}
			if( this.connection != null ) {
				this.connection.close();
				System.out.println( "Closed" );
			}
		}
	}
 
	@Override
	public boolean validateConfiguration(Configuration configuration, boolean arg1) throws Exception {
		boolean isValidate = true;
	 
	
		for (Object obj : configuration.getUserProperties().keySet() ) {
		    System.out.println( "parameter "+ obj.toString() +" - "+ configuration.getUserProperties().get(obj).toString() );
		    if( obj.toString().equals("urljdbc") ) {
		    	this.urljdbc = configuration.getUserProperties().get(obj).toString() ;
		   	} else if ( obj.toString().equals("username") ) {
		   		this.username = configuration.getUserProperties().get(obj).toString() ;
		    } else if ( obj.toString().equals("password") ) {
		    	this.password = configuration.getUserProperties().get(obj).toString() ;
		    }
		}
	 
	
		if( this.urljdbc == null ) {
			isValidate = false;
		}
		if( this.username == null ) {
			isValidate = false;
		}
		if( this.password == null ) {
			isValidate = false;
		}
		 
	
		try {
			this.connection = DriverManager.getConnection( this.urljdbc, this.username, this.password);
			DriverManager.registerDriver(new org.postgresql.Driver());
		} catch (Exception ex) {
			ex.printStackTrace();
			if( this.connection != null ) {
				this.connection.close();
			}
			isValidate = false;
		} 
	 
	
		this.outputLink = configuration.getOutputLink(0);
	 
		return isValidate;
	}
}