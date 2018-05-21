import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class ReadHbaseTable {
	public static void main(String args[]) throws IOException{
		System.out.println("Scanning the HBASE Table");
		Configuration c = HBaseConfiguration.create(); // Instantiate configuration class
		HTable table = new HTable(c, "Transactions_Hbase");       // Instantiate HTable class
		Scan scan = new Scan();      // Instantiate the Scan class
		 // Scanning the required columns
	      scan.addColumn(Bytes.toBytes("customer"), Bytes.toBytes("fname")); //First parameter indicates column family and second parameter is column name
	      scan.addColumn(Bytes.toBytes("customer"), Bytes.toBytes("count")); //Add all the required columns
	      
		ResultScanner scanner = table.getScanner(scan);      // Get scan result
		// Reading values from scan result
		for (Result result = scanner.next(); result != null; result = scanner.next())
		{
			System.out.println("Result Found: " + result);
			//Convert Bytes to String 
			
			System.out.println("Result Found (fname): " + Bytes.toString(result.getValue(Bytes.toBytes("customer"),Bytes.toBytes("fname"))));
			System.out.println("Result Found (count): " + Bytes.toString(result.getValue(Bytes.toBytes("customer"),Bytes.toBytes("count"))));
		}
		scanner.close();      //close the scanner
		System.out.println("Scanning finished");
	}
}
