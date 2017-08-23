package HBaseIA.TwitBase.coprocessors;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

public class CounterEndPointDemo {
	
	public static void main(String[] args) throws IOException, Throwable {
		 final String startRow = args[0];
         final String endRow = args[1];

         @SuppressWarnings("resource")
         HTableInterface table = new HTable(HBaseConfiguration.create(), "tc");
         Map<byte[], Long> results ;
         
         results = table.coprocessorExec(RelationCountProtocol.class,
        		 startRow.getBytes(), endRow.getBytes(),
        		 new Batch.Call<RelationCountProtocol, Long>(){

					@Override
					public Long call(RelationCountProtocol instance) throws IOException {
						return instance.followedByCount(startRow);
					}
         });
         
         long total = 0;
         for (Map.Entry<byte[], Long> e : results.entrySet()) {
                 System.out.println(e.getValue());
                 total += e.getValue();
         }
         System.out.println("total:" + total);
	}

}
