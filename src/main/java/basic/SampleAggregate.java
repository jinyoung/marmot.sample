package basic;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.optor.geo.AggregateFunction;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAggregate {
	private static final String INPUT = "admin/workers/heap";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.loadLayer(INPUT)
								.aggregate(AggregateFunction.MAX("POP"), AggregateFunction.MIN("POP"),
											AggregateFunction.STDDEV("POP"))
								.storeAsCsv(RESULT)
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("transform", program);
	}
}
