package basic;

import static marmot.optor.geo.AggregateFunction.AVG;
import static marmot.optor.geo.AggregateFunction.MAX;
import static marmot.optor.geo.AggregateFunction.MIN;
import static marmot.optor.geo.AggregateFunction.STDDEV;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;

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
								.aggregate(MAX("POP"), MIN("POP"), AVG("POP"), STDDEV("POP"))
								.store(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute("transform", program);
		
		SampleUtils.printFilePrefix(marmot, RESULT, 10);
	}
}
