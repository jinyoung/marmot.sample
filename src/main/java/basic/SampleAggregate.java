package basic;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.STDDEV;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

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
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
