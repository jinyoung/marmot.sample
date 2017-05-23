package basic;

import static marmot.optor.geo.AggregateFunction.COUNT;
import static marmot.optor.geo.AggregateFunction.MAX;
import static marmot.optor.geo.AggregateFunction.MIN;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleGroupByAggregate {
	private static final String INPUT = "transit/subway_stations/heap";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.loadLayer(INPUT)
								.groupBy("SIG_CD")
									.aggregate(COUNT(), MAX("SUB_STA_SN"), MIN("SUB_STA_SN"))
								.store(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute("transform", program);

		SampleUtils.printFilePrefix(marmot, RESULT, 10);
	}
}
