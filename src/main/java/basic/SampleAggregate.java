package basic;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.STDDEV;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAggregate {
	private static final String INPUT = "POI/주유소_가격";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Plan plan = marmot.planBuilder("sample_aggreate")
							.load(INPUT)
							.filter("휘발유 > 0")
							.aggregate(MAX("휘발유"), MIN("휘발유"), AVG("휘발유"), STDDEV("휘발유"))
							.storeMarmotFile(RESULT)
							.build();
		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
