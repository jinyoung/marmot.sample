package basic;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;

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
public class SampleGroupByAggregate {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Plan plan = marmot.planBuilder("group_by")
							.load(INPUT)
							.groupBy("sig_cd")
								.aggregate(COUNT(), MAX("sub_sta_sn"), MIN("sub_sta_sn"))
							.storeMarmotFile(RESULT)
							.build();
		marmot.deleteFile(RESULT);
		marmot.execute(plan);

		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
