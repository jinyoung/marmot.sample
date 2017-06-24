package carloc;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindHotHospitals {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String HOSPITAL = "시연/hospitals";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Plan plan = marmot.planBuilder("find_hot_hospitals")
								.load(TAXI_LOG)
								.filter("status==1 || status==2")
								.spatialJoin("the_geom", HOSPITAL,
											SpatialRelation.WITHIN_DISTANCE(50),
											"param.{the_geom,gid,bplc_nm,bz_stt_nm}")
								.filter("bz_stt_nm=='운영중'")
								.groupBy("gid")
									.taggedKeyColumns("the_geom,bplc_nm")
									.aggregate(AggregateFunction.COUNT())
								.rank("count:D", "rank")
								.storeMarmotFile(RESULT)
								.build();

		StopWatch watch = StopWatch.start();
		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 5);
	}
}
