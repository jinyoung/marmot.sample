package carloc;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindHotTaxiPlaces {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String EMD = "시연/서울_읍면동";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Program rank = Program.builder()
								.rank("count:D", "rank")
								.build();
		
		Program program = Program.builder("find_hot_taxi_places")
								.load(TAXI_LOG)
								.filter("status==1 || status==2")
								.spatialJoin("the_geom", EMD, SpatialRelation.INTERSECTS,
											"car_no,status,ts,param.{the_geom, EMD_CD,EMD_KOR_NM}")
								.update("hour:int", "hour=ts.substring(8,10)")
								.groupBy("hour,status,EMD_CD")
										.taggedKeyColumns("EMD_KOR_NM,the_geom")
										.aggregate(AggregateFunction.COUNT())
								.filter("count > 50")
								.groupBy("hour,status").run(rank)
								.storeMarmotFile(RESULT)
								.build();

		StopWatch watch = StopWatch.start();
		marmot.deleteFile(RESULT);
		marmot.execute(program);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 5);
	}
}
