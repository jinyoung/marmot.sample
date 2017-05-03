package lx;

import static marmot.optor.geo.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Sample2 {
	private static final String SGG = "lx/sgg_kyounggi/heap";
	private static final String HOTEL = "lx/hotel/heap";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.loadLayer(SGG)
								.aggregateJoin("the_geom", HOTEL, INTERSECTS,
										SUM("cnt_room").as("sum_room"))
								.filter("sum_room >= 200")
								.storeLayer(RESULT, "the_geom", "EPSG:5179")
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("test", program);
		
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
