package lx;

import static marmot.optor.geo.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.WITHIN_DISTANCE;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Sample3 {
	private static final String SGG = "lx/sgg_kyounggi/heap";
	private static final String VISITOR = "lx/tour_visitor/heap";
	private static final String HOTEL = "lx/hotel/heap";
	private static final String RESULT = "tmp/result";
	private static final String TEMP_VISITOR = "tmp/visitors";
	private static final String TEMP02 = "tmp/temp02";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program;
		
		program = Program.builder()
						.loadLayer(VISITOR)
						.filter("sub3_nm.contains('수련장')")
						.storeLayer(TEMP_VISITOR, "the_geom", "EPSG:5179")
						.build();
		marmot.deleteLayer(TEMP_VISITOR);
		marmot.execute("visitors", program);
		
		program = Program.builder()
						.loadLayer(HOTEL)
						.aggregateJoin("the_geom", TEMP_VISITOR, WITHIN_DISTANCE(10000),
										SUM("tot_13").as("sum_13"),
										SUM("tot_14").as("sum_14"),
										SUM("tot_15").as("sum_15"))
						.storeLayer(RESULT, "the_geom", "EPSG:5179")
						.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("buffer", program);
		
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
