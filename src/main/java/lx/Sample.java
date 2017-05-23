package lx;

import static marmot.optor.geo.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Sample {
	private static final String SGG = "lx/sgg_kyounggi/heap";
	private static final String VISITOR = "lx/tour_visitor/heap";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.loadLayer(SGG)
								.aggregateJoin("the_geom", VISITOR, INTERSECTS,
										SUM("tot_15").as("sum_15"))
								.filter("sum_15 >= 500000")
								.storeLayer(RESULT, "the_geom", "EPSG:5179")
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("test", program);
		
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
