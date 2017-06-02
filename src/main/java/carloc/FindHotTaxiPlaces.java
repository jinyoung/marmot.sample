package carloc;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteCatalog;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindHotTaxiPlaces {
	private static final String SRID = "EPSG:5186";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("find_hot_taxi_places ");
		parser.addArgOption("taxi_log", "name", "taxi trip layer name", true);
		parser.addArgOption("emd", "name", "emd layer name", true);
		parser.addArgOption("output", "path", "output file path", true);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}
		
		String taxiTripLogLayer = cl.getOptionValue("taxi_log");
		String emdLayerName = cl.getOptionValue("emd");
		String outputLayerName = cl.getOptionValue("output");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		Program rank = Program.builder()
								.rank("count:D", "rank")
								.build();
		
		Program program = Program.builder()
								.loadLayer(taxiTripLogLayer)
								.filter("status==1 || status==2")
								.spatialJoin("the_geom", emdLayerName, SpatialRelation.INTERSECTS,
											"car_no,status,ts,param.{the_geom, EMD_CD,EMD_KOR_NM}")
								.transform("hour:int", "hour=ST_DTGetHour(ST_DTFromMillis(ts))")
								.groupBy("hour,status,EMD_CD")
										.taggedKeyColumns("EMD_KOR_NM,the_geom")
										.aggregate(AggregateFunction.COUNT())
								.filter("count > 50")
								.groupBy("hour,status").run(rank)
								.storeLayer(outputLayerName, "the_geom", SRID)
								.build();

		marmot.deleteLayer(outputLayerName);
		marmot.execute("find_hot_taxi_places", program);
	}
}
