package carloc;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.RecordSet;
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
public class FindBestRoadsForPickup {
	private static final String SRID = "EPSG:5186";
	private static final double DISTANCE = 5;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("find_hot_hospitals ");
		parser.addArgOption("taxi_log", "name", "taxi trip layer name", true);
		parser.addArgOption("road", "name", "roadlink layer name", true);
		parser.addArgOption("output", "path", "output file path", true);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}
		
		String taxiTripLogLayer = cl.getOptionValue("taxi_log");
		String roadLayerName = cl.getOptionValue("road");
		String outputLayerName = cl.getOptionValue("output");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		Program program;

		Program rank = Program.builder()
						.rank("count:D", "rank")
						.build();
		program = Program.builder()
						.loadLayer(taxiTripLogLayer)
						.filter("status == 0")
						.transform("hour:int", "hour = ST_DTGetHour(ST_DTFromMillis(ts))")
						.knnJoin("the_geom", roadLayerName, 1, SpatialRelation.WITHIN_DISTANCE(10),
								"hour,car_no,param.{LINK_ID,the_geom,ROAD_NAME,ROADNAME_A}")
						.groupBy("hour,LINK_ID")
								.taggedKeyColumns("the_geom,ROAD_NAME,ROADNAME_A")
								.aggregate(AggregateFunction.COUNT())
						.filter("count >= 50")
						.groupBy("hour").run(rank)
						.storeLayer(outputLayerName, "the_geom", SRID)
						.build();
		marmot.deleteLayer(outputLayerName);
		marmot.execute("match_and_rank_roads", program);
		
//		exportResult(marmot, outputLayerName, "best_roads");
	}
	
	private static void exportResult(MarmotClient marmot, String resultLayerName,
									String baseDirPath) throws IOException {
		export(marmot, resultLayerName, 8, baseDirPath);
		export(marmot, resultLayerName, 22, baseDirPath);
	}
	
	private static void export(MarmotClient marmot, String resultLayerName, int hour,
								String baseName) throws IOException {
		Program program = Program.builder()
								.loadLayer(resultLayerName)
								.filter("hour == " + hour)
								.build();
		RecordSet rset = marmot.executeAndGetResult(program);

		String file = String.format("/home/kwlee/tmp/%s_%02d.shp", baseName, hour);
		marmot.writeToShapefile(rset, new File(file), "best_roads", SRID, Charset.forName("euc-kr"));
	}
}
