package appls;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FireStationService {
	private static final String SID = "구역/시도";
	private static final String LAND_USAGE = "토지/용도지역지구";
	private static final String POP = "주민/주거인구100m";
	private static final String GEOM_COL = "the_geom";
	private static final String SRID = "EPSG:5186";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = cl.getOptionValue("host", "localhost");
		int port = cl.getOptionInt("port", 12985);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		Plan plan;
		DataSet result;
		
		DataSet ds = marmot.getDataSet(LAND_USAGE);
		String geomCol = ds.getGeometryColumn();
		String srid = ds.getSRID();
		
		plan = marmot.planBuilder("combine")
					.load(LAND_USAGE)
					.filter("lclas_cl=='UQA100'")
					.spatialJoin(geomCol, POP, INTERSECTS,
								"param.{the_geom as the_geom,인구수 as pop}")
					.store(RESULT)
					.build();
		marmot.deleteDataSet(RESULT);
		result = marmot.createDataSet(RESULT, geomCol, srid, plan);
		
		System.out.println("elapsed: " + watch.stopAndGetElpasedTimeString());
		SampleUtils.printPrefix(result, 5);
	}
}
