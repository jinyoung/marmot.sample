package twitter;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.RemotePlan;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcSggHistogram {
	private static final String LEFT_DATASET = "로그/social/twitter";
	private static final String RIGHT_DATASET = "구역/시군구";
	private static final String OUTPUT_DATASET = "/tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
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
		
		Plan plan = marmot.planBuilder("calc_emd_histogram")
								.loadSpatialIndexJoin(LEFT_DATASET, RIGHT_DATASET,
											SpatialRelation.INTERSECTS,
											"left.{id},right.{the_geom,SIG_CD,SIG_KOR_NM}")
								.groupBy("SIG_CD")
									.taggedKeyColumns("the_geom,SIG_KOR_NM")
									.count()
								.store(OUTPUT_DATASET)
								.build();
		// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
		marmot.deleteDataSet(OUTPUT_DATASET);
		marmot.createDataSet(OUTPUT_DATASET, "the_geom", "EPSG:5186", plan);
	}
}
