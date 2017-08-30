package carloc;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.optor.AggregateFunction;
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
public class FindHotHospitalsTemp {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String HOSPITAL = "시연/hospitals";
	private static final String RESULT = "tmp/result";
	
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
		
		StopWatch watch;
		watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		Plan plan = marmot.planBuilder("find_hot_hospitals")
								.load(TAXI_LOG)
								.filter("status==1 || status==2")
								.spatialJoin("the_geom", HOSPITAL,
											SpatialRelation.WITHIN_DISTANCE(50),
											"the_geom,param.{gid,bplc_nm,bz_stt_nm}")
								.filter("bz_stt_nm=='운영중'")
								.store(RESULT)
								.build();

		marmot.deleteDataSet(RESULT);
		marmot.createDataSet(RESULT, "the_geom", "EPSG:5186", plan);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
//		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 5);
	}
}
