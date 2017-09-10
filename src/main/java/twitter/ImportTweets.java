package twitter;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportTweets {
	private static final String RAW_DIR = "로그/social/twitter_raw";
	private static final String OUTPUT_DATASET = "로그/social/twitter";
	private static final String SRID = "EPSG:5186";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		// 질의 처리를 위한 질의 프로그램 생성
		Plan plan = marmot.planBuilder("import_tweets")
								// 'LOG_DIR' 디렉토리에 저장된 Tweet 로그 파일들을 읽는다.
								.load(RAW_DIR)
								// 'coordinates'의 위경도 좌표계를 EPSG:5186으로 변경한 값을
								// 'the_geom' 컬럼에 저장시킨다.
								.transformCRS("the_geom", "the_geom", "EPSG:4326", SRID)
								// 중복된 id의 tweet를 제거시킨다.
								.distinct("id")
								// 'OUTPUT_LAYER'에 해당하는 레이어로 저장시킨다.
								.store(OUTPUT_DATASET)
								.build();
		
		// 프로그램 수행 이전에 기존 OUTPUT_LAYER을 제거시킨다.
//		marmot.deleteDataSet(OUTPUT_DATASET);
		// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
//		DataSet result = marmot.createDataSet(OUTPUT_DATASET, "the_geom", SRID, plan);
		DataSet result = marmot.getDataSet(OUTPUT_DATASET);
		result.cluster();
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
		
		SampleUtils.printPrefix(result, 10);
	}
}
