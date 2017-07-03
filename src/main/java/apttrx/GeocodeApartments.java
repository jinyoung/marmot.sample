package apttrx;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.optor.MapReduceOptions;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeocodeApartments {
	private static final String APT_TRX = "주택/실거래/아파트매매";
	private static final String RESULT = "tmp/아파트실매매/아파트위치";
	
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

		Plan plan;
		plan = marmot.planBuilder("geocode_apts")
						.load(APT_TRX)
						
						// 지오코딩을 위해 대상 아파트의 지번주소 구성
						.update("addr:string", "addr = 시군구 + ' ' + 번지 + ' ' + 단지명")
						// 지오코딩과 관련없는 컬럼 제거
						.project("addr,시군구,번지,단지명")
						// 중복된 아파트 주소를 제거
						// 지오코딩에 소요시간이 많이들기 때문에, distinct시 강제로 많은 수의
						// partition으로 나눠서 수행하도록한다.
						// 이렇게 되면 다음에 수행되는 지오코딩이 각 partition별로
						// 수행되기 때문에 높은 병렬성을 갖게된다.
						.distinct("addr", 29)
						// 지오코딩을 통해 아파트 좌표 계산
						.lookupPostalAddress("addr", "info")
						.update("the_geom:multi_polygon", "the_geom = info.?geometry")
//						.centroid("the_geom", "the_geom")
						.project("*-{addr}")
						.skip(0)
						
						.store(RESULT)
						.build();
		
		marmot.deleteDataSet(RESULT);		
		DataSet result = marmot.createDataSet(RESULT, "the_geom", "EPSG:5186", plan);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
	}
}
