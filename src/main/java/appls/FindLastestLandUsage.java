package appls;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.vividsolutions.jts.geom.Geometry;

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
public class FindLastestLandUsage {
	private static final String SID = "구역/시도";
	private static final String LAND_USAGE = "토지/토지이용계획";
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

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		Plan plan;
		DataSet result;
		
		DataSet ds = marmot.getDataSet(LAND_USAGE);
		String geomCol = ds.getGeometryColumn();
		String srid = ds.getSRID();
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		Geometry seoul = getSeoulBoundary(marmot);
		
		plan = marmot.planBuilder("combine")
					.load(LAND_USAGE)
					.project("등록일자")
					.distinct("등록일자")
					.sort("등록일자:D")
					.store(RESULT)
					.build();
		marmot.deleteDataSet(RESULT);
		marmot.createDataSet(RESULT, plan);
		
		System.out.println("elapsed: " + watch.stopAndGetElpasedTimeString());
		
//		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
	
	private static Geometry getSeoulBoundary(MarmotClient marmot) {
		Plan plan;
		
		DataSet sid = marmot.getDataSet(SID);
		plan = marmot.planBuilder("get_seoul")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.build();
		return marmot.executeLocally(plan).toList().get(0)
					.getGeometry(sid.getGeometryColumn());
	}
}
