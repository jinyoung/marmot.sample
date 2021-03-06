package basic;

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
public class SampleExpand {
	private static final String INPUT = "교통/지하철/서울역사";
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

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		DataSet input = marmot.getDataSet(INPUT);
		String geomCol = input.getGeometryColumn();
		String srid = input.getSRID();

		Plan plan = marmot.planBuilder("update")
							.load(INPUT)
							.expand("the_geom:point,area:double,sig_cd:int",
										"area = ST_Area(the_geom);"
										+ "the_geom = ST_Centroid(the_geom);"
										+ "sig_cd=Integer.parseInt(sig_cd);"
										+ "kor_sub_nm='Station(' + kor_sub_nm + ')'")
							.project("the_geom,area,sig_cd,kor_sub_nm")
							.store(RESULT)
							.build();
		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, geomCol, srid, plan);
		watch.stop();

		SampleUtils.printPrefix(result, 10);
		System.out.printf("elapsed=%s%n", watch.getElapsedTimeString());
	}
}
