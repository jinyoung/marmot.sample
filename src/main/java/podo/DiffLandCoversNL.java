package podo;

import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

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
public class DiffLandCoversNL {
	private static final String LAND_COVER_1987 = "토지/토지피복도/1987S";
	private static final String LAND_COVER_2007 = "토지/토지피복도/2007S";
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
		
		DataSet cover1987 = marmot.getDataSet(LAND_COVER_1987);
		String geomCol = cover1987.getGeometryColumn();

		Plan plan = marmot.planBuilder("토지피복_변화량")
						.load(LAND_COVER_2007, 2)
						.update("분류구 = (분류구.length() > 0) ? 분류구 : 재분류")
						.intersectionJoin(geomCol, LAND_COVER_1987,
											"the_geom,param.분류구 as t1987,분류구 as t2007")
						.expand("area:double", "area = ST_Area(the_geom);")
						.project("*-{the_geom}")
						.groupBy("t1987,t2007")
							.workerCount(1)
							.aggregate(SUM("area").as("total_area"))
						.storeAsCsv(RESULT)
						.build();
		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		
		watch.stop();
		System.out.println("완료: 토지피복도 교차조인");
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
	}
}
