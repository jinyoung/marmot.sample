package oldbldr;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.optor.AggregateFunction;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1FlowPop {
	private static final String FLOW_POP = "주민/유동인구/월별_시간대/2015";
	private static final String EMD = "구역/읍면동";
	private static final String RESULT = "tmp/flowpop_emd";
	
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

		String avgExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.collect(Collectors.joining("+", "(", ")"));
		avgExpr = String.format("avg = %s / 24", avgExpr);
		
		DataSet info = marmot.getDataSet(EMD);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		Plan plan = marmot.planBuilder("읍면동별 2015년도 유동인구 집계")
							.load(FLOW_POP)
							.update("avg:double", avgExpr)
							.update("year:int", "year=std_ym.substring(0,4);")
							.project("the_geom,block_cd,year,avg")
							.spatialJoin("the_geom", EMD, INTERSECTS,
									"*-{the_geom},param.{the_geom,emd_cd,emd_kor_nm as emd_nm}")
							.groupBy("emd_cd")
								.taggedKeyColumns(geomCol + ",year,emd_nm")
								.workerCount(1)
								.aggregate(AVG("avg").as("avg"))
							.project(String.format("%s,*-{%s}", geomCol, geomCol))
							.store(RESULT)
							.build();
		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, geomCol, srid, plan);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());

		SampleUtils.printPrefix(result, 10);
	}
}