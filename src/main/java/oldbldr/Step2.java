package oldbldr;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

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
public class Step2 {
	private static final String FLOW_POP = "주민/유동인구/시간대/2015";
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

		AggregateFunction[] aggrFuncs = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.map(name -> SUM(name).as(name))
								.toArray(sz -> new AggregateFunction[sz]);
		
		DataSet info = marmot.getDataSet(EMD);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		Plan plan = marmot.planBuilder("flow_pop_on_emd")
							.load(FLOW_POP)
							.spatialJoin("the_geom", EMD, INTERSECTS,
									"*,param.{emd_cd,emd_kor_nm as emd_nm}")
							.groupBy("emd_cd,std_ym")
								.taggedKeyColumns(geomCol + ",emd_nm")
								.aggregate(aggrFuncs)
							.project(String.format("%s,*-{%s}", geomCol, geomCol))
							.store(RESULT)
							.build();
		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, geomCol, srid, plan);

		SampleUtils.printPrefix(result, 10);
	}
}