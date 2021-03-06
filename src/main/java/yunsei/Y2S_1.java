package yunsei;

import static marmot.optor.AggregateFunction.SUM;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.vividsolutions.jts.geom.Geometry;

import marmot.DataSet;
import marmot.DataSetType;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.geo.SpatialRelation;
import marmot.process.geo.DistanceDecayFunctions;
import marmot.process.geo.E2SFCAParameters;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.DimensionDouble;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Y2S_1 {
	private static final String BUS_SUPPLY = "연세대/강남구_버스";
	private static final String SUBWAY_SUPPLY = "연세대/강남구_지하철";
	private static final String FLOW_POP = "주민/유동인구/시간대/2015";
	private static final String SGG = "구역/시군구";
	private static final String TEMP_FLOW_POP_GANGNAM = "tmp/flow_pop_gangnam";
	private static final String RESULT_BUS = "tmp/E2SFCA/bus";
	private static final String RESULT_SUBWAY = "tmp/E2SFCA/subway";
	private static final String RESULT_CONCAT = "tmp/concat";
	private static final String RESULT = "tmp/result";
	
	private static final DimensionDouble CELL_SIZE = new DimensionDouble(1000,1000);
	private static final int NWORKERS = 25;
	
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
		
		getGangnumGuFlowPop(marmot, TEMP_FLOW_POP_GANGNAM);
		
		E2SFCAParameters params1 = new E2SFCAParameters();
		params1.inputDataset(TEMP_FLOW_POP_GANGNAM);
		params1.paramDataset(BUS_SUPPLY);
		params1.outputDataset(RESULT_BUS);
		params1.inputFeatureColumns("avg_08tmst","avg_15tmst");
		params1.paramFeatureColumns("slevel_08","slevel_15");
		params1.outputFeatureColumns("index_08", "index_15");
		params1.taggedColumns("block_cd");
		params1.radius(400);
		params1.distanceDecayFunction(DistanceDecayFunctions.fromString("power:-0.1442"));
		marmot.executeProcess("e2sfca", params1.toMap());
		
		E2SFCAParameters params2 = new E2SFCAParameters();
		params2.inputDataset(TEMP_FLOW_POP_GANGNAM);
		params2.paramDataset(SUBWAY_SUPPLY);
		params2.outputDataset(RESULT_SUBWAY);
		params2.inputFeatureColumns("avg_08tmst","avg_15tmst");
		params2.paramFeatureColumns("slevel_08","slevel_15");
		params2.outputFeatureColumns("index_08", "index_15");
		params2.taggedColumns("block_cd");
		params2.radius(800);
		params2.distanceDecayFunction(DistanceDecayFunctions.fromString("power:-0.1442"));
		marmot.executeProcess("e2sfca", params2.toMap());
		
		DataSet busResult = marmot.getDataSet(RESULT_BUS);
		String hdfsPath = busResult.getHdfsPath();
		String parentPath = FilenameUtils.getFullPathNoEndSeparator(hdfsPath);
		marmot.deleteDataSet(RESULT_CONCAT);
		marmot.bindExternalDataSet(RESULT_CONCAT, parentPath, DataSetType.FILE,
									busResult.getGeometryColumn(), busResult.getSRID());
		
		plan = marmot.planBuilder("merge")
					.load(RESULT_CONCAT)
					.groupBy("block_cd")
						.taggedKeyColumns("the_geom")
						.aggregate(SUM("index_08").as("index_08"),
								SUM("index_15").as("index_15"))
					.store(RESULT)
					.build();
		marmot.deleteDataSet(RESULT);
		marmot.createDataSet(RESULT, busResult.getGeometryColumn(), busResult.getSRID(), plan);
		
		marmot.deleteDataSet(RESULT_CONCAT);
		marmot.deleteDataSet(RESULT_SUBWAY);
		marmot.deleteDataSet(RESULT_BUS);
		marmot.deleteDataSet(TEMP_FLOW_POP_GANGNAM);
		
		System.out.println("done, elapsed=" + watch.stopAndGetElpasedTimeString());
	}
	
	private static Geometry getGangnamGu(MarmotClient marmot) {
		Plan plan;
		plan = marmot.planBuilder("강남구 추출")
					.load(SGG)
					.filter("sig_cd.startsWith('11') && sig_kor_nm == '강남구'")
					.project("the_geom")
					.build();
		return marmot.executeLocally(plan).toList().get(0).getGeometry("the_geom");
	}
	
	private static void getGangnumGuFlowPop(MarmotClient marmot, String output) {
		Plan plan;
		
		Geometry gangnaum = getGangnamGu(marmot);
		
		DataSet flowPop = marmot.getDataSet(FLOW_POP);
		String geomCol = flowPop.getGeometryColumn();
		String srid = flowPop.getSRID();
		
		plan = marmot.planBuilder("강남구 영역 유동인구 정보 추출")
						.load(FLOW_POP, SpatialRelation.INTERSECTS, gangnaum)
						.project("the_geom,block_cd,avg_08tmst,avg_15tmst")
						.store(output)
						.build();
		marmot.deleteDataSet(output);
		DataSet result = marmot.createDataSet(output, geomCol, srid, plan);
	}
}
