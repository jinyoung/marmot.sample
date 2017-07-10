package yunsei;

import static marmot.optor.AggregateFunction.AVG;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.DataSet;
import marmot.DataSetType;
import marmot.Plan;
import marmot.optor.JoinOptions;
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
	private static final String BLOCK_CENTERS = "구역/지오비전_집계구_Point";
	private static final String FLOW_POP = "주민/유동인구_평균/시간대/2015";
	private static final String TEMP_FLOW_POP = "tmp/geom_flow_pop";
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

		String host = cl.getOptionValue("host", "localhost");
		int port = cl.getOptionInt("port", 12985);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		Plan plan;
		DataSet result;
		
		prepareFlowPopulation(marmot);
		
		E2SFCAParameters params1 = new E2SFCAParameters();
		params1.inputDataset(TEMP_FLOW_POP);
		params1.paramDataset(BUS_SUPPLY);
		params1.outputDataset(RESULT_BUS);
		params1.inputFeatureColumns("avg_08tmst","avg_15tmst");
		params1.paramFeatureColumns("slevel_08","slevel_15");
		params1.outputFeatureColumns("index_08", "index_15");
		params1.radius(400);
		params1.distanceDecayFunction(DistanceDecayFunctions.fromString("power:-0.1442"));
		marmot.executeProcess("e2sfca", params1.toMap());
		
		E2SFCAParameters params2 = new E2SFCAParameters();
		params2.inputDataset(TEMP_FLOW_POP);
		params2.paramDataset(SUBWAY_SUPPLY);
		params2.outputDataset(RESULT_SUBWAY);
		params2.inputFeatureColumns("avg_08tmst","avg_15tmst");
		params2.paramFeatureColumns("slevel_08","slevel_15");
		params2.outputFeatureColumns("index_08", "index_15");
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
						.aggregate(AVG("index_08").as("index_08"),
									AVG("index_15").as("index_15"))
					.store(RESULT)
					.build();
		marmot.deleteDataSet(RESULT);
		marmot.createDataSet(RESULT, busResult.getGeometryColumn(), busResult.getSRID(), plan);
		
		marmot.deleteDataSet(RESULT_CONCAT);
		marmot.deleteDataSet(RESULT_SUBWAY);
		marmot.deleteDataSet(RESULT_BUS);
		marmot.deleteDataSet(TEMP_FLOW_POP);
		
		System.out.println("done, elapsed=" + watch.stopAndGetElpasedTimeString());
	}
	
	private static void prepareFlowPopulation(MarmotClient marmot) {
		Plan plan;
		
		DataSet bus = marmot.getDataSet(BUS_SUPPLY);
		String busGeomCol = bus.getGeometryColumn();
		
		plan = marmot.planBuilder("")
						.load(BLOCK_CENTERS)
						.join("block_cd", FLOW_POP, "block_cd",
								"*,param.{avg_08tmst,avg_15tmst}",
								new JoinOptions().workerCount(32))
						.store(TEMP_FLOW_POP)
						.build();
		marmot.deleteDataSet(TEMP_FLOW_POP);
		marmot.createDataSet(TEMP_FLOW_POP, "the_geom", "EPSG:5186", plan);
	}
}
