package geom.advanced;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
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
public class SampleBuildSpatialHistogram {
	private static final String STATIONS = "POI/주유소_가격";
	private static final String BORDER = "시연/서울특별시";
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
		
		DataSet borderLayer = marmot.getDataSet(BORDER);
		Envelope envl = borderLayer.getBounds();
		DimensionDouble cellSize = new DimensionDouble(envl.getWidth() / 30,
														envl.getHeight() / 30);
		
		List<String> valueCols = Lists.newArrayList("휘발유", "경유");
		Plan plan = marmot.planBuilder("build_spatial_histogram")
								// 서울특별시 구역을 기준으로 사각 그리드를 생성함.
								.loadSquareGridFile(BORDER, cellSize)
								// 사각 그리드 셀 중에서 서울특별시 영역만 필터링.
								.spatialSemiJoin("the_geom", BORDER, INTERSECTS, false)
								// 사각 그리드 셀 중에서 주유소와 겹치는 셀만 필터링.
//								.spatialSemiJoin("the_geom", STATIONS, SpatialRelation.INTERSECTS)
								.buildSpatialHistogram("the_geom", STATIONS, valueCols)
								.update("cell_pos:string,count:int",
										"cell_pos = cell_pos.toString(); count=count;")
								.store(RESULT)
								.build();

		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, "the_geom", borderLayer.getSRID(), plan);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 10);
	}
}
