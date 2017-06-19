package carloc;

import static marmot.optor.geo.SpatialRelation.WITHIN_DISTANCE;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.RecordSet;
import marmot.optor.AggregateFunction;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindBestRoadsForPickup {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String ROADS = "교통/도로망/링크";
	private static final String RESULT = "tmp/result";
	private static final String SRID = "EPSG:5186";
	private static final double DISTANCE = 5;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Program program;

		Program rank = Program.builder()
						.rank("count:D", "rank")
						.build();
		program = Program.builder("match_and_rank_roads")
						.load(TAXI_LOG)
						.filter("status == 0")
						.update("hour:int", "hour=ts.substring(8,10)")
						.knnJoin("the_geom", ROADS, 1, WITHIN_DISTANCE(10),
								"hour,car_no,param.{LINK_ID,the_geom,ROAD_NAME,ROADNAME_A}")
						.groupBy("hour,LINK_ID")
								.taggedKeyColumns("the_geom,ROAD_NAME,ROADNAME_A")
								.aggregate(AggregateFunction.COUNT())
						.filter("count >= 50")
						.groupBy("hour").run(rank)
						.storeMarmotFile(RESULT)
						.build();

		StopWatch watch = StopWatch.start();
		marmot.deleteFile(RESULT);
		marmot.execute(program);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 5);
	}
	
	private static void exportResult(MarmotClient marmot, String resultLayerName,
									String baseDirPath) throws IOException {
		export(marmot, resultLayerName, 8, baseDirPath);
		export(marmot, resultLayerName, 22, baseDirPath);
	}
	
	private static void export(MarmotClient marmot, String resultLayerName, int hour,
								String baseName) throws IOException {
		Program program = Program.builder()
								.load(resultLayerName)
								.filter("hour == " + hour)
								.build();
		RecordSet rset = marmot.executeAndGetResult(program);

		String file = String.format("/home/kwlee/tmp/%s_%02d.shp", baseName, hour);
		marmot.writeToShapefile(rset, new File(file), "best_roads", SRID,
								Charset.forName("euc-kr"), false);
	}
}
