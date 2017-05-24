package twitter;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.RecordSchema;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.type.DataType;

/**
 * 본 클래스는 트위트 레이어를 읽어서, 2015.12.30 부터  2016.01.2이전까지의 트윗을
 * 읽어 그 중 'the_geom'과 'id'에 해당하는 값을 화면에 출력시킨다. 
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportTweets {
	private static final String LOG_DIR = "data/social/twitter";
	private static final String OUTPUT_LAYER = "social/tweets/heap";
	
	private static final RecordSchema SCHEMA = RecordSchema.builder()
													.addColumn("the_geom", DataType.POINT)
													.addColumn("id", DataType.LONG)
													.addColumn("user_id", DataType.LONG)
													.addColumn("created_at", DataType.DATETIME)
													.addColumn("coordinates", DataType.POINT)
													.addColumn("text", DataType.STRING)
													.build();
	private static final String INIT_SCRIPT = "$format=ST_DTPattern('EEE MMM dd HH:mm:ss Z yyyy').withLocale(Locale.ENGLISH)";
	private static final String PARSE = "$json = ST_ParseJSON(text);"
										+ "id = $json.id;"
										+ "user_id = $json.user.id;"
										+ "created_at = ST_DTParseLE($json.created_at, $format);"
										+ "coordinates = $json.coordinates != null "
													+ "? ST_GeomFromGeoJSON($json.coordinates) "
													+ ": ST_EmptyPoint();"
										+ "text = $json.text";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		String targetSrid = "EPSG:5186";
		
		// 질의 처리를 위한 질의 프로그램 생성
		Program program = Program.builder()
								// 'LOG_DIR' 디렉토리에 저장된 Tweet 로그 파일들을 읽는다.
								.loadTextFile(LOG_DIR)
								// Json 포맷 파싱
								.transform(SCHEMA, PARSE,
											opts->opts.initializeScript(INIT_SCRIPT))
								// 'coordinates'의 위경도 좌표계를 EPSG:5186으로 변경한 값을
								// 'the_geom' 컬럼에 저장시킨다.
								.transformCRS("coordinates", "the_geom", "EPSG:4326", targetSrid)
								// 중복된 id의 tweet를 제거시킨다.
								.distinct("id")
								// 'OUTPUT_LAYER'에 해당하는 레이어로 저장시킨다.
								.storeLayer(OUTPUT_LAYER, "the_geom", targetSrid)
								.build();
		
		// 프로그램 수행 이전에 기존 OUTPUT_LAYER을 제거시킨다.
		marmot.deleteLayer(OUTPUT_LAYER);
		// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
		marmot.execute("find_by_datetime", program);
		
		SampleUtils.printLayerPrefix(marmot, OUTPUT_LAYER, 10);
	}
}
