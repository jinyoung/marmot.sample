package twitter;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 본 클래스는 트위트 레이어를 읽어서, 2015.12.30 부터  2016.01.2이전까지의 트윗을
 * 읽어 그 중 'the_geom'과 'id'에 해당하는 값을 화면에 출력시킨다. 
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportTweets {
	private static final String RAW_DIR = "로그/social/twitter_raw";
	private static final String OUTPUT_DATASET = "로그/social/twitter";
	private static final String SRID = "EPSG:5186";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		// 질의 처리를 위한 질의 프로그램 생성
		Program program = Program.builder("import_tweets")
								// 'LOG_DIR' 디렉토리에 저장된 Tweet 로그 파일들을 읽는다.
								.load(RAW_DIR)
								// 'coordinates'의 위경도 좌표계를 EPSG:5186으로 변경한 값을
								// 'the_geom' 컬럼에 저장시킨다.
								.transformCRS("the_geom", "EPSG:4326", "the_geom", SRID)
								// 중복된 id의 tweet를 제거시킨다.
								.distinct("id")
								// 'OUTPUT_LAYER'에 해당하는 레이어로 저장시킨다.
								.store(OUTPUT_DATASET)
								.build();
		
		// 프로그램 수행 이전에 기존 OUTPUT_LAYER을 제거시킨다.
		marmot.deleteDataSet(OUTPUT_DATASET);
		// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
		DataSet result = marmot.createDataSet(OUTPUT_DATASET, "the_geom", SRID, program);
		result.cluster();
		
		SampleUtils.printPrefix(result, 10);
	}
}
