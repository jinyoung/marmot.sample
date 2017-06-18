package twitter;

import static marmot.support.DateTimeFunctions.ST_DTToString;

import java.time.LocalDateTime;

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
public class FindByDateTime {
	private static final String TWEETS = "로그/social/twitter";
	private static final String RESULT = "/tmp/result";
	private static final LocalDateTime FROM = LocalDateTime.of(2015, 12, 30, 0, 0);
	private static final LocalDateTime TO = LocalDateTime.of(2016, 01, 01, 0, 0);

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		// 2015.12.25 부터  2015.12.26 이전까지 tweets을 검색하기 위한 조건 문자열 생성
		String initPred = String.format("$begin=ST_DTFromString('%s'); "
										+ "$end=ST_DTFromString('%s');",
										ST_DTToString(FROM), ST_DTToString(TO));
		String betweenDTPred = "ST_DTIsBetween(created_at,$begin,$end)";
		
		// 질의 처리를 위한 질의 프로그램 생성
		Program program = Program.builder("find_by_datetime")
								// 'INPUT' 디렉토리에 저장된 Tweet 로그 파일들을 읽는다.
								.load(TWEETS)
								// 2015.12.30 부터  2016.01.2 이전까지 레코드만을 뽑아서
								.filter(initPred, betweenDTPred)
								// 레코드에서 'the_geom'과 'id' 컬럼만의 레코드를 만들어서
								.project("the_geom,id,created_at")
								// 'OUTPUT_LAYER'에 해당하는 레이어로 저장시킨다.
								.store(RESULT)
								.build();
		
		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, "the_geom", "EPSG:5186", program);
		
		SampleUtils.printPrefix(result, 10);
	}
}
