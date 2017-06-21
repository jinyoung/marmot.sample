package twitter;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindByEmd {
	private static final String TWEETS = "로그/social/twitter";
	private static final String RESULT = "/tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		// 강남구의 행정 영역 정보를 획득한다.
		Geometry border = getBorder(marmot);

		// 생성될 결과 레이어의 좌표체계를 위해 tweet 레이어의 것도 동일한 것을
		// 사용하기 위해 tweet 레이어의 정보를 서버에서 얻는다.
		DataSet info = marmot.getDataSet(TWEETS);

		// 프로그램 수행 이전에 기존 OUTPUT_LAYER을 제거시킨다.
		marmot.deleteDataSet(RESULT);
		
		Program program = Program.builder("find_emd")
								// tweet 레이어를 읽어, 서초동 행정 영역과 겹치는 트위 레코드를 검색한다.
								.load(TWEETS, SpatialRelation.INTERSECTS, border)
								.project("the_geom,id")
								// 검색된 레코드를 'OUTPUT_LAYER' 레이어에 저장시킨다.
								.store(RESULT)
								.build();
		
		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, "the_geom", info.getSRID(), program);
		
		SampleUtils.printPrefix(result, 10);
	}

	private static final String EMD = "구역/읍면동";
	private static Geometry getBorder(MarmotClient marmot) throws Exception {
		// '읍면동 행정구역' 레이어에서 강남구 행정 영역 정보를 검색하는 프로그램을 구성한다.
		//
		Program program = Program.builder("find_emd")
								// 읍면동 행정구역 레이어를 읽는다.
								.load(EMD)
								// 강남구 레코드를 검색한다.
								.filter("emd_kor_nm=='서초동'")
								// 강남구 행정 영역 컬럼만 뽑는다.
								.project("the_geom")
								.build();
		// 프로그램 수행으로 생성된 임시 레이어를 읽어 강남구 영역을 읽는다.
		return marmot.executeSequentially(program)
						.stream()
						.findAny().get()
						.getGeometry(0);
	}
}
