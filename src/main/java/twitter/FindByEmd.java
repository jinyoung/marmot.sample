package twitter;

import java.util.UUID;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteCatalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindByEmd {
	private static final String INPUT_LAYER = "/social/tweets/clusters";
	private static final String OUTPUT_LAYER = "/tmp/social/tweets/result_emd";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		// 강남구의 행정 영역 정보를 획득한다.
		Geometry border = getBorder(marmot);

		// 생성될 결과 레이어의 좌표체계를 위해 tweet 레이어의 것도 동일한 것을
		// 사용하기 위해 tweet 레이어의 정보를 서버에서 얻는다.
		LayerInfo info = catalog.getLayerInfo(INPUT_LAYER);

		// 프로그램 수행 이전에 기존 OUTPUT_LAYER을 제거시킨다.
		marmot.deleteLayer(OUTPUT_LAYER);
		
		Program program = Program.builder()
								// tweet 레이어를 읽어, 서초동 행정 영역과 겹치는 트위 레코드를 검색한다.
								.loadLayer(INPUT_LAYER, SpatialRelation.INTERSECTS, border)
								.project("the_geom,id")
								// 검색된 레코드를 'OUTPUT_LAYER' 레이어에 저장시킨다.
								.storeLayer(OUTPUT_LAYER, "the_geom", info.getSRID())
								.build();
		// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
		marmot.execute("find_emd", program);
		
/*
		// RecordSet에 포함된 모든 레코드를 읽어 화면에 출력시킨다.
		RecordSet rset = marmot.readLayer(OUTPUT_LAYER);
		Record record = DefaultRecord.of(rset.getRecordSchema());
		for ( int i =0; i < 10 && rset.next(record); ++i ) {
			System.out.println(record);
		}
*/
	}

	private static final String EMD_LAYER = "/admin/political/emd/heap";
	private static Geometry getBorder(MarmotClient marmot) throws Exception {
		// 생성될 임시 레이어의 좌표체계를 위해 '읍면동 행정구역' 레이어의 것도 동일한 것을
		// 사용하기 위해 '읍면동 행정구역' 레이어의 정보를 서버에서 얻는다.
		LayerInfo info = marmot.getCatalog().getLayerInfo(EMD_LAYER);
		String srid = info.getSRID();
		
		// 강남구 행정 영역을 저장할 임시 레이어 이름을 생성한다.
		String tempLayerName = "/social/tmp/find_emd/" + UUID.randomUUID().toString();
		try {
			// '읍면동 행정구역' 레이어에서 강남구 행정 영역 정보를 검색하는 프로그램을 구성한다.
			//
			Program program = Program.builder()
									// 읍면동 행정구역 레이어를 읽는다.
									.loadLayer(EMD_LAYER)
									// 강남구 레코드를 검색한다.
									.filter("EMD_KOR_NM=='서초동'")
									// 강남구 행정 영역 컬럼만 뽑는다.
									.project("the_geom")
									// 임시 레이어로 저장한다.
									.storeLayer(tempLayerName, "the_geom", srid)
									.build();
			// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
			marmot.execute("find_emd", program);
			
			// 프로그램 수행으로 생성된 임시 레이어를 읽어 강남구 영역을 읽는다.
			return marmot.readLayer(tempLayerName)
						.stream()
						.findAny().get()
						.getGeometry(0);
		}
		finally {
			// 생성된 임시 레이어를 삭제한다.
			marmot.deleteLayer(tempLayerName);
		}
	}
}
