package carloc;

import marmot.Program;
import marmot.ProgramBuilder;
import marmot.Record;
import marmot.RecordSet;
import marmot.optor.geo.SpatialRelationship;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RankMatchingRoads {
	private static final String TRAVEL = "tmp/taxi/travel";
	private static final String ROAD = "transit/road_link_heap";
	private static final int MIN_COUNT = 10;
	private static final String RESULT = "tmp/taxi/road_ranks";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();

		Program rank = Program.builder()
								.rank("count:D", "rank")
								.build();
		ProgramBuilder builder = Program.builder()
										.loadSpatialIndexJoin(SpatialRelationship.INTERSECTS,
															TRAVEL, ROAD, "the_geom,hour,car_no",
															"LINK_ID")
										.groupBy("hour,car_no,LINK_ID").findAny()
										.groupBy("hour,LINK_ID").count();
		if ( MIN_COUNT > 0 ) {
			builder.filter("count >= " + MIN_COUNT);
		}
		Program program = builder.groupBy("hour").run(rank)
								.store(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute("rank_matching_roads", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		RecordSet rset = marmot.readLayer(RESULT);
		Record record = DefaultRecord.of(rset.getRecordSchema());
		int count = 0;
		while ( ++count <= 10 && rset.next(record) ) {
			System.out.println(record);
		}
	}
}
