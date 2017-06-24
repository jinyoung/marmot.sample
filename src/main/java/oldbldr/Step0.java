package oldbldr;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step0 {
	private static final String BUILDINGS = "건물/통합정보";
	private static final String EMD = "구역/읍면동";
	private static final String RESULT = "tmp/old_ages";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		DataSet info = marmot.getDataSet(BUILDINGS);
		String geomCol = info.getGeometryColumn();
		
		String schema = "old:byte,be5:byte";
		String init = "$now = ST_DateNow();";
		String trans = "$date = (사용승인일자 != null && 사용승인일자.length() >= 8) "
								+ "? ST_DateParse(사용승인일자,'yyyyMMdd') : null;"
						+ "$period = ($date != null) ? ST_DateDaysBetween($date,$now) : -1;"
						+ "$age = $period/365L;"
						+ "old = $age >= 20 ? 1 : 0;"
						+ "be5 = $age >= 5 ? 1 : 0;";
		
		Plan plan = marmot.planBuilder("find_old_buildings")
								.load(BUILDINGS)
								.update(schema, init, trans)
								.spatialJoin("the_geom", EMD, INTERSECTS,
											"the_geom,원천도형ID,old,be5,"
											+ "param.{emd_cd,emd_kor_nm as emd_nm}")
								.groupBy("emd_cd")
									.taggedKeyColumns(geomCol + ",emd_nm")
									.aggregate(SUM("old").as("old_cnt"),
												SUM("be5").as("be5_cnt"), COUNT())
								.storeMarmotFile(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
