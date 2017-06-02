package oldbldr;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step0 {
	private static final String LAND_MASTER = "admin/land/registry/master/heap";
	private static final String EMD = "admin/political/emd/heap";
	private static final String RESULT = "tmp/old_ages";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(LAND_MASTER);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		String schema = "old:byte,be5:byte";
		String init = "$now = ST_DateNow();";
		String trans = "$date = (trsct_dtim != null && trsct_dtim.length() >= 8) "
								+ "? ST_DateParse(trsct_dtim.substring(0,8),'yyyyMMdd') : null;"
						+ "$period = ($date != null) ? ST_DateDaysBetween($date,$now) : -1;"
						+ "$age = $period/365L;"
						+ "old = $age >= 20 ? 1 : 0;"
						+ "be5 = $age >= 5 ? 1 : 0;";
		
		Program program = Program.builder()
								.loadLayer(LAND_MASTER)
								.update(schema, trans, opts->opts.initializeScript(init))
								.spatialJoin("the_geom", EMD, INTERSECTS,
											"the_geom,mgm_bldrgs,old,be5,"
											+ "param.{emd_cd,emd_kor_nm as emd_nm}")
								.groupBy("emd_cd")
									.taggedKeyColumns(geomCol + ",emd_nm")
									.aggregate(SUM("old").as("old_cnt"),
												SUM("be5").as("be5_cnt"), COUNT())
								.storeLayer(RESULT, geomCol, srid)
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("card_sales", program);
		
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
