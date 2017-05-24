package bizarea;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.MarmotDataSet;
import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteMarmotDataSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step2 {
	private static final String BIZ_GRID_SALES = "tmp/biz/sales_grid100";
	private static final String BIZ_GRID_FLOW_POP = "tmp/biz/flow_pop_grid100";
	private static final String SID_SGG = "admin/political/sid_sgg/heap";
	private static final String RESULT = "tmp/biz/grid_sales_flowpop";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(BIZ_GRID_SALES);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		MarmotDataSet bizGridFlowPop = RemoteMarmotDataSet.layer(BIZ_GRID_FLOW_POP);
		String script = "if ( std_ym == null ) {std_ym = param_std_ym;}"
						+ "if ( cell_id == null ) {cell_id = param_cell_id;}"
						+ "if ( sgg_cd == null ) {sgg_cd = param_sgg_cd;}";

		Program program = Program.builder()
								.loadLayer(BIZ_GRID_SALES)
								.join("std_ym,cell_id,sgg_cd", bizGridFlowPop, "std_ym,cell_id,sgg_cd",
										"*, param.{"
											+ "the_geom as param_the_geom,"
											+ "std_ym as param_std_ym,"
											+ "cell_id as param_cell_id,"
											+ "sgg_cd as param_sgg_cd,"
											+ "flow_pop}", opt->opt.workerCount(16))
								.update(script)
								.project("*-{param_the_geom,param_std_ym,param_cell_id,param_sgg_cd}")
								// 최종 결과에 행정도 코드를 부여한다.
								.spatialJoin("the_geom", SID_SGG, INTERSECTS,
											"*-{cell_pos},param.*-{the_geom,sgg_cd}")
								.storeLayer(RESULT, geomCol, srid)
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("merge", program);
		
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
