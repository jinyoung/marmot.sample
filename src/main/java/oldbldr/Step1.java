package oldbldr;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.MarmotDataSet;
import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.optor.AggregateFunction;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteMarmotDataSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1 {
	private static final String FLOW_POP = "data/geo_vision/flow_pop/2015/time";
	private static final String BLOCK_CENTERS = "geo_vision/block_centers/heap";
	private static final String EMD = "admin/political/emd/heap";
	private static final String RESULT = "tmp/flowpop_emd";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		AggregateFunction[] aggrFuncs = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.map(name -> AVG(name).as(name))
								.toArray(sz -> new AggregateFunction[sz]);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(EMD);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		MarmotDataSet block_centers = RemoteMarmotDataSet.layer(BLOCK_CENTERS);
		
		Program program = Program.builder()
								.loadCsvFiles(FLOW_POP)
								.join("block_cd", block_centers, "block_cd",
										"param.*-{block_cd},*", opt->opt.workerCount(32))
								.spatialJoin("the_geom", EMD, INTERSECTS,
										"*,param.{emd_cd,emd_kor_nm as emd_nm}")
								.groupBy("emd_cd")
									.taggedKeyColumns(geomCol + ",emd_nm")
									.aggregate(aggrFuncs)
								.project(String.format("%s,*-{%s}", geomCol, geomCol))
								.storeLayer(RESULT, geomCol, srid)
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("flow_pop_on_emd", program);
		
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
