package oldbldr;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.optor.AggregateFunction;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1 {
	private static final String FLOW_POP = "로그/유동인구/2015/시간대별";
	private static final String BLOCK_CENTERS = "tmp/centers";
	private static final String EMD = "구역/읍면동";
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
		
		DataSet info = marmot.getDataSet(EMD);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		Program program = Program.builder("flow_pop_on_emd")
								.load(FLOW_POP)
								.join("block_cd", BLOCK_CENTERS, "block_cd",
										"param.*-{block_cd},*", opt->opt.workerCount(32))
								.spatialJoin("the_geom", EMD, INTERSECTS,
										"*,param.{emd_cd,emd_kor_nm as emd_nm}")
								.groupBy("emd_cd")
									.taggedKeyColumns(geomCol + ",emd_nm")
									.aggregate(aggrFuncs)
								.project(String.format("%s,*-{%s}", geomCol, geomCol))
								.storeMarmotFile(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute(program);
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
