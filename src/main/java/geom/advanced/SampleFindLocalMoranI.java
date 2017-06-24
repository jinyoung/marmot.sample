package geom.advanced;

import java.util.Map;
import java.util.UUID;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Maps;

import common.SampleUtils;
import marmot.Plan;
import marmot.Record;
import marmot.RemotePlan;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.LISAWeight;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleFindLocalMoranI {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "시연/대전공장";
	private static final String VALUE_COLUMN = "FCTR_MEAS";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		String tempPath = "tmp/" + UUID.randomUUID();
		Plan plan0 = RemotePlan.builder("find_statistics")
								.load(INPUT)
								.aggregate(AggregateFunction.COUNT(),
											AggregateFunction.AVG(VALUE_COLUMN),
											AggregateFunction.STDDEV(VALUE_COLUMN))
								.storeMarmotFile(tempPath)
								.build();
		marmot.execute(plan0);
		
		Map<String,Object> params = Maps.newHashMap();

		Record result = marmot.readMarmotFile(tempPath).stream().findAny().get();
		params.putAll(result.toMap());
		marmot.deleteFile(tempPath);
		
		double avg = (Double)params.get("avg");
		Plan plan1 = RemotePlan.builder("find_statistics2")
								.load(INPUT)
								.update("diff:double", "diff = fctr_meas -" + avg)
								.update("diff2:double,diff4:double",
											"diff2 = diff * diff; diff4=diff2*diff2")
								.aggregate(AggregateFunction.SUM("diff").as("diffSum"),
											AggregateFunction.SUM("diff2").as("diff2Sum"),
											AggregateFunction.SUM("diff4").as("diff4Sum"))
								.storeMarmotFile(tempPath)
								.build();
		marmot.execute(plan1);
		Record result2 = marmot.readMarmotFile(tempPath).stream().findAny().get();
		params.putAll(result2.toMap());
		marmot.deleteFile(tempPath);
		
		Plan plan = RemotePlan.builder("local_spatial_auto_correlation")
								.loadLocalMoranI(INPUT, "UID", "FCTR_MEAS", 1000,
												LISAWeight.FIXED_DISTANCE_BAND)
								.project("UID,moran_i,moran_zscore,moran_pvalue")
								.sort("UID")
								.storeMarmotFile(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 5);
	}
}
