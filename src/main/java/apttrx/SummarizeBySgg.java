package apttrx;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SummarizeBySgg {
	private static final String APT_LOC = "tmp/아파트실매매/아파트위치";
	private static final String APT_TRX = "주택/실거래/아파트매매";
	private static final String SGG = "구역/시군구";
	private static final String RESULT = "tmp/아파트실매매/시군구별";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		StopWatch watch = StopWatch.start();

		Plan plan;
		DataSet emd = marmot.getDataSet(SGG);
		String geomCol = emd.getGeometryColumn();
		String srid = emd.getSRID();
		
		plan = marmot.planBuilder("summarize_by_station")
						.load(APT_TRX)
						.join("시군구,번지,단지명", APT_LOC, "시군구,번지,단지명", "*,param.{info}", null)
						.update("평당거래액:int",
								"평당거래액 = (int)Math.round((거래금액*3.3) / 전용면적);")
						.update("sgg_cd:string", "sgg_cd = info.getSggCode()")
						.groupBy("sgg_cd")
							.aggregate(COUNT().as("거래건수"),
										SUM("거래금액").as("총거래액"),
										AVG("평당거래액").as("평당거래액"),
										MAX("거래금액").as("최대거래액"),
										MIN("거래금액").as("최소거래액"))
						.update("평당거래액:int", "평당거래액=평당거래액")
						
						.join("sgg_cd", SGG, "sig_cd",
								String.format("*,param.{%s,sig_kor_nm}", geomCol), null)
						.sort("거래건수:D")
						
						.store(RESULT)
						.build();
		
		marmot.deleteDataSet(RESULT);		
		marmot.createDataSet(RESULT, geomCol, srid, plan);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
	}
}
