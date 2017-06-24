package bizarea;

import static marmot.optor.AggregateFunction.SUM;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.optor.JoinOptions;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1CardSales {
	private static final String BIZ_GRID = "tmp/bizarea/grid100";
	private static final String CARD_SALES = "국민/카드매출/시간대/2015";
	private static final String RESULT = "tmp/bizarea/grid100_sales";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		StopWatch watch = StopWatch.start();

		String sumExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("sale_amt_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		sumExpr = "daily_sales="+sumExpr;
		
		DataSet info = marmot.getDataSet(BIZ_GRID);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		Plan plan = marmot.planBuilder("card_sales")
								// 전국 카드매출액 파일을 읽는다.
								.load(CARD_SALES)
								// 시간대 단위의 매출액은 모두 합쳐 하루 매출액을 계산한다. 
								.update("daily_sales:double", sumExpr)
								.project("std_ym,block_cd,daily_sales")
								// BIZ_GRID와 소지역 코드를 이용하여 조인하여, 대도시 상업지역과 겹치는
								// 매출액 구역을 뽑는다.
								.join("block_cd", BIZ_GRID, "block_cd",
										"param.*,std_ym,daily_sales",
										new JoinOptions().workerCount(64))
								// 한 그리드 셀에 여러 소지역 매출액 정보가 존재하면,
								// 해당 매출액은 모두 더한다. 
								.groupBy("std_ym,cell_id")
									.taggedKeyColumns(geomCol + ",sgg_cd")
									.aggregate(SUM("daily_sales").as("daily_sales"))
								.project(String.format("%s,*-{%s}", geomCol, geomCol))
								.store(RESULT)
								.build();
		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, geomCol, srid, plan);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
