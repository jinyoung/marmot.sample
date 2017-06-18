package bizarea;

import static marmot.optor.AggregateFunction.AVG;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1FlowPop {
	private static final String BIZ_GRID = "tmp/bizarea/grid100";
	private static final String FLOW_POP = "로그/유동인구/2015/시간대별";
	private static final String RESULT = "tmp/bizarea/grid100_pop";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		StopWatch watch = StopWatch.start();

		String avgExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		avgExpr = String.format("flow_pop=(%s)/24", avgExpr);
		
		DataSet info = marmot.getDataSet(BIZ_GRID);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		Program program = Program.builder("flow_pop")
								.load(FLOW_POP)
								// 시간대 단위의 유동인구는 모두 합쳐 하루 매출액을 계산한다. 
								.update("flow_pop:double", avgExpr)
								.project("std_ym,block_cd,flow_pop")
								// BIZ_GRID와 소지역 코드를 이용하여 조인하여, 대도시 상업지역과 겹치는
								// 유동인구 구역을 뽑는다. 
								.join("block_cd", BIZ_GRID, "block_cd",
										"param.*,std_ym,flow_pop", opt->opt.workerCount(32))
								// 한 그리드 셀에 여러 소지역 유동인구 정보가 존재하면,
								// 해당 유동인구들의 평균을 구한다.
								.groupBy("std_ym,cell_id")
									.taggedKeyColumns(geomCol + ",sgg_cd")
									.aggregate(AVG("flow_pop").as("flow_pop"))
								.project(String.format("%s,*-{%s}", geomCol, geomCol))
								.store(RESULT)
								.build();
		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, geomCol, srid, program);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
	}
}
