package bizarea;

import static marmot.optor.geo.AggregateFunction.SUM;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
public class Step1CardSales {
	private static final String BIZ_GRID = "tmp/biz/grid100";
	private static final String BLOCK_CENTERS = "geo_vision/block_centers/heap";
	private static final String CARD_SALES = "data/geo_vision/card_sales/2015/time";
//	private static final String CARD_SALES = "data/geo_vision/card_sales/2015/sample";
	private static final String RESULT = "tmp/biz/sales_grid100";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		String sumExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("sale_amt_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		sumExpr = "daily_sales="+sumExpr;
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(BIZ_GRID);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		Program program = Program.builder()
								.loadCsvFiles(CARD_SALES)
								// 시간대 단위의 매출액은 모두 합쳐 하루 매출액을 계산한다. 
								.expand("daily_sales:double", sumExpr)
								.project("std_ym,block_cd,daily_sales")
								// BIZ_GRID와 소지역 코드를 이용하여 조인하여, 대도시 상업지역과 겹치는
								// 매출액 구역을 뽑는다. 
								.join(setter -> setter
									.withLayer(BIZ_GRID)
									.onColumns("block_cd", "block_cd")
									.output("param.*,std_ym,daily_sales")
									.workerCount(64)
								)
								// 한 그리드 셀에 여러 소지역 매출액 정보가 존재하면,
								// 해당 매출액은 모두 더한다. 
								.groupBy("std_ym,cell_id")
									.taggedKeyColumns(geomCol + ",sgg_cd")
									.aggregate(SUM("daily_sales").as("daily_sales"))
								.project(String.format("%s,*-{%s}", geomCol, geomCol))
								.storeLayer(RESULT, geomCol, srid)
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("card_sales", program);
		
		SampleUtils.printLayerPrefix(marmot, BIZ_GRID, 10);
	}
}
