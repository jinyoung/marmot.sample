package carloc;

import marmot.Program;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;
import marmot.support.DefaultRecord;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExtractTravel {
	private static final String INPUT = "data/carloc_hst";
	private static final int STATUS = 0;
	private static final String RESULT = "taxi/travel_heap";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		RecordSchema csvSchema = RecordSchema.builder()
											.addColumn("car_no", DataType.STRING)
											.addColumn("ts", DataType.LONG)
											.addColumn("month", DataType.BYTE)
											.addColumn("region", DataType.INT)
											.addColumn("bessel_x", DataType.DOUBLE)
											.addColumn("bessel_y", DataType.DOUBLE)
											.addColumn("status", DataType.BYTE)
											.addColumn("company", DataType.STRING)
											.addColumn("driver_id", DataType.INT)
											.addColumn("wgs_x", DataType.DOUBLE)
											.addColumn("wgs_y", DataType.DOUBLE)
											.build();
		
		String schemaDef = "the_geom:point,hour:int";
		String rtrans = "the_geom = ST_Point(wgs_x, wgs_y); hour = (ts/10000) % 100;";
		
		Program program = Program.builder()
								.loadCsvFiles(INPUT, csvSchema)
								.filter("status == " + STATUS)
								.transform(schemaDef, rtrans, false)
								.project("the_geom,car_no,driver_id,hour")
								.transformCRS("the_geom", "EPSG:4326", "EPSG:5186")
								.reduceGeometryPrecision("the_geom", 2)
								.storeLayer(RESULT, "the_geom", "EPSG:5186")
								.build();

		catalog.deleteLayer(RESULT);
		marmot.execute("extract_travel", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		RecordSet rset = marmot.readLayer(RESULT);
		Record record = DefaultRecord.of(rset.getRecordSchema());
		int count = 0;
		while ( ++count <= 10 && rset.next(record) ) {
			System.out.println(record);
		}
	}
}
