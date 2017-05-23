package basic;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import marmot.Column;
import marmot.RecordSchema;
import marmot.geo.catalog.LayerInfo;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteCatalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PrintCatalog {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		// 카다로그에 등록된 모든 레이어 등록정보를 출력한다.
		List<LayerInfo> layerInfos = catalog.getLayerInfoAll();
		for ( LayerInfo info: layerInfos ) {
			System.out.println("Layer name: " + info.getName());
			System.out.println("\tDir: " + info.getDirName());
			System.out.println("\tType: " + info.getLayerType());
			System.out.println("\tSRID: " + info.getSRID());
			System.out.println("\t# of records: " + info.getRecordCount());
			System.out.println("\tGeometry columns: " + info.getGeometryColumn());

			System.out.println("\tColumns: ");
			RecordSchema schema = info.getRecordSchema();
			for ( Column col: schema.getColumnAll() ) {
				System.out.printf("\t\t%02d:%s, %s%n", col.getOrdinal(), col.getName(),
												col.getType().getName());
			}
		}
		
		// 특정 이름의 레이어의 등록정보를 접근
		LayerInfo info = catalog.getLayerInfo("transit/subway_stations/heap");
		
		// 카다로그에 등록된 모든 폴더를 접근한다.
		List<String> folders = catalog.getDirAll();
		for ( String folder: folders ) {
			System.out.println("Dir: " + folder);
			
			// 폴더에 저장된 레이어의 등록정보를 접근한다.
			List<LayerInfo> infos = catalog.getLayerInfoAllInDir(folder, false);
			
			// 폴더에 등록된 하위 폴더를 접근한다.
			List<String> subDirs = catalog.getSubDirAll(folder, false);
		}
		
		// 특정 이름의 레이어를 삭제시킨다.
		marmot.deleteLayer("tmp/result");
		
		// 특정 폴더에 등록된 모든 레이어들을 삭제시킨다. (모든 하위 폴더의 레이어들도 삭제 대상임)
		catalog.deleteDir("tmp");
	}
}
