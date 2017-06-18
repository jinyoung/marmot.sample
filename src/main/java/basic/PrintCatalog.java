package basic;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import marmot.Column;
import marmot.DataSet;
import marmot.RecordSchema;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

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
		
		// 카다로그에 등록된 모든 레이어 등록정보를 출력한다.
		List<DataSet> dsList = marmot.getDataSetAll();
		for ( DataSet ds: dsList ) {
			System.out.println("DataSet name: " + ds.getId());
			System.out.println("\tDir: " + ds.getDirName());
			System.out.println("\tType: " + ds.getType());
			System.out.println("\tSRID: " + ds.getSRID());
			System.out.println("\t# of records: " + ds.getRecordCount());
			System.out.println("\tGeometry columns: " + ds.getGeometryColumn());

			System.out.println("\tColumns: ");
			RecordSchema schema = ds.getRecordSchema();
			for ( Column col: schema.getColumnAll() ) {
				System.out.printf("\t\t%02d:%s, %s%n", col.getOrdinal(), col.getName(),
												col.getType().getName());
			}
		}
		
		// 특정 이름의 레이어의 등록정보를 접근
		DataSet info = marmot.getDataSet("교통/지하철/역사");
		
		// 카다로그에 등록된 모든 폴더를 접근한다.
		List<String> folders = marmot.getDirAll();
		for ( String folder: folders ) {
			System.out.println("Dir: " + folder);
			
			// 폴더에 저장된 레이어의 등록정보를 접근한다.
			List<DataSet> infos = marmot.getDataSetAllInDir(folder, false);
			
			// 폴더에 등록된 하위 폴더를 접근한다.
			List<String> subDirs = marmot.getSubDirAll(folder, false);
		}
		
		// 특정 이름의 레이어를 삭제시킨다.
		marmot.deleteDataSet("tmp/result");
		
		// 특정 폴더에 등록된 모든 레이어들을 삭제시킨다. (모든 하위 폴더의 레이어들도 삭제 대상임)
		marmot.deleteDir("tmp");
	}
}
