package vworld;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.google.common.base.Splitter;

import marmot.type.MapTile;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LogRecord2D {
	private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("dd/MMM/yyyy:kk:mm:ss Z");
	
	String m_reqHost;
	LocalDateTime m_ts;
	String m_mapType;
	MapTile m_tile;
	String m_layerName;
	String m_apiKey;
	int m_reponseCode;
	
	public static LogRecord2D parse(String logStr) {
		LogRecord2D rec = new LogRecord2D();
		Locator locator = new Locator(logStr);
		
		if ( !locator.locate(' ') ) {
			throw new IllegalArgumentException("reqHost, log=" + logStr);
		}
		rec.m_reqHost = locator.getString(0, -1);
		
		if ( !locator.locate('[', ']') ) {
			throw new IllegalArgumentException("ts, log=" + logStr);
		}
		String timeStr = locator.getString(1, -1);
		rec.m_ts = LocalDateTime.parse(timeStr, FORMAT);
		
		locator.findNonWhitespace(1);
		if ( !locator.locate(' ', ' ') ) {
			throw new IllegalArgumentException("query, log=" + logStr);
		}
		String query = locator.getString(1, -1);
		parseQuery(query, rec);

		if ( !locator.locate(' ', ' ') ) {
			throw new IllegalArgumentException("query, log=" + logStr);
		}
		rec.m_reponseCode = Integer.parseInt(locator.getString(1, -1));
		
		return rec;
	}
	
	@Override
	public String toString() {
		return m_reqHost;
	}
	
	static LogRecord2D parseRequestLayer(String logStr) {
		LogRecord2D log = new LogRecord2D();
		Locator locator = new Locator(logStr);
		
		if ( !locator.locate(' ') ) {
			throw new IllegalArgumentException("reqHost, log=" + logStr);
		}
		log.m_reqHost = locator.getString(0, -1);
		
		if ( !locator.locate('[', ']') ) {
			throw new IllegalArgumentException("ts, log=" + logStr);
		}
		String timeStr = locator.getString(1, -1);
		log.m_ts = LocalDateTime.parse(timeStr, FORMAT);
	
		if ( !locator.locate('/', '?') ) {
			throw new IllegalArgumentException("log=" + logStr);
		}
		String[] path = locator.getString(-1, -1).split("/");
		log.m_mapType = path[path.length-1];
		
		if ( !locator.locate(' ') ) {
			throw new IllegalArgumentException("log=" + logStr);
		}
		String query = locator.getString(0, -1);
		if ( query.startsWith("&") ) {
			query = query.substring(1);
		}
		Map<String,String> params = Splitter.on('&').trimResults().withKeyValueSeparator("=").split(query);
		
		log.m_layerName = params.get("Layer");
		String levelStr = params.get("Level");
		String xStr = params.get("IDX");
		String yStr = params.get("IDY");
		if ( levelStr != null && xStr != null && yStr != null ) {
			log.m_tile = new MapTile(Integer.parseInt(levelStr), Integer.parseInt(xStr),
									Integer.parseInt(yStr));
		}
		log.m_apiKey = params.get("APIKey");

		if ( !locator.locate(' ', ' ') ) {
			throw new IllegalArgumentException("query, log=" + logStr);
		}
		log.m_reponseCode = Integer.parseInt(locator.getString(1, -1));
		
		return log;
	}
	
	private static void parseQuery(String query, LogRecord2D log) {
		// 맨뒤에 붙는 확장자 이름을 제거한다.
		int idx = query.indexOf('.');
		if ( idx >= 0 ) {
			query = query.substring(0, idx);
		}
		
		String[] parts = query.split("/");
		log.m_mapType = parts[2];
		log.m_tile = null;
		
		if ( parts.length == 7 ) {
			log.m_tile = new MapTile(Integer.parseInt(parts[4]), Integer.parseInt(parts[5]),
									Integer.parseInt(parts[6]));
		}
		else if ( parts.length == 6 ) {
			int zoom = ( log.m_mapType.equals("Satellite") ) ? 7 : 6;
			log.m_tile = new MapTile(zoom, Integer.parseInt(parts[4]), Integer.parseInt(parts[5]));
		}
		else if ( parts.length == 8 ) {
			if ( parts[4].trim().equals("") || parts[4].trim().equals("?") ) {
				log.m_tile = new MapTile(Integer.parseInt(parts[5]), Integer.parseInt(parts[6]),
										Integer.parseInt(parts[7]));
			}
		}
		
		if ( log.m_tile == null) {
			throw new IllegalArgumentException("invalid map access log: str=" + query);
		}
	}
	
	static class Locator {
		private final String m_str;
		int m_begin, m_end;
		
		Locator(String str) {
			m_str = str;
			m_begin = 0;
			m_end = 0;
		}
		
		String getString() {
			return m_str.substring(m_begin, m_end);
		}
		
		String getString(int beginMargin, int endMargin) {
			return m_str.substring(m_begin+beginMargin, m_end+endMargin);
		}
		
		boolean findNonWhitespace(int startMargin) {
			m_end += startMargin;
			
			while ( true ) {
				if ( m_end >= m_str.length() ) {
					return false;
				}
				
				if ( Character.isWhitespace(m_str.charAt(m_end)) ) {
					return true;
				}
				++m_end;
			}
		}
		
		boolean locate(int stopToken) {
			return locate(-1, stopToken);
		}
		
		boolean locate(int startToken, int stopToken) {
			return locate(m_end, startToken, stopToken);
		}
		
		boolean locate(int begin, int startToken, int stopToken) {
			m_begin = (startToken >= 0) ? m_str.indexOf(startToken, begin) : begin;
			if ( m_begin >= 0 ) {
				m_end = m_str.indexOf(stopToken, m_begin+1);
				if ( m_end >= 0 ) {
					++m_end;
					return true;
				}
				else {
					return false;
				}
			}
			
			return false;
		}
		
		@Override
		public String toString() {
			int begin = Math.max(0, m_begin);

			return String.format("%d:%d:\"%s\"", m_begin, m_end, m_str.substring(begin, m_end));
		}
	}
}
