# -*- coding: utf-8 -*-

"""데이터베이스 모듈
:filename:          - database.py
:modified:          - 2017.08.17
:note:              - 이 모듈에서는 데이터베이스 클래스를 정의함
					- 데이터베이스 업무에 필요한 쿼리 클래스도 정의함
					- ORACLE, MYSQL 관계없이 하나의 클래스에서 관리
					- SQLALCHEMY의 엔진 활용해서 데이터베이스 연결

"""

'''모듈 불러오기'''
import pandas as pd                     			#PANDAS
from sqlalchemy import create_engine, types			#ALCHEMY for engine


'''클래스'''
class Query(object):
	"""쿼리 클래스 정의	"""

	'''클래스 생성자 및 소멸자'''
	def __init__(self, sql:str, col:tuple=None, dtype:dict=None):
		"""클래스 생성자
		- 입력받은 쿼리문, 인덱스, 바인드변수를 맴버변수로 설정

		:param sql: 쿼리문
		:param col: pandas.DataFrame 에서 사용하게 될 컬럼 이름
		:param dtype: 나중에 테이블로 다시 INSERT할때 사용하게 될 데이터타입

		:var _sql: = param sql
		:var _col: = param col
		:var _dtype: = param dtype
		:var _primary_key: pandas.DataFrame 에서 인덱스로 사용하게 될 컬럼 이름

		:Example:

			>>> sql = '.....'
			>>> col = ('TRADE_DATE','PX_LAST')
			>>> ql_px_last = Query(sql, col)
			>>> bind = {'tdate':'20170101','ticker':'HSCEI'}
			>>> ql_px_last.set_bind(bind)

		"""
		#멤버변수 설정
		self._sql = sql
		self._col = col
		self._dtype = dtype
		self._primary_key = None

		#컬럼이 하나 이상 존재하면, 첫번째 컬럼을 인덱스로 설정
		if col is not None:
			if len(col) > 1:
				self._primary_key = col[0].lower()


	'''접근자'''
	@property
	def sql(self):
		return self._sql

	@property
	def col(self):
		return self._col

	@property
	def dtype(self):
		return self._dtype

	@property
	def primary_key(self):
		return self._primary_key

	@sql.setter
	def sql(self, value):
		self._sql = value

	@col.setter
	def col(self, value):
		self._col = value

	@dtype.setter
	def dtype(self, value):
		self._dtype = value

	@primary_key.setter
	def primary_key(self, value):
		self._primary_key = value

	'''클래스 함수'''
	def set_bind(self, bind:dict):
		"""바인드변수 설정
		- 외부에서 바인드변수 입력받아서 클래스 내부의 쿼리문에 바인드

		:param bind: 외부에서 입력받는 바인드 변수

		"""
		try:
			self._sql = self._sql.format(**bind)
		except:
			for key, value in bind.items():
				self._sql = self._sql.replace('{'+key+'}', value)
				
		
class Database(object):
	"""데이터베이스 클래스 정의"""

	'''클래스 상수'''
	#접속가능한 데이터베이스들의 스펙
	_SPEC_SPT_RMS01 = {'name':'RMS01',
					   'user':'spt','password':'spt','type':'ORACLE',
					   'host':'RMS01','port':None,'database':'RMS01'}

	_SPEC_SPS_SPTWD = {'name':'SPTWD',
					   'user':'sps','password':'sps','type':'ORACLE',
					   'host':'SPTWD','port':None,'database':'SPTWD'}

	_SPEC_JHJO_TESTDB = {'name':'TESTDB',
						 'user':'python','password':'python','type':'MYSQL',
						 'host':'128.20.8.188','port':'3306','database':'eqd_jh_jo'}

	#접속가능한 데이터베이스들의 스펙 리스트
	_SPEC = [_SPEC_SPT_RMS01,
			 _SPEC_SPS_SPTWD,
			 _SPEC_JHJO_TESTDB]

	'''클래스 생성자 및 소멸자'''
	def __init__(self, user_id:str, db_name:str):
		"""클래스 생성자
		- 입력받은 사용자이름, 데이버이스이름에 따라서 데이터베이스 스펙변수 및 접속변수 설정

		:param user_id: 사용자이름
		:param db_name: 데이터베이스이름
		:var _spec: 클래스 상수중에서 입력변수에 해당하는 상수를 찾아서 스펙변수 설정
		:var _conn_str: 해당하는 접속변수 설정
		:var _engine: 데이터베이스 연결에 사용되는 엔진 커넥션
		:Example:

			>>> database1 = Database('spt','RMS01')    		 	#RMS01에 접속 (spt/spt)
			>>> database2 = Database('sps','SPTWD')     		#SPT.WORLD에 접속 (sps/sps)
			>>> database3 = Database('python','TESTDB')  		#TESTDB에 접속 (python/python)

		"""
		#데이터베이스 스펙변수 및 접속변수 설정
		self._spec = self._get_spec(user_id, db_name)
		self._conn_str = self._get_conn_str(self._spec)
		self._engine = None

		#데이터베이스에 연결, self._engine변수 설정됨
		self._connect()

	def __del__(self):
		"""클래스 소멸자
		- 스펙변수, 접속변수 초기화

		"""
		#멤버변수 초기화
		self._spec = None
		self._conn_str = None
		self._engine.dispose()
		self._engine = None

	'''접근자'''
	@property
	def spec(self):
		return self._spec

	@property
	def conn_str(self):
		return self._conn_str

	@property
	def engine(self):
		return self._engine

	@spec.setter
	def spec(self, value):
		raise Exception("ERROR _ private member")

	@conn_str.setter
	def conn_str(self, value):
		raise Exception("ERROR _ private member")

	@engine.setter
	def engine(self, value):
		raise Exception("ERROR _ private member")


	'''클래스 함수'''
	def _get_spec(self, user_id:str, db_name:str) -> dict:
		"""데이터베이스 스펙변수 조회
		- 입력받은 사용자이름, 데이터베이스이름에 따라서 데이터베이스 스펙변수 반환

		:param user_id: 사용자이름
		:param db_name: 데이터베이스이름
		:raises Exception: 해당하는 데이터베이스의 스펙변수가 없는 경우 예외발생
		:return: 해당되는 데이터베이스의 스펙변수 반환

		"""
		for self._spec in self._SPEC:
			if self._spec['user'] == user_id and self._spec['name'] == db_name:
				return self._spec

		raise Exception('ERROR _ invalid argument: user_id or db_name')

	def _get_conn_str(self, spec:dict) -> str:
		"""데이터베이스 접속변수 조회
		- 입력받은 스펙변수에 따라 대응되는 접속변수 반환

		:param spec: 스펙변수
		:raises Exception: 해당 스펙변수가 ORACLE, MYSQL 이외의 데이터베이스 인 경우 예외발생
		:return: 해당되는 접속변수 반환

		"""
		conn_str = spec['user'] + ':' + spec['password'] + '@' + spec['host']

		if spec['type'] == 'ORACLE':
			conn_str = 'oracle://' + conn_str
		elif spec['type'] == 'MYSQL':
			conn_str = 'mysql+mysqlconnector://' + conn_str + ":" + spec['port'] + "/" + spec['database']
		else:
			raise Exception('ERROR _ invalid argument: dbTYPE')

		return conn_str

	def _connect(self):
		"""데이터베이스에 접속
		- 생성자에서 설정된 스펙변수와 접속변수에 따라 데이터베이스에 접속하고 접속변수 반환

		:raises Exception: 해당 스펙변수가 ORACLE, MYSQL 이외의 데이터베이스 인 경우 예외발생
		:return: 없음

		"""
		if self.is_connected() == False:
			self._engine = create_engine(self._conn_str)

	def is_connected(self) -> bool:
		"""데이터베이스 연결 여부 확인

		:return: 연결여부에 따라 True/False 반환
		"""
		try:
			if self._engine is None:
				return False
			else:
				return True
		except:
			self._engine = None
			return False

	def select(self, query:Query, show_sql:bool=False):
		"""쿼리 수행하여 결과를 그대로 데이터프레임 변수로 출력

		:param query: 쿼리 클래스 객체
		:param show_sql: 쿼리 문자열 출력여부 설정
		:param bind: 바인딩 변수
		:return: 쿼리 수행 결과를 pd.DataFrame 형식으로 반환

		"""
		if show_sql == True:
			print(query.sql)

		result = pd.read_sql_query(sql=query.sql,
								   con=self._engine,
								   index_col=query.primary_key)

		#컬럼명, 인덱스면 대문자로 변경
		result.columns = map(str.upper, result.columns)
		if query.primary_key is not None:
			result.index.name = result.index.name.upper()

		#결과반환
		return result

	def insert(self, name_table:str, data_frame:pd.DataFrame, dtype:dict, if_exists:str):
		"""데이터프레임 변수를 그대로 테이블로 인서트

		:param name_table: 인서트를 원하는 테이블 명
		:param data_frame: 데이터프레임 변수
		:param dtype: 테이블의 컬럼 데이터 타입
		:return: 없음

		"""
		data_frame.to_sql(name=name_table,
						  con=self._engine,
						  if_exists=if_exists,
						  dtype=dtype)

	def execute(self, query:Query):
		"""단순 쿼리문 직접 수행

		:param sql: 쿼리문
		:return: 없음

		"""
		self._engine.execute(query.sql)


'''메인함수'''
def main():
	#데이터베이스 설정
	dbRMS01 = Database('spt','RMS01')

	#쿼리 설정
	sql = \
		'''	SELECT TO_DATE(TRADE_DATE,'yyyymmdd') TRADE_DATE, PX_LAST 
			FROM spt.BL_DATA 
			WHERE TRADE_DATE='{trade_date}' AND TICKER='{ticker}' 
		'''
	col = ('trade_date','px_last')
	dtype = {'TRADE_DATE':types.DateTime(), 'PX_LAST':types.Float()}
	bind = {'trade_date': '20170531', 'ticker': 'HSCEI'}
	qrTEST = Query(sql, col, dtype)
	qrTEST.set_bind(bind)
	
	#결과출력
	dfTEST = dbRMS01.select(qrTEST)
	print(dfTEST)


	sql = \
		'''
		SELECT DISTINCT BASIC_ASSET, STND_DATE FROM INFO_KSD_UNDERLYING WHERE ISIN_NO='KR6653357444' AND STND_DATE='20140428' AND SEQ=1
		'''
	col = ('BASIC_ASSET','STND_DATE')
	dtype = {'BASIC_ASSET':types.NVARCHAR(length=30), 'STND_DATE':types.DateTime()}
	qrTEST = Query(sql, col, dtype)
	dfTEST = dbRMS01.select(qrTEST)
	print(dfTEST)


'''스크립트 파일 직접 수행되는 경우에만 실행'''
if __name__ == '__main__':
	main()


'''쿼리 인스턴스 임포트'''
#기본상품정보조회
	#컬럼명
_prod_info_basic_col = \
	('ISIN_NO', 'STD_DATE', 'CUST_NO', 'FIRST_AMT', 'REMAIN_AMT', 'EFF_DATE', 'MAT_DATE')
	#컬럼 데이터타입
_prod_info_basic_datatype = {'ISIN_NO':types.NVARCHAR(length=50),
							 'STD_DATE':types.DateTime(),
							 'CUST_NO':types.NVARCHAR(length=10),
							 'FIRST_AMT':types.BigInteger(),
							 'REMAIN_AMT':types.BigInteger(),
							 'EFF_DATE':types.DateTime(),
							 'MAT_DATE':types.DateTime()}
	#쿼리문
_prod_info_basic_sql = \
	(
		"select tblLATEST.ISIN_NO, "  																		# ISIN번호
		"		to_date(greatest(tblLATEST.STND_DATE, nvl(tblREFUND.STND_DATE,0)),'yyyy-mm-dd') STD_DATE, "	# 처리일자
		"		tblLATEST.CUST_NO, "  																		# 발행사번호
		"		tblLATEST.FIRST_AMT, "  																	# 최초발행금액
		"		greatest(tblLATEST.FIRST_AMT-nvl(tblREFUND.REFUND,0),0) REMAIN_AMT, "  						# 현재발행잔액
		"		tblLATEST.EFF_DATE, "  																		# 설정일자
		"		tblLATEST.MAT_DATE "  																		# 만기일자
		"from "
		"( "
		"	select 	ISIN_NO, "
		"			max(STND_DATE) STND_DATE, "
		"			to_number(SUBSTR(MAX(CUST_NO),-2),'99') CUST_NO, "
		"			max(FIRST_ISSUE_AMT) FIRST_AMT, "
		"			to_date(max(STD_ESTIM_T_DATE),'yyyy-mm-dd') EFF_DATE, "
		"			to_date(max(XPIR_ESTIM_T_DATE),'yyyy-mm-dd') MAT_DATE "
		"	from 	INFO_KSD_ISSUE_LIST "
		"	where 	STND_DATE<replace('{eval_date}','-') and "
		"			SECURITY_TYPE=41 and "  																# ELS에 대해서만 조회
		"			STD_ESTIM_T_DATE>replace('{fr_eff_date}','-') and " 		 							# 설정일자 조건
		"			STD_ESTIM_T_DATE<replace('{to_eff_date}','-') and "  									# 설정일자 조건
		"			XPIR_ESTIM_T_DATE>replace('{fr_exp_date}','-') and "  									# 만기일자 조건
		"			XPIR_ESTIM_T_DATE<replace('{to_exp_date}','-') and "  									# 만기일자 조건
		"			PRSV_RATE<={prsv_rate} "  																# 원금보장비율 조건
		"	group by ISIN_NO "
		"	having min(ISSUE_AMT)>{req_amt_min} " 	 														# 현재발행잔액 조건
		") tblLATEST "  																					# tblLATEST 최근발행정보
		"left outer join "
		"( "
		"	select 	ISIN_NO, "
		"			max(STND_DATE) STND_DATE, "
		"			sum(REFUND_QTY) REFUND "
		"	from 	INFO_KSD_REFUND "
		"	where 	STND_DATE<replace('{eval_date}','-') and "
		"			SEC_TYPE IN ('1','3') "
		"	group by ISIN_NO "
		") tblREFUND "  																					# tblREFUND 환매정보
		"on tblREFUND.ISIN_NO=tblLATEST.ISIN_NO "
		"where 	greatest(tblLATEST.FIRST_AMT-nvl(tblREFUND.REFUND,0),0)>{req_amt_min} and "					# 현재발행잔액 조건
		" 		greatest(tblLATEST.FIRST_AMT-nvl(tblREFUND.REFUND,0),0)<{req_amt_max} and "					# 현재발행잔액 조건
		"		greatest(tblLATEST.STND_DATE, nvl(tblREFUND.STND_DATE,0))>replace('{fr_std_date}','-') "
		"order by tblLATEST.EFF_DATE asc "
	)
	#쿼리객체 생성
query_info_basic = Query(_prod_info_basic_sql,
						 _prod_info_basic_col,
						 _prod_info_basic_datatype)

#기초자산정보조회
	#컬럼명
_asset_info_basic_col = \
	('ISIN_NO','STD_DATE','NAME_AST1','NAME_AST2','NAME_AST3','NAME_AST4','NAME_AST5')
	#컬럼 데이터타입
_asset_info_basic_dtype = \
	{'ISIN_NO':types.NVARCHAR(length=50),
	 'STD_DATE':types.DateTime(),
	 'NAME_AST1':types.NVARCHAR(length=20),
	 'NAME_AST2':types.NVARCHAR(length=20),
	 'NAME_AST3':types.NVARCHAR(length=20),
	 'NAME_AST4':types.NVARCHAR(length=20),
	 'NAME_AST5':types.NVARCHAR(length=20)}
_asset_list_decode = \
	"'KP2','1','NKY','2','HSC','3','SXE','4','SPX','5','HSI','6','7'"
_asset_list_in = \
	"('KP2','NKY','HSC','SXE','SPX','HSI')"
_asset_regex_name = \
	''' '^.*(KOSPI2|KP2|200).*$','KP2'),
		'^.*(NKY|NIKKEI).*$','NKY'),
		'^.*(HSCEI|HANGSENGCHINAENT).*$','HSC'),
		'^.*(EUROSTOXX|SX5E).*$','SXE'),
		'^.*(SP500).*$','SPX'),
		'^.*(HSI|HANGSENG).*$','HSI' 
	'''
	#쿼리문
_asset_info_basic_sql = \
	(
		"select	ISIN_NO, "
		"		to_date(min(STD_DATE),'yyyy-mm-dd') STD_DATE,"
		"		max(NAME_AST1) NAME_AST1, "
		"		max(NAME_AST2) NAME_AST2, "
		"		max(NAME_AST3) NAME_AST3, "
		"		max(NAME_AST4) NAME_AST4, "
		"		max(NAME_AST5) NAME_AST5 "
		"from "
		"( "
		"	select 	tmpNOR.ISIN_NO, "
		"			tmpNOR.STND_DATE STD_DATE, "
		"			SEQ, "
		"			lead(BASIC_ASSET,0) over (partition by tmpNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST1, " 
		"			lead(BASIC_ASSET,1) over (partition by tmpNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST2, "
		"			lead(BASIC_ASSET,2) over (partition by tmpNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST3, "
		"			lead(BASIC_ASSET,3) over (partition by tmpNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST4, "
		"			lead(BASIC_ASSET,4) over (partition by tmpNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ ASC) NAME_AST5 " 
		"	from "
		"	( "	
		"		select 	ISIN_NO, "
		"				SEQ, "
		"				STND_DATE," 
		"				regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
		"				regexp_replace(regexp_replace(upper(BASIC_ASSET),'( ){1,}'),'[^A-Za-z0-9]','')," + _asset_regex_name + ") BASIC_ASSET "
		"		from 	INFO_KSD_UNDERLYING " 
		"		where 	INDEX_TYPE='1' and "
		"				STND_DATE>=replace('{last_date}','-') and "
		"				STND_DATE<replace('{eval_date}','-') "
		"	) tmpNOR, " 
		"	( "
		"		select 	ISIN_NO, "
		"				min(STND_DATE) STND_DATE " 
		"		from 	INFO_KSD_UNDERLYING "
		"		where INDEX_TYPE='1' "
		"		group by ISIN_NO "
		"	) tmpGRP "
		"	where 	tmpNOR.ISIN_NO=tmpGRP.ISIN_NO and "
	   	"			tmpNOR.STND_DATE=tmpGRP.STND_DATE and "
		"			length(tmpNOR.BASIC_ASSET)=3 and "
		"			tmpGRP.STND_DATE>=replace('{last_date}','-') and "
		"			tmpGRP.STND_DATE<replace('{eval_date}','-') "
		") "
		"where 	SEQ=1 "
		"group by ISIN_NO "
	)
	#쿼리객체 생성
query_info_underlying = Query(_asset_info_basic_sql,
							  _asset_info_basic_col,
							  _asset_info_basic_dtype)

#작업일자정보삭제
	#쿼리문
_oper_hist_delete_sql = \
	(
		"delete from "
		"els_market_oper_hist "
		"where dateOPER='{date_oper}' "
	)
	#쿼리객체생성
query_oper_hist_delete = Query(_oper_hist_delete_sql)

#작업일자 정보입력
	#쿼리문
_oper_hist_insert_sql = \
	(
		"insert into "
		"els_market_oper_hist "
		"(dateOPER) values ('{date_oper}') "
	)
	#쿼리객체생성
query_oper_hist_insert = Query(_oper_hist_insert_sql)

#최근작업일자 정보조회
	#쿼리문
_oper_hist_select_sql = \
	(
		"select max(dateOPER) STD_DATE "
		"from eqd_jh_jo.els_market_oper_hist "
	)
	#컬럼명
_oper_hist_select_col = ('STD_DATE',)
	#컬럼 데이터타입
_oper_hist_select_dtype = {'STD_DATE':types.DateTime()}
	#쿼리객체생성
query_oper_hist_select = Query(_oper_hist_select_sql,
							   _oper_hist_select_col,
							   _oper_hist_select_dtype)

#기본정보테이블 제약조건 설정
	#쿼리문
_alter_table_basic_info_sql = \
	(
		"alter table {name_table} " 
		"change column ISIN_NO ISIN_NO varchar(50) not null, "
		"change column FIRST_AMT FIRST_AMT bigint(20) unsigned null default null, "
		"change column REMAIN_AMT REMAIN_AMT bigint(20) unsigned null default null, "
		"add primary key (ISIN_NO) "
	)
	#쿼리객체생성
query_alter_table_basic_info = Query(_alter_table_basic_info_sql)

#기초자산정보테이블 제약조건 설정
	#쿼리문
_alter_table_asset_info_sql = \
	(
		"alter table els_market_underlying_info " 
		"change column ISIN_NO ISIN_NO varchar(50) not null, "
		"add primary key (ISIN_NO) "
	)
	#쿼리객체생성
query_alter_table_asset_info = Query(_alter_table_asset_info_sql)
