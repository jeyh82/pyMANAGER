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
import copy											#DEEP COPY
from sqlalchemy import create_engine, types			#ALCHEMY for engine


'''클래스'''
class Connection(object):
	"""데이터베이스 커넥션 클래스: 데이터베이스에 접속하는 것만을 목적으로 함"""

	'''클래스 상수'''
	#접속가능한 데이터베이스들의 스펙
	_SPEC_SPT_RMS01 = \
		{'name':'RMS01','type':'ORACLE', 'user':'spt','password':'spt',
		 'host':'RMS01','port':None, 'database':None}

	_SPEC_SPS_SPTWD = \
		{'name':'SPTWD','type':'ORACLE', 'user':'sps','password':'sps',
		 'host':'SPTWD','port':None, 'database':None}

	_SPEC_JHJO_TESTDB = \
		{'name':'TESTDB','type':'MYSQL', 'user':'python','password':'python',
		 'host':'128.20.8.188','port':'3306', 'database':'eqd_jh_jo'}

	#접속가능한 데이터베이스들의 스펙 리스트
	_SPEC = [_SPEC_SPT_RMS01, _SPEC_SPS_SPTWD, _SPEC_JHJO_TESTDB]

	'''생성자 및 소멸자'''
	def __init__(self, user_id:str, db_name:str):
		"""클래스 생성자: 입력받은 사용자이름, 데이버이스이름에 따라서 데이터베이스 스펙변수 및 접속변수 설정

		:param user_id: 사용자이름
		:param db_name: 데이터베이스이름
		:Example:
			>>> database1 = Connection('spt','RMS01')    		 	#RMS01에 접속 (spt/spt)
			>>> database2 = Connection('sps','SPTWD')     		#SPT.WORLD에 접속 (sps/sps)
			>>> database3 = Connection('python','TESTDB')  		#TESTDB에 접속 (python/python)

		"""
		#데이터베이스 스펙변수 및 접속변수 설정
		self._spec = self._get_spec(user_id, db_name)
		self._conn_str = self._get_conn_str(self._spec)
		self._engine = None

		#데이터베이스에 연결, self._engine변수 설정됨
		self._connect()

	def __del__(self):
		"""클래스 소멸자: 스펙변수, 접속변수 초기화"""
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
		raise Exception("ERROR @spec _ private member")

	@conn_str.setter
	def conn_str(self, value):
		raise Exception("ERROR @conn_str _ private member")

	@engine.setter
	def engine(self, value):
		raise Exception("ERROR @engine _ private member")


	'''클래스 함수'''
	def _get_spec(self, user_id:str, db_name:str) -> dict:
		"""데이터베이스 스펙변수 조회: 입력받은 사용자이름, 데이터베이스이름에 따라서 데이터베이스 스펙변수 반환

		:param user_id: 사용자이름
		:param db_name: 데이터베이스이름
		:raises Exception: 해당하는 데이터베이스의 스펙변수가 없는 경우 예외발생
		:return: 해당되는 데이터베이스의 스펙변수 반환

		"""
		for spec in self._SPEC:
			if spec['user'] == user_id and spec['name'] == db_name:
				return spec

		raise Exception('ERROR @_get_spec _ invalid argument: user_id or db_name')

	def _get_conn_str(self, spec:dict) -> str:
		"""데이터베이스 접속변수 조회: 입력받은 스펙변수에 따라 대응되는 접속변수 반환

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
			raise Exception('ERROR @_get_conn_str _ invalid argument: dbTYPE')

		return conn_str

	def _connect(self):
		"""데이터베이스에 접속: 생성자에서 설정된 스펙변수와 접속변수에 따라 데이터베이스에 접속"""
		if not self._is_connected():
			self._engine = create_engine(self._conn_str)

	def _is_connected(self) -> bool:
		"""데이터베이스 연결 여부 확인"""
		try:
			return False if self._engine is None else True
		except:
			self._engine = None
			return False


'''클래스'''
class Item(object):
	"""데이터베이스 아이템 클래스: 데이터베이스 작업 수행하는 아이템들의 부모클래스 역할 수행"""

	'''생성자 및 소멸자'''
	def __init__(self, engine, col_info:dict, index_col:str=None):
		"""클래스 생성자: 쿼리, 테이블, 데이터프레임 등에 공통으로 사용되는 요소들 처리

		:param engine: 데이터베이스 연결 엔진
		:param col_info: 컬럼정보 {컬럼명:컬럼타입}
		:param index_col: 데이터프레임에서 인텍스 컬럼으로 사용할 컬럼명

		"""
		#멤버변수 설정
		self._engine = engine
		self._col_info = col_info

		self._index_col = None
		if self._col_info is not None:
			if len(self._col_info.keys()) > 1:
				self._index_col = copy.deepcopy(index_col)


	'''접근자'''
	@property
	def engine(self):
		return self._engine

	@property
	def col_info(self):
		return self._col_info

	@property
	def index_col(self):
		return self._index_col

	@engine.setter
	def engine(self, value):
		self._engine = value

	@col_info.setter
	def col_info(self, value):
		self._col_info = value

	@index_col.setter
	def index_col(self, value):
		self._index_col = value


'''클래스'''
class QueryItem(Item):
	"""쿼리아이템 클래스: 아이템 클래스에서 상속"""

	'''클래스 상수'''
	#데이터프레임 거치지 않고 바로 수행되어야 하는 명령의 목록
	__list_operation_execute = \
		['insert', 'delete', 'alter']


	'''생성자 및 소멸자'''
	def __init__(self, sql:str, engine, col_info:dict=None, index_col:str=None):
		"""클래스 생성자: 아이템클래스 생성자 활용

		:param sql: 쿼리문
		:param engine: 데이터베이스 연결 엔진
		:param col_info: 쿼리결과문 컬럼 정보
		:param index_col: 데이터프레임의 인덱스 컬럼

		"""
		#멤버변수 설정
		Item.__init__(self, engine, col_info, index_col)
		self._sql = copy.deepcopy(sql)						#나중에 바인드되면서 오염될 수 있기 때문에


	'''클래스 함수'''
	def _get_type(self) -> str:
		"""쿼리문의 첫 6글자를 반환. 어떤 종류의 쿼리문인지 나중에 확인하기 위함"""
		return self._sql[0:6]

	def _is_operation_excecute(self) -> bool:
		"""실행쿼리문인지의 여부"""
		return True if self._get_type().strip().lower() in self.__list_operation_execute else False

	def _is_operation_read(self) -> bool:
		"""읽기쿼리문인지의 여부"""
		return True if not self._is_operation_excecute() else False

	def bind(self, bind:dict):
		"""쿼리문 파라미터 바인드"""
		try:
			self._sql = self._sql.format(**bind)
		except:
			for key, value in bind.items():
				self._sql = self._sql.replace('{'+key+'}', value)

		return self

	def execute(self):
		"""쿼리문수행: 쿼리문타입에 따라 해당되는 작업 수행하고 결과물 반환"""
		rtn_value = None
		index_col = copy.deepcopy(self.index_col)

		if self._is_operation_read():
			if isinstance(index_col, str):
				index_col = index_col.lower()

			rtn_value = \
				pd.read_sql_query(sql=self._sql,
								  con=self._engine,
								  index_col=index_col)

			# 컬럼명, 인덱스면 대문자로 변경
			rtn_value.columns = map(str.upper, rtn_value.columns)
			if self.index_col is not None:
				rtn_value.index.name = rtn_value.index.name.upper()

		elif self._is_operation_excecute():
			self.engine.execute(self._sql)

		return rtn_value


'''클래스'''
class TableItem(Item):
	"""테이블아이템 클래스: 아이템 클래스에서 상속"""

	'''생성자 및 소멸자'''
	def __init__(self, table_name:str, engine, col_info:dict, index_col:str=None):
		"""클래스 생성자: 아이템클래스 생성자 활용

		:param table_name: 읽어오고자 하는 테이블 명
		:param engine: 데이터베이스 연결 엔진
		:param col_info: 쿼리결과문 컬럼 정보
		:param index_col: 데이터프레임의 인덱스 컬럼
		"""
		Item.__init__(self, engine, col_info, index_col)
		self._table_name = table_name

	def execute(self) -> pd.DataFrame:
		 return pd.read_sql_table(table_name=self._table_name,
								  con=self._engine,
								  index_col=self._index_col,
								  columns=list(self._col_info.keys()))


'''클래스'''
class DataFrameItem(Item):

	def __init__(self, data_frame:pd.DataFrame, table_name:str, if_exists:str,
				 engine, col_info:dict):
		Item.__init__(self, engine, col_info, None)
		self._data_frame = data_frame
		self._table_name = table_name
		self._if_exist = if_exists

	def execute(self):
		self._data_frame.to_sql(name=self._table_name,
								con=self._engine,
								if_exists=self._if_exist,
								dtype=self._col_info)

		return None


'''메인함수'''
def main():

	#데이터베이스 설정
	dbRMS01 = Connection('spt','RMS01')
	dbMYSQL = Connection('python', 'TESTDB')

	#쿼리아이템 예제
	#쿼리 설정
	sql = \
		'''	SELECT TO_DATE(TRADE_DATE,'yyyymmdd') TRADE_DATE, PX_LAST 
			FROM spt.BL_DATA 
			WHERE TRADE_DATE='{trade_date}' AND TICKER='{ticker}' 
		'''
	col_info = {'TRADE_DATE': types.DateTime(), 'PX_LAST': types.Float()}
	index_col = 'TRADE_DATE'

	testQuery = QueryItem(sql, dbRMS01.engine, col_info, index_col)

	bind = {'trade_date': '20170531', 'ticker': 'HSCEI'}
	testQuery.bind(bind)

	#결과출력
	testRESULT = testQuery.execute()
	print(testRESULT)
	print('\n')

	#테이블아이템 예제
	#테이블 설정
	table = 'els_market_oper_hist'
	col_info = {'OPER_DATE': types.DateTime()}

	testQuery = TableItem(table, dbMYSQL.engine, col_info)

	# 결과출력
	testRESULT = testQuery.execute()
	print(testRESULT)
	print('\n')


'''스크립트 파일 직접 수행되는 경우에만 실행'''
if __name__ == '__main__':
	main()
