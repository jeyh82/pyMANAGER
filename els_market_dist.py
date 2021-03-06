"""ELS 시장분포 모듈
:filename:          - els_market_dist.py
:modified:          - 2017.09.27
:note:              - 시장전체 ELS 분포에 대해서 조사 및 집계
:TODO:				- index레벨에서부터 min_index레벨을 만들어내고 (numba활용)
					  거기서부터 낙인터치여부관찰하기로 한다
					- 낙인레벨구조도 테이블에 입력하기로 한다
"""


'''모듈 불러오기'''
import numpy as np									# NUMPY
import pandas as pd									# PANDAS

import auxiliary.database as database				# 데이터베이스 모듈 불러오기
import auxiliary.query as query						# 쿼리 모듈 불러오기

import calendar										# 달력작업
from datetime import date, datetime, timedelta		# 날짜작업
from dateutil.relativedelta import relativedelta

import matplotlib.pyplot as plt						# MATPLOTLIB
from matplotlib import font_manager, rc				# MATPLOTLIB 한글폰트문제
import seaborn as sns								# SEABORN

import time											# 시간경과계산
# import getLVL_AST

# 불필요한 경고해제
pd.options.mode.chained_assignment = None

# SEABORN설정
sns.set_context(
		"paper",
		rc={"font.size":13,
			"axes.titlesize":13,
			"axes.labelsize":13})
sns.set_style("whitegrid",{'grid.linestyle': '--'})

# MATPLOTLIB한글폰트설정
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/NanumBarunGothicBold.ttf").get_name()
rc('font', family=font_name)


'''함수'''
def _get_end_of_month(date: str, format_in: str, format_out: str = '%Y-%m-%d'):
	"""특정포멧의 텍스트날짜 입력받아서 다른 텍스트포멧으로 출력

	:param date: 텍스트날짜 입력
	:param format_in: 입력받는 텍스트의 날짜포멧
	:param format_out: 출력하려는 텍스트의 날짜포멧
	:return:
	"""
	rtn_value = datetime.strptime(date, format_in)
	day = calendar.monthrange(rtn_value.year, rtn_value.month)[1]
	return rtn_value.replace(day=day).strftime(format_out)

def _get_date_monthly(data_frame:pd.DataFrame, name_col:str):
	"""데이터프레임에 날짜컬럼을 월단위로 표현하는 컬럼추가

	:param data_frame: 데이터프레임
	:param name_col: 작업원하는 컬럼
	"""
	data_frame[name_col+'_MONTHLY'] = \
		data_frame[name_col].apply(lambda x: x.strftime('%y-%m'))


'''클래스'''
class MarketDist(object):
	"""ELS 시장분포 클래스 정의
	"""
	'''클래스 상수'''
	# 하나의 상품이 가질 수 있는 최대 기초자산 개수
	_MAX_DIM = 5

	# 숫자단위변환
	_TO_ZO 		= 1000000.0
	_TO_H_UK 	= 10000.0
	_TO_UK 		= 100.0

	# 퍼포먼스 BUMP비율 리스트
	_INDEX_BUMP_STEP = 1.0
	_LIST_INDEX_BUMP = [x/1.0 for x in range(85, 115, 1)]

	# 지수리스트	
	_LIST_INDEX = ['ALL','KP2','NKY','HSC','SXE','SPX','HSI']
	
	# 발행사리스트
	_SPEC_COMP = \
		{50: '신한', 51: '대신', 	52: '미래', 	59: '미래', 	53: '삼성',
		 54: '신영', 55: 'NH', 	69: 'NH',	57: '한투',	58: 'KB',
		 77: 'KB', 	60: '맥쿼리',	61: '메리츠',	62: '교보', 	64: '유진',
		 65: '한화', 66: '유안타',	67: '동부', 	68: 'SK', 	70: '하나',
		 72: 'IBK', 73: '현대차',	74: 'SC', 	75: '노무라',	76: '키움',
		 78: 'BNP', 79: '하이' }

	# 그래프 출력에 사용하는 관심발행사 리스트
	# _LIST_NAME_BIG_COMP = ['신영','미래','KB','NH','삼성','한투','신한','기타']
	# _LIST_COLOR_BIG_COMP = ['# 114F1C','# F36910','# FBCB35','# FF0000','# 0052A5','# 503200','# 551A8B','# A9A9A9']
	_LIST_NAME_BIG_COMP = ['신영','기타']
	_LIST_COLOR_BIG_COMP = ['# 114F1C','# A9A9A9']

	'''클래스 생성자 및 소멸자'''
	def __init__(self):
		"""클래스 생성자: 필요한 데이터베이스 연결 및 날짜정보 설정
		"""
		# 저장소 설정
		self._dbRMS01 = database.Connection('spt','RMS01')
		self._dbMYSQL = database.Connection('python','TESTDB')
		self._hdf5_store = pd.io.pytables.HDFStore('store.h5')

		# 날짜 설정
		today = date.today()
		self._eval_date = today.strftime('%Y-%m-%d')
		self._last_date = self._get_last_oper_date()

		self._fr_this_year = (today-relativedelta(months=11)).strftime('%Y-%m')+'-01'
		self._to_this_year = self._eval_date

		self._fr_full_year = '2000-01-01'
		self._to_full_year = '2100-12-31'

		# 테이블 불러오기
		try:
			self._table_initial 	= self._hdf5_store['els_market_initial_info']
			self._table_issue		= self._hdf5_store['els_market_issue_info']
			self._table_exercise 	= self._hdf5_store['els_market_exercise_info']
			self._table_underlying 	= self._hdf5_store['els_market_underlying_info']
			self._table_struct 		= self._hdf5_store['els_market_structure_info']
			self._table_ki 			= self._hdf5_store['els_market_ki_info']

			self._table_index = self._hdf5_store['els_market_index_info']
			self._latest_index = self._table_index.tail(1)
			self._index_date = self._latest_index.reset_index().iat[0,0].strftime('%Y-%m-%d')
		except:
			pass


	'''클래스 함수'''
	'''ORACLE데이터베이스에서 데이터정리해서 MYSQL데이터베이스로 마이그레이션'''
	def transfer_basic_info(self, oper_name:str, show:bool=True):
		"""기본정보 조회하여 MYSQL에 저장
		- 시장전체 ELS상품들의 기본정보를 조회하여 MYSQL에 저장
		- 최종적으로는 MYSQL데이터베이스에 다시 입력하기 위한 용도로 활용
		- 작업구분에 따라, 	현재시점에서 남아있는 상품들에 대해서 조회
		 				특정기간동안 발행된 상품 조회
		 				특정기간동안 상환된 상품 조회
		  등의 작업 수행

		:param oper_name: 수행할 작업의 종류 결정
		:raises Exception: 작업목록이 잘못된 경우 예외발생
		:return: pd.DataFrame 형식으로 기본정보 조회하여 출력
		"""
		# 작년말 기준으로 활성화되어 있는 상품에 대해서만
		if oper_name == 'listInit':
			table_name = 'els_market_initial_info'
			bind_param = \
				{'eval_date': self._fr_this_year,
				 'prsv_rate': '1000', 									# 원금 보장여부: 관계없음
				 'req_amt_min': '0',									# 발행잔액 남아있는 경우만
				 'req_amt_max': '100000000000',
				 'fr_std_date': self._fr_full_year,
				 'fr_eff_date': self._fr_full_year,						# 발행일자 아주 먼 과거에서부터, 작년말까지
				 'to_eff_date': self._fr_this_year,
				 'fr_exp_date': self._fr_this_year,						# 작년 말에서부터, 아주 먼 미래까지
				 'to_exp_date': self._to_full_year}

		# 특정기간동안 발행되었던 전체 상품에 대해서
		elif oper_name == 'listIssue':
			table_name = 'els_market_issue_info'
			bind_param = \
				{'eval_date': self._eval_date,
				 'prsv_rate': '1000', 									# 원금 보장여부: 관계없음
				 'req_amt_min': '-1',									# 발행잔액: 관계없음
				 'req_amt_max': '100000000000',
				 'fr_std_date': self._fr_this_year,						# 최근변동일자: 발행이 올해초에 되었다면, 적어도 올해초는 넘어야함
				 'fr_eff_date': self._fr_this_year,						# 최초발행일자: 올해초에서부터 조회기준일까지
				 'to_eff_date': self._eval_date,
				 'fr_exp_date': self._fr_full_year,						# 만기상환일자: 관계없음
				 'to_exp_date': self._to_full_year}

		# 특정기간동안 상환되었던 전체 상품에 대해서
		elif oper_name == 'listExercise':
			table_name = 'els_market_exercise_info'
			bind_param = \
				{'eval_date': self._eval_date,
				 'prsv_rate': '1000', 									# 원금 보장여부: 관계없음
				 'req_amt_min': '-1',									# 발행잔액: 0인것만 조회
				 'req_amt_max': '1',
				 'fr_std_date': self._fr_this_year,						# 최근변동일자: 만기일자가 될텐데. 올해초는 넘어야함
				 'fr_eff_date': self._fr_full_year,						# 최초발행일자: 아주먼과거에서부터, 조회기준일 이전까지
				 'to_eff_date': self._eval_date,
				 'fr_exp_date': self._eval_date,						# 만기상환일자: 조회기준일부터, 아주먼미래까지
				 'to_exp_date': self._to_full_year}
		else:
			raise Exception('ERROR _ invalid operation name')

		# 기본정보조회
		rtn_basic = \
			self._dbRMS01.query(
					sql=query.basic_sql,
					bind=bind_param,
					index_col='ISIN_NO')

		# 데이터프레임 변수를 데이터베이스에 인서트
		self._dbMYSQL.insert_table(
				table_name=table_name,
				data_frame=rtn_basic,
				if_exists='replace',
				col_info=query.basic_col)

		# 제약조건 설정
		self._dbMYSQL.query(
				sql=query.alter_basic_sql,
				bind={'table_name':table_name})

		# 작업완료 메세지출력
		if show is True:
			print('기본정보가 입력되었습니다.')
			
	def transfer_underlying_info(self, show:bool=True):
		"""기초자산 정보 조회하여 MYSQL에 저장
		- 시장전체 ELS상품들의 기초자산 정보를 조회하여 MYSQL에 저장
		- 최종적으로는 MYSQL데이터베이스에 다시 입력하기 위한 용도로 활용
		- 최근작업일자 이후, 현재작업일자 이전에 발생한 데이터들에 대해서만 처리

		:return: 기초자산정보 데이터프레임 형태로 반환
		"""
		# 바인드변수 설정
		bind_param = \
			{'last_date':self._last_date,
			 'eval_date':self._eval_date}

		# 기초자산정보조회
		rtn_asset = \
			self._dbRMS01.query(
					sql=query.asset_sql,
					bind=bind_param,
					index_col='ISIN_NO')

		# 조회 결과물 데이터베이스에 인서트
		self._dbMYSQL.insert_table(
				table_name='els_market_underlying_info',
				data_frame=rtn_asset,
				if_exists='append',
				col_info=query.asset_col)
		
		# 작업완료 메세지출력
		if show is True:
			print('기초자산정보가 입력되었습니다.')

	def transfer_structure_info(self, show:bool=True):
		"""상환구조 정보 조회
		- 시장전체 ELS상품들의 상환구조 정보를 조회
		- 최종적으로는 MYSQL데이터베이스에 다시 입력하기 위한 용도로 활용
		- 최근작업일자 이후, 현재작업일자 이전에 발생한 데이터들에 대해서만 처리

		:return: 상환구조 데이터프레임 형태로 반환
		"""
		# 바인드변수 설정
		bind_param = \
			{'last_date': self._last_date,
			 'eval_date': self._eval_date}

		# 상환구조정보조회
		rtn_asset = \
			self._dbRMS01.query(
					sql=query.exer_sql,
					bind=bind_param,
					index_col=None)

		# 조회 결과물 데이터베이스에 인서트
		self._dbMYSQL.insert_table(
				table_name='els_market_structure_info',
				data_frame=rtn_asset,
				if_exists='append',
				col_info=query.exer_col,
				index=False)
		
		# 작업완료 메세지출력
		if show is True:
			print('구조정보가 입력되었습니다.')

	def transfer_ki_info(self, show:bool=True):
		"""낙인구조 정보 조회
		- 시장전체 ELS상품들의 낙인구조 정보를 조회
		- 최종적으로는 MYSQL데이터베이스에 다시 입력하기 위한 용도로 활용
		- 최근작업일자 이후, 현재작업일자 이전에 발생한 데이터들에 대해서만 처리

		:return: 낙인구조 데이터프레임 형태로 반환
		"""
		# 바인드변수 설정
		bind_param = \
			{'last_date': self._last_date,
			 'eval_date': self._eval_date}

		# 상환구조정보조회
		rtn_asset = \
			self._dbRMS01.query(
					sql=query.ki_sql,
					bind=bind_param,
					index_col=None)

		# 조회 결과물 데이터베이스에 인서트
		self._dbMYSQL.insert_table(
				table_name='els_market_ki_info',
				data_frame=rtn_asset,
				if_exists='append',
				col_info=query.ki_col,
				index=False)
		
		# 작업완료 메세지출력
		if show is True:
			print('낙인정보가 입력되었습니다.')

	'''데이터베이스 작업중 작업일자 기록작성'''
	def _delete_log(self):
		"""해당일자 로그가 존재하면 삭제
		"""
		self._dbMYSQL.query(
				sql=query.hist_delete_sql,
				bind={'oper_date':self._eval_date})

	def _create_log(self):
		"""작업일자를 기록으로 남김
		"""
		self._dbMYSQL.query(
				sql=query.hist_insert_sql,
				bind={'oper_date':self._eval_date})

	'''데이터프레임 변수반복접근'''
	def _get_lastest_index_px(self, asset):
		"""최근일자 지수레벨 반환

		:param asset: 기초자산명
		:return: 최근일자 지수레벨
		"""
		return self._latest_index.reset_index().at[0, asset]

	def _get_cust_name(self, cust_no):
		"""회사코드를 회사명으로 변경

		:param cust_no: 발행사번호
		:return: 회사명반환
		"""
		rtn_value = self._SPEC_COMP.get(cust_no, '기타')
		return rtn_value if rtn_value in self._LIST_NAME_BIG_COMP else '기타'

	def _get_last_oper_date(self) -> str:
		"""가장 최근 작업일자 조회

		:return: 가장 최근 작업일자
		"""
		last_date = \
			self._dbMYSQL.query(sql=query.hist_select_sql)\
				.iat[0,0]

		return '{:%Y-%m-%d}'.format(last_date)

	'''데이터프레밍 마스크작업 반복적용'''
	def _get_mask_asset(self, data_frame:pd.DataFrame, asset:str):
		"""해당 데이터프레임의 항목중 특정기초자산을 포함하는 항목들에 대해서만 마스크작성

		:param data_frame: 데이터프레임
		:param asset: 기초자산명
		:return: 데이터프레임 마스크 반환
		"""
		return (data_frame['NAME_AST1'] == asset) | \
			   (data_frame['NAME_AST2'] == asset) | \
			   (data_frame['NAME_AST3'] == asset) | \
			   (data_frame['NAME_AST4'] == asset) | \
			   (data_frame['NAME_AST5'] == asset)

	def _get_mask_date(self, data_frame:pd.DataFrame, date:str):
		"""해당 데이터프레임의 항목중 특정일자 이전에 해당하는 항목들에 대해서만 마스크작성

		:param data_frame: 데이터프레임
		:param asset: 기초자산명
		:return: 데이터프레임 마스크 반환
		"""
		return data_frame['STD_DATE'] <= date
	
	'''기타편의함수'''
	def _get_monthly_table_empty(self) -> pd.DataFrame:
		"""월간보고서 작성을 위한 기본 테이블 작성

		:return: 기본테이블 반환
		"""
		months = pd.date_range(self._fr_this_year, periods=12, freq='M')
		data = months.map(lambda x: datetime.strftime(x, '%Y-%m-%d'))
		index = months.map(lambda x: datetime.strftime(x, '%y-%m'))
		return pd.DataFrame(data=data, index=index, columns=['EOMONTH'])

	'''데이터프레임 불러오는 함수'''
	def get_table_basic(self, table_name:str, date:str=None, asset:str=None, comp:str=None, prsv:float=None, order:int=0):
		"""특정 기초자산을 포함하는, 연초기준 활성화 목록 반환

		:param table_name: 집계대상에 따라 테이블이름 설정
		:param date: 기준일자 입력
		:param asset: 특정 기초자산 포함하는 경우에만 
		:param comp: 특정발생사가 발행한 상품에 대해서만
		:param prsv: 입력된 원금보장레벨 이하에 대해서만 
		:param order: 10단위 숫자제거		
		:return: 연초기준 활성화목록
		"""
		# 계산에 필요한 테이블설정
		if table_name == 'INITIAL':
			table = self._table_initial
		elif table_name == 'ISSUE':
			table = self._table_issue
		elif table_name == 'EXERCISE':
			table = self._table_exercise
		else:
			raise Exception

		# 노셔널 10단위 제거
		for col in ('FIRST_AMT','REMAIN_AMT'):
			table[col] = round(table[col]/(10**order))
			table[col] = table[col].astype(int)

		# 디폴트마스크 작성성
		mask = pd.Series(
				data=[True]*table.shape[0],
				index=table.index)

		# 날짜에 따라 필터링
		if date is not None:
			mask &= self._get_mask_date(table, date)

		# 기초자산에 따라필터링
		if asset is not None:
			mask &= self._get_mask_asset(table,asset)

		# 발행사에 따라 필터링
		if comp is not None:
			mask &= (table['COMP']==comp)

		# 원금보장비율에 따라 필터링
		if prsv is not None:
			mask &= (table['PRSV_RATE']<=prsv)
		
		# 필터링된 결과값 반환
		return table[mask]

	def get_active_list(self, date:str=None, asset:str=None, comp:str=None, prsv:float=100.0, order:int=0) -> pd.DataFrame:
		"""기준일자 시점에서 활성화 목록 반환

		:param date: 기준일자 입력
		:param asset: 특정 기초자산 포함하는 경우에만 
		:param comp: 특정발생사가 발행한 상품에 대해서만
		:param prsv: 입력된 원금보장레벨 이하에 대해서만 
		:param order: 10단위 숫자제거
		:return: 기준일자 시점의 활성화 목록
		"""
		# 기본테이블 조회
		table_initial = self.get_table_basic('INITIAL',date, asset, comp, prsv, order)
		table_issue = self.get_table_basic('ISSUE', date, asset, comp, prsv, order)
		table_exercise = self.get_table_basic('EXERCISE',date, asset, comp, prsv, order)

		# 작년말기준 활성화목록에 올해 발행목록 결합
		rtn_table = \
			pd.concat(
					objs=[table_initial, table_issue],
					join='outer')

		# 올해 상환목록도 결합
		rtn_table = \
			rtn_table.join(
					other=table_exercise,
					how='left',
					rsuffix='_EXER')

		# 상환테이블정보가 없는 경우만 필터링
		mask = rtn_table['STD_DATE_EXER'].isnull()

		# 상환목록은 더이상 필요하지 않으므로 해당컬럼 제거
		rtn_table.drop([col for col in rtn_table.columns if '_EXER' in col],axis=1,inplace=True)

		# 결과값 반환
		return rtn_table[mask]
	
	'''그래프 작성에 필요한 데이터 테이블 작성하는 함수'''
	def get_monthly_report(self, asset:str=None, comp:str=None, prsv:float=100.0, order:int=0):
		"""특정 기초자산을 포함하는, 월간합계 보고서 반환

		:param asset: 해당 기초자산을 포함하는 경우에 대해서만
		:param comp: 해당 발행사가 발행한 경우에 대해서만
		:param prsv: 해당 원금보장비율 이하에 대해서만 
		:param order: 10단위 숫자제거
		:return: 월간합계 보고서
		"""
		# 발행정보, 상환정보 조회
		table_issue = self.get_table_basic('ISSUE' ,None, asset, comp, prsv, order)
		table_exercise = self.get_table_basic('EXERCISE', None, asset, comp, prsv, order)

		# 발행정보 합계계산. EFF_DATE기준으로 합산한다는 것에 주의
		table_issue_monthly = \
			table_issue\
				.groupby('EFF_DATE_MONTHLY')['FIRST_AMT'] \
				.agg(['sum']) \
				.rename(columns={'sum':'ISSUE'})

		# 상환정보 합계계산. STD_DATE기준으로 합산한다는 것에 주의
		table_exercise_monthly = \
			table_exercise\
				.groupby('STD_DATE_MONTHLY')['FIRST_AMT']\
				.agg(['sum'])\
				.rename(columns={'sum':'EXERCISE'})

		# 월별 합계정보 테이블 준비
		rtn_table = self._get_monthly_table_empty()

		# 발행정보, 상환정보를 월별 합계정보에 결합
		rtn_table = rtn_table.join(table_issue_monthly)
		rtn_table = rtn_table.join(table_exercise_monthly)
		rtn_table.fillna(0, inplace=True)

		# 발행잔액 계산하여 결합
		for idx, item in rtn_table.iterrows():
			rtn_table.loc[idx,'ACTIVE'] = \
				self.get_active_list(item['EOMONTH'], asset, comp, prsv, order)['FIRST_AMT'].sum()

		# 인텍스를 JAN, FEB 형식으로 변경
		rtn_table.index = rtn_table.index.map(lambda x: calendar.month_abbr[int(x[3:5])])

		# 결과값 반환
		return rtn_table

	def get_exercise_report(self, asset:str, period:int):
		"""특정 기초자산을 포함하는, 월간상환 보고서 반환

		:param asset: 해당 기초자산을 포함하는 경우에 대해서만
		:param period: 해당 기간동안에 대해서만 (단위: 월)
		:return: 월간상환 보고서
		"""
		# 기초자산 입력값이 없으면 에러발생
		if asset is None:
			raise Exception("ERROR _ invalid input 'asset'")

		# 해당기초자산 포함하는 현재활성화 리스트
		table = self.get_active_list(None, asset, None, 0.0, 0)

		# 기초자산 BUMP스케줄에 따라서
		for idx_bump, item_bump in enumerate(self._LIST_INDEX_BUMP):
			# BUMP스케줄에 따라서 퍼포먼스 기록
			self.create_performance_column(table, asset, item_bump)

			# 상환보고서 포함기준
			mask = (table['EXE_DATE'] <= date.today()+relativedelta(months=period)) & \
				   (table['WORST_AST'] == asset) & \
				   (table['EXE_LVL'] >= table['WORST_LVL']) & \
				   (table['EXE_LVL'] < table['WORST_LVL']+self._INDEX_BUMP_STEP)

			# 행사가격 표시
			table.loc[mask,'K_LBOUND'] = item_bump

		# 최종그래프 작업에 포함되는 데이터만 필터링
		mask = (table['K_LBOUND'] >= self._LIST_INDEX_BUMP[0]) & \
			   (table['K_LBOUND'] <= self._LIST_INDEX_BUMP[-1] + self._INDEX_BUMP_STEP)
		
		# 필터링된 데이터 그룹별로 합계계산
		rtn_table = \
			table[mask]\
				.reset_index()\
				.groupby(['EXE_DATE','K_LBOUND'])['REMAIN_AMT']\
				.agg(['sum'])\
				.rename(columns={'sum':'REMAIN_AMT'})

		# 100억단위로 변경
		rtn_table['REMAIN_AMT'] /= self._TO_H_UK

		# 결과값 반환
		return rtn_table

	'''그래프 작성하는 함수'''
	def draw_monthly_figure(self, asset:str=None, prsv:str=None):
		"""월간합계보고서 출력
		- get_monthly_report에서 가져오는 데이터프레임을 발행사별로 출력
		- 순서대로, 발행액, 상환액, 잔존금액
		
		:param asset: 특정기초자산에 대해서만 집계하는 경우 
		:param prsv: 원금보장형 포함여부
		:return: 그래프, 축 반환
		"""
		# 그래프 제목설정
		title = ['월별발행금액 (단위:조)\n기초자산:{asset}, ELB포함:{prsv}' + '\n',
				 '월별상환금액 (단위:조)\n기초자산:{asset}, ELB포함:{prsv}' + '\n',
				 '월별발행잔액 (단위:조)\n기초자산:{asset}, ELB포함:{prsv}' + '\n']
		title = [item.replace('{asset}',asset) for item in title]
		title = [item.replace('{prsv}', prsv) for item in title]

		# 그래프변수 설정
		alpha = 0.60

		# 입력변수 필터링
		asset = None if asset == 'ALL' else asset
		prsv = 100.0 if prsv == 'YES' else 0.0

		# 그래프 데이터 초기화
		fig, (ax_issue, ax_exercise, ax_active) = \
			plt.subplots(3, 1, figsize=(11, 18))
		grp_fig = [ax_issue, ax_exercise, ax_active, fig]
		plt.subplots_adjust(hspace=0.4)

		# 집계금액에 따라 순환
		for idx_col, item_col in enumerate(['ISSUE', 'EXERCISE', 'ACTIVE']):
			# 스택 데이터 초기화
			list_y_values = []
			y_values = np.zeros(12)
			list_legend = []

			# 발행사에 따라 순환
			for idx_comp, item_comp in enumerate(self._LIST_NAME_BIG_COMP):
				# 발행사에 따라 월간보고서 집계
				tbl = self.get_monthly_report(asset, item_comp, prsv)

				# 월간보고서에서 리스트 집계
				x_values = list(tbl.index)
				col_values = list(round(tbl[item_col].astype(float)/self._TO_ZO,2))		# 조단위로 수정

				# 스택데이터 누적
				y_values = [val_col+val_y for val_col,val_y in zip(col_values, y_values)]
				list_y_values.insert(0, y_values)
				
				# 범례설정
				legend = \
					plt.Rectangle(
							(0,0),1,1,
							fc=self._LIST_COLOR_BIG_COMP[(idx_comp+1)*-1],
							alpha=alpha,
							edgecolor='black')
				list_legend.insert(0, legend)

			# 스택그래프 출력
			for idx_comp, item_comp in enumerate(self._LIST_NAME_BIG_COMP):
				sns.barplot(
						x=x_values,
						y=list_y_values[idx_comp],
						ax=grp_fig[idx_col],
						color=self._LIST_COLOR_BIG_COMP[(idx_comp+1)*-1],
						alpha=alpha,
						linewidth=1.25,
						edgecolor='black')

			# 그래프설정
			yticks = ['{:3,.2f}'.format(tick) for tick in grp_fig[idx_col].get_yticks()]
			grp_fig[idx_col].set_yticklabels(yticks)
			grp_fig[idx_col].set_title(title[idx_col], fontstyle='italic')
			grp_fig[idx_col].tick_params(labelsize=13)
			grp_fig[idx_col].grid(True, which='both')
			grp_fig[idx_col].legend(
					list_legend,
					self._LIST_NAME_BIG_COMP,
					loc='right',
					ncol=1,
					bbox_to_anchor=(1.15,0.5),
					prop={'size':13})

			# 그래프위에 숫자출력
			for patch in grp_fig[idx_col].patches:
				height = patch.get_height()
				grp_fig[idx_col].text(
						patch.get_x()+patch.get_width()/2.0,
						height+0.05,
						'{:1.2f}'.format(height),
						ha='center')

		# 그래프 반환
		return grp_fig

	def draw_exercise_figure(self, asset:str, period:int=1):
		"""일별행사가격분포 리포트 출력

		:param asset: 분포작성을 원하는 기초자산명
		:return: 그래프 반환
		"""

		# 그래프 제목설정
		title = '일별행사가격분포 (단위:백억)\n기초자산:' + asset + '\n'

		# 입력변수 필터링
		if 	asset is None or \
			not asset in self._LIST_INDEX:
			raise Exception("ERROR _ invalid input 'asset'")

		# 그래프 데이터 초기화
		fig, ax = plt.subplots(figsize=(15, 11))

		# 기초자산에 따라 월간보고서 집계
		tbl = self.get_exercise_report(asset, period)
		pvtbl = tbl.reset_index().pivot(
				index='EXE_DATE',
				columns='K_LBOUND',
				values='REMAIN_AMT')
		pvtbl.fillna(0, inplace=True)

		# 빈 로우 0으로 채우기
		date_index = pd.date_range(date.today(), periods=30, freq='D')
		for row in date_index:
			if not row in list(pvtbl.index):
				if row.weekday() < 5:
					pvtbl.loc[row] = [0]*pvtbl.shape[1]

		# 로우에 주말이 있는 경우 제거
		for idx_row, item_row in enumerate(list(pvtbl.index)):
			if item_row.weekday() >= 5:
				pvtbl.drop(pvtbl.index[idx_row], inplace=True)

		# 빈 컬럼 0으로 채우기
		for col in self._LIST_INDEX_BUMP:
			if not col in list(pvtbl.columns.values):
				pvtbl[col] = 0.0

		# 인덱스에 대해 정렬
		pvtbl.sort_index(axis=0, inplace=True)
		pvtbl.sort_index(axis=1, inplace=True)

		# 시작시점의 요일에 따라 그래프 설정 변경
		loc_begin = (5 - pvtbl.head(1).index.weekday) % 5
		loc_begin = loc_begin._data.item(0)

		# 인덱스 날짜형식변경
		pvtbl.index = pvtbl.index.map(lambda x: x.strftime('%m/%d'))

		# 그래프출력
		sns.heatmap(
				data=pvtbl,
				annot=True,
				fmt=',.1f',
				linewidths=0.5,
				cmap='BuGn',
				square=False,
				vmin=0, vmax=20,
				cbar=False,
				mask=pvtbl<0.01,
				ax=ax)

		# 그래프설정
		ax.set_title(title, fontstyle='italic')
		ax.tick_params(labelsize=13)
		ax.hlines([range(0, pvtbl.shape[0]+1, 1)], *ax.get_xlim(), color='gray', linestyles='--')
		ax.hlines([range(loc_begin+0, pvtbl.shape[0]+1, 5)], *ax.get_xlim(), color='black', linestyles='-')
		ax.vlines([range(0, pvtbl.shape[1]+1, 1)], *ax.get_ylim(), color='gray', linestyles='--')
		ax.vlines([range(0, pvtbl.shape[1]+1, 5)], *ax.get_ylim(), color='black', linestyles='-')

		# Y레이블
		for tick in ax.get_yticklabels():
			tick.set_rotation(0)

		# X레이블
		ax.set_xlabel('행사가격%')
		last_px = self._get_lastest_index_px(asset)
		xlabels = ['{:>5,.2f}'.format(last_px*float(label.get_text())/100.0)+'   '+'{:>3d}'.format(int(float(label.get_text()))) for label in ax.get_xticklabels()]
		ax.set_xticklabels(xlabels)
		for tick in ax.get_xticklabels():
			tick.set_rotation(90)

		# 그래프 반환
		return fig, ax

	'''작성된 그래프 래핑해서 반환하는 함수'''
	def set_monthly_figure(self):
		"""월간 금액 그래프결과를 기초자산별로 사전형태로 그루핑하여 반환

		:return: 그래프결과 사전 반환
		"""
		# 결과값 초기화
		dict_grp_fig = {}

		# 기초자산에 대해서, 원금보장포함여부에 대해서 루프
		for idx_asset, item_asset in enumerate(self._LIST_INDEX):
			for idx_prsv, item_prsv in enumerate(['YES']):
				# TODO 원금비보장까지 포함하는 경우에 대해서만 처리하기로 한다. 나중에 계산속도에 여력이 생기면 추가하는 것으로 하자
				# 데이터하나씩 추가
				grp_fig = self.draw_monthly_figure(item_asset, item_prsv)
				key = item_asset + item_prsv
				dict_grp_fig.update({key: grp_fig})

		# 결과값 반환
		return dict_grp_fig

	def set_exercise_figure(self):
		"""일별 행사가격 그래프 결과를 기초자산별로 계산하여 사전형태로 반환

		:return: 그래프결과 사전 반환
		"""
		# 결과값 초기화
		dict_fig = {}

		# 기초자산에 대해서 루프
		for idx_asset, item_asset in enumerate(self._LIST_INDEX[1:]):
			# 데이터하나씩 추가
			fig, ax = self.draw_exercise_figure(item_asset)
			dict_fig.update({item_asset: [fig, ax]})

		# 결과값 반환
		return dict_fig

	'''오퍼레이션 함수'''
	def operation_transfer(self):
		"""트랜스터 함수 일괄처리

		:return:
		"""
		self.transfer_underlying_info()
		self.transfer_structure_info()
		self.transfer_ki_info()

		self.transfer_basic_info('listInit')
		self.transfer_basic_info('listIssue')
		self.transfer_basic_info('listExercise')
		
	def operation_basic_store(self):
		"""데이터베이스 내용 불러와서 HDF5로 입력

		:return:
		"""
		# 기초자산 정보 입력
		self._hdf5_store['els_market_underlying_info'] = \
			self._dbMYSQL.select_table('view_els_market_underlying_info', 'ISIN_NO')

		# 상품구조 정보 입력
		self._hdf5_store['els_market_structure_info'] = \
			self._dbMYSQL.query(
					sql=query.next_exer_sql,
					bind={'eval_date': self._eval_date},
					index_col='ISIN_NO')

		# 낙인레벨 정보 입력
		self._hdf5_store['els_market_ki_info'] = \
			self._dbMYSQL.select_table('view_els_market_ki_info', 'ISIN_NO')

		# 인덱스 레벨 정보 입력
		self._hdf5_store['els_market_index_info'] = \
			self._dbRMS01.query(
					sql=query.index_sql,
					bind={'date_from': '2014-01-01',
						  'eval_date': self._eval_date},
					index_col='STD_DATE')

		# 기본정보 테이블 조인 후 정보 입력
		table_name = ['els_market_initial_info',
					  'els_market_issue_info',
					  'els_market_exercise_info']

		for index, item in enumerate(table_name):
			# 테이블 기본정보조회
			table = \
				self._dbMYSQL.select_table(
						table_name=item,
						index_col='ISIN_NO')

			# 발행회사 정보추가
			table['COMP'] = \
				table.index.str[3:5].astype(int).map(self._get_cust_name)

			# 지수레벨정보 조인, 기준가격에 대해서 조인

			table = \
				table.join(
						other=self._hdf5_store['els_market_underlying_info'],
						how='inner')

			# 기초자산정보 조인
			table = \
				pd.merge(
						left=table,
						right=self._hdf5_store['els_market_index_info'],
						how='left',
						left_on=['EFF_DATE'],
						right_index=True)

			# 상환구조정보조인
			table = \
				pd.merge(
						left=table,
						right=self._hdf5_store['els_market_structure_info'],
						how='left',
						left_index=True,
						right_index=True)

			# 낙인구조정보조인
			table = \
				pd.merge(
						left=table,
						right=self._hdf5_store['els_market_ki_info'],
						how='left',
						left_index=True,
						right_index=True)

			# 날짜에 해당하는 컬럼들마다 매월말일에 해당하는 컬럼추가
			for col in ['STD_DATE', 'EFF_DATE', 'MAT_DATE']:
				_get_date_monthly(table, col)

			# 계산결과 HDF5 저장
			self._hdf5_store[item] = table

	def operation_report_store(self):
		"""그래프 작성에 필요한 데이터정보 계산후 저장

		:return: 없음
		"""
		for asset in self._LIST_INDEX[1:]:
			for idx_comp, item_comp in enumerate(self._LIST_NAME_BIG_COMP):
				for prsv in [100.0, 0.0]:
					report = self.get_monthly_report(asset, item_comp, prsv)
					name = 'report_monthly_'+str(asset)+'_'+str(idx_comp)+'_'+('YES' if prsv == 100.0 else 'NO')
					self._hdf5_store[name] = report
		
		for asset in self._LIST_INDEX[1:]:
			report = self.get_exercise_report(asset, 1)
			name = 'report_exercise_' + str(asset)
			self._hdf5_store[name] = report
			

	'''추가적인 컬럼정보 작성'''
	def create_performance_column(self, table:pd.DataFrame, target_asset:str=None, ratio_mult_in:float=100.0):
		"""테이블에 기초자산 퍼포먼스 컬럼추가
		- LVL_ASTX 컬럼 추가됨: X번째 기초자산의 현재가/기준가 수준 기록
		- WORST_LVL 컬럼 추가됨: LVL_ASTX중에서 가장 작은 숫자 기록
		- WORST_AST 컬럼 추가됨: WORST_LVL을 기록한 기초자산의 이름 기록

		:param table: 컬럼을 추가하고자 하는 테이블
		:param target_asset: 지수 BUMP를 반영하고자 하는 기초자산 이름
		:param ratio_mult_in: 지수 BUMP비율
		:return: 없음
		"""
		# 지수레벨정보로부터 퍼포먼스(최초기준가대비 현재지수레벨) 계산
		for idx in range(1, self._MAX_DIM+1):
			# 컬럼명
			name_str = 'NAME_AST' + str(idx)
			level_str = 'LVL_AST' + str(idx)

			# 마스크설정
			for name in self._LIST_INDEX[1:]:
				latest_level = self._get_lastest_index_px(name)
				ratio_mult = ratio_mult_in if name == target_asset else 100.0
				mask = table[name_str] == name

				table.loc[mask, level_str] = \
					latest_level / table.loc[mask, name] * ratio_mult

		# 워스트퍼포머 계산
		col_lvl = ['LVL_AST' + str(x) for x in range(1, self._MAX_DIM+1)]
		col_name = ['NAME_AST' + str(x) for x in range(1, self._MAX_DIM+1)]
		col_name.insert(0, 'WORST_AST')

		table['WORST_LVL'] = table[col_lvl].min(axis=1)
		table['WORST_AST'] = table[col_lvl].idxmin(axis=1)
		table['WORST_AST'] = \
			table[col_name].apply(
				lambda row: np.NaN if row['WORST_AST'] is np.NaN else \
							row['NAME_AST' + row['WORST_AST'][-1]], axis=1)


'''메인함수'''
def main():
	# 시작시점설정
	start = time.time()

	# 시장분포 객체생성
	distMarket = MarketDist()

	distMarket.operation_basic_store()
	distMarket.operation_report_store()

	'''원본 데이터베이스 내역 정리해서 새로삽입'''
	# 오늘 작업 내역 존재하지 않는 경우에만 작업
	if distMarket._eval_date != distMarket._last_date:
		distMarket.operation_transfer()
		distMarket.operation_basic_store()
		distMarket.operation_report_store()

	# 작업일자 기록
	distMarket._delete_log()
	distMarket._create_log()
	print('작업일자가 기록되었습니다.')
	print(distMarket._eval_date + '\n')

	# 종료시점 설정
	elapsed = time.time() - start
	print(elapsed)


'''스트립트 파일 직접 수행되는 경우에만 실행'''
if __name__ == '__main__':
	main()



