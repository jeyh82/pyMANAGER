"""ELS 시장분포 모듈
:filename:          - els_market_dist.py
:modified:          - 2017.08.17
:note:              - 시장전체 ELS 분포에 대해서 조사 및 집계
:TODO:				- 월간보고서 시작 시점이 매년 1월로 되어있는데 가급적이면 직전12개월 이런식으로 수정하자 (나중에)
"""


'''모듈 불러오기'''
import numpy as np									#NUMPY
import pandas as pd									#PANDAS

import auxiliary.database as database				#데이터베이스 모듈 불러오기
import auxiliary.query as query						#쿼리 모듈 불러오기

import calendar										#달력작업
from datetime import date, datetime, timedelta		#날짜작업
from dateutil.relativedelta import relativedelta

import seaborn as sns								#SEABORN
import matplotlib.pyplot as plt						#MATPLOTLIB


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
	#하나의 상품이 가질 수 있는 최대 기초자산 개수
	_MAX_DIM = 5

	#숫자단위변환
	_TO_ZO = 1000000.0
	_TO_UK = 100.0

	#퍼포먼스 BUMP비율 리스트
	_LIST_INDEX_BUMP = [0.85, 0.90, 0.95, 1.00, 1.05, 1.10, 1.15]
	_INDEX_BUMP_STEP = 0.05

	#지수리스트	
	_LIST_INDEX = ['ALL','KP2','NKY','HSC','SXE','SPX','HSI']
	
	#발행사리스트
	_SPEC_COMP = \
		{50: '신한', 51: '대신', 	52: '미래', 	59: '미래', 	53: '삼성',
		 54: 'SY', 55: 'NH', 	69: 'NH',	57: '한투',	58: 'KB',
		 77: 'KB', 	60: '맥쿼리',	61: '메리츠',	62: '교보', 	64: '유진',
		 65: '한화', 66: '유안타',	67: '동부', 	68: 'SK', 	70: '하나',
		 72: 'IBK', 73: '현대차',	74: 'SC', 	75: '노무라',	76: '키움',
		 78: 'BNP', 79: '하이' }

	#그래프 출력에 사용하는 관심발행사 리스트
	_LIST_NAME_BIG_COMP = ['SY', 'ALL']
	_LIST_COLOR_BIG_COMP = ['#114F1C','#A9A9A9']
	

	'''클래스 생성자 및 소멸자'''
	def __init__(self):
		"""클래스 생성자: 필요한 데이터베이스 연결 및 날짜정보 설정
		"""
		today = date.today()

		self._dbRMS01 = database.Connection('spt','RMS01')
		self._dbMYSQL = database.Connection('python','TESTDB')

		self._eval_date = today.strftime('%Y-%m-%d')
		self._last_date = self._get_last_oper_date()

		self._fr_this_year = (today-relativedelta(months=11)).strftime('%Y-%m')+'-01'
		self._to_this_year = self._eval_date

		self._fr_full_year = '2000-01-01'
		self._to_full_year = '2100-12-31'

		#기초자산 테이블 불러오기
		self._table_asset = \
			self._dbMYSQL.select_table(
					table_name='view_els_market_underlying_info',
					index_col='ISIN_NO')

		#상환구조 테이블 불러오기
		self._table_struct = \
			self._dbMYSQL.query(
					sql=query.next_exer_sql,
					bind={'eval_date': self._eval_date},
					index_col='ISIN_NO')

		#인덱스가격 조회
		self._table_index = \
			self._dbRMS01.query(
					sql=query.index_sql,
					bind={'date_from': '2014-01-01'},
					index_col='STD_DATE')

		#기본정보 테이블 3세트 불러오기
		self._table_basic = [None, None, None]
		self._table_name = ['els_market_basic_info',
							'els_market_issue_info',
							'els_market_exercise_info']

		for index, item in enumerate(self._table_basic):
			#테이블 기본정보조회
			table = \
				self._dbMYSQL.select_table(
						table_name=self._table_name[index],
						index_col='ISIN_NO')
			
			#발행회사 정보추가
			table['COMP'] = \
				table.index.str[3:5].astype(int).map(self._get_cust_name)

			#기초자산정보 조인
			table = \
				table.join(
						other=self._table_asset,
						how='inner')

			#지수레벨정보 조인, 기준가격에 대해서 조인
			table = \
				pd.merge(
						left=table,
						right=self._table_index,
						how='left',
						left_on=['EFF_DATE'],
						right_index=True)

			#상환구조정보조인
			table = \
				pd.merge(
						left=table,
						right=self._table_struct,
						how='left',
						left_index=True,
						right_index=True)

			#날짜에 해당하는 컬럼들마다 매월말일에 해당하는 컬럼추가
			for col in ['STD_DATE', 'EFF_DATE', 'MAT_DATE']:
				_get_date_monthly(table, col)

			#계산결과저장
			self._table_basic[index] = table


	'''접근자'''
	@property
	def dbRMS01(self):
		return self._dbRMS01

	@property
	def dbMYSQL(self):
		return self._dbMYSQL

	@property
	def eval_date(self):
		return self._eval_date

	@property
	def last_date(self):
		return self._last_date

	@property
	def fr_this_year(self):
		return self._fr_this_year

	@property
	def to_this_year(self):
		return self._to_this_year

	@property
	def fr_full_year(self):
		return self._fr_full_year

	@property
	def to_full_year(self):
		return self._to_full_year

	@dbRMS01.setter
	def dbRMS01(self, value):
		raise Exception("ERROR _ private member")

	@dbMYSQL.setter
	def dbMYSQL(self, value):
		raise Exception("ERROR _ private member")

	@eval_date.setter
	def eval_date(self, value):
		raise Exception("ERROR _ private member")

	@last_date.setter
	def last_date(self, value):
		raise Exception("ERROR _ private member")

	@fr_this_year.setter
	def fr_this_year(self, value):
		raise Exception("ERROR _ private member")

	@to_this_year.setter
	def to_this_year(self, value):
		raise Exception("ERROR _ private member")

	@fr_full_year.setter
	def fr_full_year(self, value):
		raise Exception("ERROR _ private member")

	@to_full_year.setter
	def to_full_year(self, value):
		raise Exception("ERROR _ private member")


	'''클래스 함수'''
	def _get_cust_name(self, cust_no):
		"""회사코드를 회사명으로 변경

		:param self:
		:param cust_no:
		:return:
		"""
		rtn_value = self._SPEC_COMP.get(cust_no, 'ALL')
		return rtn_value if rtn_value in self._LIST_NAME_BIG_COMP else 'ALL'

	def transfer_basic_info(self, oper_name:str) -> pd.DataFrame:
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
		#작년말 기준으로 활성화되어 있는 상품에 대해서만
		if oper_name == 'listInit':
			table_name = 'els_market_basic_info'
			bind_param = \
				{'eval_date': self._fr_this_year,
				 'prsv_rate': '1000', 									#원금 보장여부: 관계없음
				 'req_amt_min': '0',									#발행잔액 남아있는 경우만
				 'req_amt_max': '100000000000',
				 'fr_std_date': self._fr_full_year,
				 'fr_eff_date': self._fr_full_year,						#발행일자 아주 먼 과거에서부터, 작년말까지
				 'to_eff_date': self._fr_this_year,
				 'fr_exp_date': self._fr_this_year,						#작년 말에서부터, 아주 먼 미래까지
				 'to_exp_date': self._to_full_year}

		#특정기간동안 발행되었던 전체 상품에 대해서
		elif oper_name == 'listIssue':
			table_name = 'els_market_issue_info'
			bind_param = \
				{'eval_date': self._eval_date,
				 'prsv_rate': '1000', 									#원금 보장여부: 관계없음
				 'req_amt_min': '-1',									#발행잔액: 관계없음
				 'req_amt_max': '100000000000',
				 'fr_std_date': self._fr_this_year,						#최근변동일자: 발행이 올해초에 되었다면, 적어도 올해초는 넘어야함
				 'fr_eff_date': self._fr_this_year,						#최초발행일자: 올해초에서부터 조회기준일까지
				 'to_eff_date': self._eval_date,
				 'fr_exp_date': self._fr_full_year,						#만기상환일자: 관계없음
				 'to_exp_date': self._to_full_year}

		#특정기간동안 상환되었던 전체 상품에 대해서
		elif oper_name == 'listExercise':
			table_name = 'els_market_exercise_info'
			bind_param = \
				{'eval_date': self._eval_date,
				 'prsv_rate': '1000', 									#원금 보장여부: 관계없음
				 'req_amt_min': '-1',									#발행잔액: 0인것만 조회
				 'req_amt_max': '1',
				 'fr_std_date': self._fr_this_year,						#최근변동일자: 만기일자가 될텐데. 올해초는 넘어야함
				 'fr_eff_date': self._fr_full_year,						#최초발행일자: 아주먼과거에서부터, 조회기준일 이전까지
				 'to_eff_date': self._eval_date,
				 'fr_exp_date': self._eval_date,						#만기상환일자: 조회기준일부터, 아주먼미래까지
				 'to_exp_date': self._to_full_year}
		else:
			raise Exception('ERROR _ invalid operation name')

		#기본정보조회
		rtn_basic = \
			self._dbRMS01.query(
					sql=query.basic_sql,
					bind=bind_param,
					index_col='ISIN_NO')

		#데이터프레임 변수를 데이터베이스에 인서트
		self._dbMYSQL.insert_table(
				table_name=table_name,
				data_frame=rtn_basic,
				if_exists='replace',
				col_info=query.basic_col)

		#제약조건 설정
		self._dbMYSQL.query(
				sql=query.alter_basic_sql,
				bind={'table_name':table_name})

		#쿼리결과값 반환
		return rtn_basic

	def transfer_underlying_info(self):
		"""기초자산 정보 조회하여 MYSQL에 저장
		- 시장전체 ELS상품들의 기초자산 정보를 조회하여 MYSQL에 저장
		- 최종적으로는 MYSQL데이터베이스에 다시 입력하기 위한 용도로 활용
		- 최근작업일자 이후, 현재작업일자 이전에 발생한 데이터들에 대해서만 처리

		:return: 기초자산정보 데이터프레임 형태로 반환
		"""
		#바인드변수 설정
		bind_param = \
			{'last_date':self._last_date,
			 'eval_date':self._eval_date}

		#기초자산정보조회
		rtn_asset = \
			self._dbRMS01.query(
					sql=query.asset_sql,
					bind=bind_param,
					index_col='ISIN_NO')

		#조회 결과물 데이터베이스에 인서트
		self._dbMYSQL.insert_table(
				table_name='els_market_underlying_info',
				data_frame=rtn_asset,
				if_exists='append',
				col_info=query.asset_col)

		#쿼리결과값 반환
		return rtn_asset

	def transfer_structure_info(self):
		"""상환구조 정보 조회
		- 시장전체 ELS상품들의 상환구조 정보를 조회
		- 최종적으로는 MYSQL데이터베이스에 다시 입력하기 위한 용도로 활용
		- 최근작업일자 이후, 현재작업일자 이전에 발생한 데이터들에 대해서만 처리

		:return: 상환구조 데이터프레임 형태로 반환
		"""
		#바인드변수 설정
		bind_param = \
			{'last_date': self._last_date,
			 'eval_date': self._eval_date}
				#{'last_date':'2017-07-01',
			# 'eval_date':'2017-09-04'}

		#상환구조정보조회
		rtn_asset = \
			self._dbRMS01.query(
					sql=query.exer_sql,
					bind=bind_param,
					index_col=None)

		#조회 결과물 데이터베이스에 인서트
		self._dbMYSQL.insert_table(
				table_name='els_market_structure_info',
				data_frame=rtn_asset,
				if_exists='append',
				col_info=query.exer_col,
				index=False)

		#쿼리결과값 반환
		return rtn_asset

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

	def _get_last_oper_date(self) -> str:
		"""가장 최근 작업일자 조회
		"""
		last_date = \
			self._dbMYSQL.query(sql=query.hist_select_sql)\
				.iat[0,0]

		return '{:%Y-%m-%d}'.format(last_date)

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

	def _get_mask_valid(self, data_frame:pd.DataFrame):
		"""해당 데이터프레임 항목들 중에서 유의미한 값들에 대해서만 마스크작성

		:param data_frame: 데이터프레임
		:return: 데이터프레임 마스크 반환
		"""
		return data_frame['STD_DATE_EXER'].isnull()

	def _get_monthly_table_empty(self) -> pd.DataFrame:
		"""월간보고서 작성을 위한 기본 테이블 작성
		"""
		months = pd.date_range(self._fr_this_year, periods=12, freq='M')
		data = months.map(lambda x: datetime.strftime(x, '%Y-%m-%d'))
		index = months.map(lambda x: datetime.strftime(x, '%y-%m'))
		return pd.DataFrame(data=data, index=index, columns=['EOMONTH'])

	def create_performance_column(self, table:pd.DataFrame, target_asset:str=None, ratio_mult_in:float=1.00):
		"""테이블에 기초자산 퍼포먼스 컬럼추가
		- LVL_ASTX 컬럼 추가됨: X번째 기초자산의 현재가/기준가 수준 기록
		- WORST_LVL 컬럼 추가됨: LVL_ASTX중에서 가장 작은 숫자 기록
		- WORST_AST 컬럼 추가됨: WORST_LVL을 기록한 기초자산의 이름 기록

		:param table: 컬럼을 추가하고자 하는 테이블
		:param target_asset: 지수 BUMP를 반영하고자 하는 기초자산 이름
		:param ratio_mult_in: 지수 BUMP비율
		:return: 없음
		"""
		#지수레벨정보로부터 퍼포먼스(최초기준가대비 현재지수레벨) 계산
		for idx in range(1, self._MAX_DIM+1):
			#최근일자 기준으로 계산
			latest_level = self._table_index.loc[self._eval_date]
			name_str = 'NAME_AST' + str(idx)
			ratio_mult = 1.0 if target_asset != name_str else ratio_mult_in

			table['LVL_AST' + str(idx)] = \
				table.apply(
						lambda row: np.NaN if row[name_str] not in self._LIST_INDEX else \
									ratio_mult*latest_level[row[name_str]]/row[row[name_str]], axis=1)

		#워스트퍼포머 계산
		col_lvl = ['LVL_AST' + str(x) for x in range(1, self._MAX_DIM+1)]
		col_name = ['NAME_AST' + str(x) for x in range(1, self._MAX_DIM+1)]
		col_name.insert(0, 'WORST_AST')

		table['WORST_LVL'] = table.loc[:, col_lvl].min(axis=1)
		table['WORST_AST'] = table.loc[:, col_lvl].idxmin(axis=1)
		table['WORST_AST'] = \
			table[col_name].apply(
				lambda row: np.NaN if row['WORST_AST'] is np.NaN else \
							row['NAME_AST' + row['WORST_AST'][-1]], axis=1)
		#table['WORST_LVL'] = int(table['WORST_LVL']/0.05)*0.05

	def get_table_basic(self, table_name:str, date:str=None, asset:str=None, comp:str=None, prsv:float=None, order:int=0):
		"""특정 기초자산을 포함하는, 연초기준 활성화 목록 반환

		:param asset: 해당 기초자산을 포함하는 항목에 대해서만
		:return: 연초기준 활성화목록
		"""
		if table_name == 'INITIAL':
			table = self._table_basic[0]
		elif table_name == 'ISSUE':
			table = self._table_basic[1]
		elif table_name == 'EXERCISE':
			table = self._table_basic[2]
		else:
			raise Exception

		for col in ('FIRST_AMT','REMAIN_AMT'):
			table[col] = round(table[col]/(10**order))
			table[col] = table[col].astype(int)

		if date is not None:
			mask = self._get_mask_date(table, date)
			table = table[mask]

		if asset is not None:
			mask = self._get_mask_asset(table,asset)
			table = table[mask]

		if comp is not None:
			table = table[table['COMP']==comp]

		if prsv is not None:
			table = table[table['PRSV_RATE']<=prsv]

		return table

	def get_active_list(self, date:str=None, asset:str=None, comp:str=None, prsv:float=100.0, order:int=0) -> pd.DataFrame:
		"""기준일자 시점에서 활성화 목록 반환

		:param date_ref: 기준일자 입력
		:return: 기준일자 시점의 활성화 목록
		"""
		#기본테이블 조회
		table_initial = self.get_table_basic('INITIAL',date, asset, comp, prsv, order)
		table_issue = self.get_table_basic('ISSUE', date, asset, comp, prsv, order)
		table_exercise = self.get_table_basic('EXERCISE',date, asset, comp, prsv, order)

		#작년말기준 활성화목록에 올해 발행목록 결합
		rtn_table = \
			pd.concat(
					objs=[table_initial, table_issue],
					join='outer')

		#올해 상환목록도 결합
		rtn_table = \
			rtn_table.join(
					other=table_exercise,
					how='left',
					rsuffix='_EXER')

		#기본테이블정보가 유효하고 상환테이블정보가 없는 경우만
		rtn_table = rtn_table[self._get_mask_valid(rtn_table)]

		#상환목록은 더이상 필요하지 않으므로 해당컬럼 제거
		rtn_table.drop([col for col in rtn_table.columns if '_EXER' in col],axis=1,inplace=True)

		#결과값 반환
		return rtn_table

	def set_monthly_figure(self):
		dict_grp_fig = {}
		for idx_asset, item_asset in enumerate(self._LIST_INDEX):
			for idx_prsv, item_prsv in enumerate(['YES', 'NO']):
				grp_fig = self.draw_monthly_figure(item_asset, item_prsv)
				key = item_asset + item_prsv
				dict_grp_fig.update({key:grp_fig})
		return dict_grp_fig

	def get_monthly_report(self, asset:str=None, comp:str=None, prsv:float=100.0, order:int=0):
		"""특정 기초자산을 포함하는, 월간합계 보고서 반환

		:param asset: 해당 기초자산을 포함하는 경우에 대해서만
		:return: 월간합계 보고서
		"""
		#발행정보, 상환정보 조회
		table_issue = self.get_table_basic('ISSUE' ,None, asset, comp, prsv, order)
		table_exercise = self.get_table_basic('EXERCISE', None, asset, comp, prsv, order)

		#발행정보 합계계산. EFF_DATE기준으로 합산한다는 것에 주의
		table_issue_monthly = \
			table_issue\
				.groupby('EFF_DATE_MONTHLY')['FIRST_AMT']\
				.agg(['sum'])\
				.rename(columns={'sum':'ISSUE'})

		#상환정보 합계계산. STD_DATE기준으로 합산한다는 것에 주의
		table_exercise_monthly = \
			table_exercise\
				.groupby('STD_DATE_MONTHLY')['FIRST_AMT']\
				.agg(['sum'])\
				.rename(columns={'sum':'EXERCISE'})

		#월별 합계정보 테이블 준비
		rtn_table = self._get_monthly_table_empty()

		#발행정보, 상환정보를 월별 합계정보에 결합
		rtn_table = rtn_table.join(table_issue_monthly)
		rtn_table = rtn_table.join(table_exercise_monthly)
		rtn_table.fillna(0, inplace=True)

		#발행잔액 계산하여 결합
		rtn_table['ACTIVE'] = \
			rtn_table\
				.apply(lambda row:self.get_active_list(row['EOMONTH'], asset, comp, prsv, order)['FIRST_AMT'].sum(), axis=1)

		#인텍스를 JAN, FEB 형식으로 변경
		rtn_table.index = rtn_table.index.map(lambda x: calendar.month_abbr[int(x[3:5])])

		#상환금액 정보의 부호를 반대로 변경
		rtn_table[['ISSUE','EXERCISE','ACTIVE']]=rtn_table[['ISSUE','EXERCISE','ACTIVE']].astype(int)
		rtn_table['EXERCISE'] = rtn_table['EXERCISE'].map(lambda x:x*-1)

		#결과값 반환
		return rtn_table

	def get_exercise_report(self, asset:str=None):
		"""특정 기초자산을 포함하는, 월간상환 보고서 반환

		:param asset: 해당 기초자산을 포함하는 경우에 대해서만
		:return: 월간상환 보고서
		"""
		#월간발행보고서 기본세팅
		num_days = 20
		date_index = pd.date_range(date.today(),periods=num_days*2, freq='D')
		rtn_table = pd.DataFrame(index=date_index)
		rtn_table = rtn_table[rtn_table.index.weekday<5][0:num_days]

		#해당기초자산 포함하는 현재활성화 리스트
		table = self.get_active_list(None, asset, None, 0.0, 0)

		#기초자산 BUMP스케줄에 따라서
		for idx_bump, item_bump in enumerate(self._LIST_INDEX_BUMP):
			#BUMP스케줄에 따라서 퍼포먼스 기록
			self.create_performance_column(table, asset, item_bump)

			#상환보고서 포함기준
			mask = (table['EXE_DATE'] <= date_index[-1]) & \
				   (table['WORST_LVL'] >= item_bump) & \
				   (table['WORST_LVL'] < item_bump+self._INDEX_BUMP_STEP)

			#상환보고서 집계
			col_name = str(int(round(item_bump*100,0)))+"≤X<"+str(int(round((self._INDEX_BUMP_STEP+item_bump)*100,0)))

			sum_table = \
				table[mask]\
					.groupby('EXE_DATE')['REMAIN_AMT']\
					.agg(['sum'])\
					.rename(columns={'sum':col_name})

			#BUMP스케줄에 따라서 결합
			rtn_table =\
				pd.merge(
						left=rtn_table,
						right=sum_table,
						how='left',
						left_index=True,
						right_index=True)

		#결과값 반환
		return rtn_table

	def draw_monthly_figure(self, asset:str=None, prsv:str=None):
		"""월간합계보고서 출력
		- get_monthly_report에서 가져오는 데이터프레임을 발행사별로 출력
		- 순서대로, 발행액, 상환액, 잔존금액

		:return: 그래프, 축 반환
		"""
		#그래프 제목설정
		title = ['월별발행금액(단위:조)\n기초자산:{asset}, ELB포함:{prsv}',
				 '월별상환금액(단위:조)\n기초자산:{asset}, ELB포함:{prsv}',
				 '월별발행잔액(단위:조)\n기초자산:{asset}, ELB포함:{prsv}']
		title = [item.replace('{asset}',asset) for item in title]
		title = [item.replace('{prsv}', prsv) for item in title]

		#입력변수 필터링
		asset = None if asset == 'ALL' else asset
		prsv = 100.0 if prsv == 'YES' else 0.0

		#그래프 데이터 초기화
		fig, (ax_issue, ax_exercise, ax_active) = \
			plt.subplots(3, 1, figsize=(25, 20), sharex=True)
		grp_fig = [ax_issue, ax_exercise, ax_active, fig]

		#집계금액에 따라 순환
		for idx_col, item_col in enumerate(['ISSUE', 'EXERCISE', 'ACTIVE']):

			#더미테이블
			tbl = self.get_monthly_report(asset, 'ALL', prsv)
			margin_bottom = np.zeros(len(tbl.index))

			#발행사에 따라 순환
			for idx_comp, item_comp in enumerate(self._LIST_NAME_BIG_COMP):
				#발행사에 따라 월간보고서 집계 및 그래프출력
				tbl = self.get_monthly_report(asset, item_comp, prsv)
				values = list(tbl.loc[:,item_col])

				tbl.plot.bar(
						x=tbl.index,
						y=item_col,
						ax=grp_fig[idx_col],
						color=self._LIST_COLOR_BIG_COMP[idx_comp],
						stacked=True,
						bottom=margin_bottom,
						label=item_comp)
				margin_bottom += values

		#결과값 반환
		return grp_fig

	def draw_exercise_figure(self, asset:str=None):

		#그래프 제목설정
		title = ['테스트']

		#입력변수 필터링
		asset = None if asset == 'ALL' else asset





'''메인함수'''
def main():
	#시장분포 객체생성
	distMarket = MarketDist()
	distMarket.set_monthly_figure()
	'''
	#발행내역 조회 및 기록
	listIssue = distMarket.transfer_basic_info('listIssue')
	print('발행정보가 입력되었습니다.')
	print('입력된 데이터 개수: ' + str(len(listIssue.index)) + '\n')
	#print(listActive)

	#기본정보 조회 및 기록
	listActive = distMarket.transfer_basic_info('listInit')
	print('기본정보가 입력되었습니다.')
	print('입력된 데이터 개수: ' + str(len(listActive.index)) + '\n')
	#print(listActive)

	#상환내역 조회 및 기록
	listExercise = distMarket.transfer_basic_info('listExercise')
	print('상환정보가 입력되었습니다.')
	print('입력된 데이터 개수: ' + str(len(listExercise.index)) + '\n')
	#print(listActive)
	
	#오늘 작업 내역 존재하지 않는 경우에만 작업
	if distMarket.eval_date != distMarket.last_date:

		#시장분포 기초자산 정보조회
		listUnderlying = distMarket.transfer_underlying_info()
		print('기초자산정보가 입력되었습니다.')
		print('입력된 데이터 개수: ' + str(len(listUnderlying.index)) + '\n')
		#print(listUnderlying)
	
		#시장분포 상환구조 정보조회
		listExercise = distMarket.transfer_structure_info()
		print('구조정보가 입력되었습니다.')
		print('입력된 데이터 개수: ' + str(len(listExercise['ISIN_NO'])) + '\n')
		##print(listExercise)

	#작업일자 기록
	distMarket._delete_log()
	distMarket._create_log()
	print('작업일자가 기록되었습니다.')
	print(distMarket.eval_date + '\n')
	'''
	#print(distMarket.get_monthly_report())
	#print(distMarket.get_monthly_report('SXE'))
	#print(distMarket.get_monthly_report('HSC'))
	#print(distMarket.get_monthly_report('HSI'))


'''스트립트 파일 직접 수행되는 경우에만 실행'''
if __name__ == '__main__':
	main()



