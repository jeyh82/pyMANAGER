"""ELS 시장분포 모듈
:filename:          - els_market_dist.py
:modified:          - 2017.08.17
:note:              - 시장전체 ELS 분포에 대해서 조사 및 집계

"""


'''모듈 불러오기'''
import numpy as np									#NUMPY
import pandas as pd									#PANDAS
import auxiliary.database as database				#데이터베이스 모듈 불러오기
import auxiliary.query as query						#쿼리 모듈 불러오기
import calendar										#달력작업
from datetime import date, datetime, timedelta		#날짜작업

#pd.options.display.float_format = '{:,0f}'.format

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

def _get_cust_name(cust_no):
	"""회사코드를 회사명으로 변경

	:param self:
	:param cust_no:
	:return:
	"""
	return {50: '신한', 	51: '대신', 	52: '미래', 	59: '미래', 	53: '삼성',
			54: '신영', 	55: 'NH', 	69: 'NH', 	57: '한투', 	58: 'KB',
			77: 'KB', 	60: '맥쿼', 	61: '메리', 	62: '교보', 	64: '유진',
			65: '한화', 	66: '유안', 	67: '동부', 	68: 'SK', 	70: '하나',
			72: 'IBK', 	73: 'HMC', 	74: 'SC', 	75: '노무', 	76: '키움',
			78: 'BNP', 	79: '하이'
			}.get(cust_no, '--')


'''클래스'''
class MarketDist(object):
	"""ELS 시장분포 클래스 정의
	"""

	'''클래스 생성자 및 소멸자'''
	def __init__(self):
		"""클래스 생성자: 필요한 데이터베이스 연결 및 날짜정보 설정
		"""
		self._dbRMS01 = database.Connection('spt','RMS01')
		self._dbMYSQL = database.Connection('python','TESTDB')

		self._eval_date = date.today().strftime('%Y-%m-%d')
		self._last_date = self._get_last_oper_date()

		self._fr_this_year = self._eval_date[:4] + '-01-01'
		self._to_this_year = self._eval_date[:4] + '-12-31'

		self._fr_full_year = '2000-01-01'
		self._to_full_year = '2100-12-31'

		#기초자산 테이블 불러오기
		self._table_asset = \
			self._dbMYSQL.select_table(
					table_name='view_els_market_underlying_info',
					index_col='ISIN_NO')

		#기본정보 테이블 3세트 불러오기
		self._table_basic = [None, None, None]
		self._table_name = ['els_market_basic_info',
							'els_market_issue_info',
							'els_market_exercise_info']

		for index, item in enumerate(self._table_basic):

			table = \
				self._dbMYSQL.select_table(
						table_name=self._table_name[index],
						index_col='ISIN_NO')
			table = \
				table.join(
						other=self._table_asset,
						how='inner')
			table = \
				table.query('NAME_AST1 != None')

			for col in ['STD_DATE', 'EFF_DATE', 'MAT_DATE']:
				_get_date_monthly(table, col)

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

	@property
	def table_basic(self):
		return self._table_basic

	@property
	def table_issue(self):
		return self._table_issue

	@property
	def table_exer(self):
		return self._table_exer

	@property
	def table_asset(self):
		return self._table_asset

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

	@table_basic.setter
	def table_basic(self, value):
		raise Exception("ERROR _ private member")

	@table_issue.setter
	def table_issue(self, value):
		raise Exception("ERROR _ private member")

	@table_exer.setter
	def table_exer(self, value):
		raise Exception("ERROR _ private member")

	@table_asset.setter
	def table_asset(self, value):
		raise Exception("ERROR _ private member")


	'''클래스 함수'''
	def load_info_basic(self, oper_name:str) -> pd.DataFrame:
		"""기본정보 조회
		- 시장전체 ELS상품들의 기본정보를 조회
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
				 'prsv_rate': '0', 										#원금 보장여부: 원금 비보장형의 경우만
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
				 'prsv_rate': '0', 										#원금 보장여부: 원금 비보장형의 경우만
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
				 'prsv_rate': '0', 										#원금 보장여부: 원금 비보장형의 경우만
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
		rtn_basic['CUST_NO'] = \
			rtn_basic['CUST_NO'].map(_get_cust_name)

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

	def load_info_underlying(self):
		"""기초자산 정보 조회
		- 시장전체 ELS상품들의 기초자산 정보를 조회
		- 최종적으로는 MYSQL데이터베이스에 다시 입력하기 위한 용도로 활용
		- 최근작업일자 이후, 현재작업일자 이전에 발생한 데이터들에 대해서만 처리리

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
			
		#기본정보조회 결과물 데이터베이스에 인서트
		self._dbMYSQL.insert_table(
				table_name='els_market_underlying_info',
				data_frame=rtn_asset,
				if_exists='append',
				col_info=query.asset_col)

		#쿼리결과값 반환
		return rtn_asset

	def load_info_structure(self):

		bind_param = \
			{'last_date':self._last_date,
			 'eval_date':self._eval_date}

		#for idx, item in enumerate(query.exer_type):
		#	bind_param.update({'oper_num'+str(idx+1):item[oper]})

		rtn_asset = \
			self._dbRMS01.query(
					sql=query.exer_sql,
					bind=bind_param,
					index_col=None)

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
		return (~data_frame['NAME_AST1'].isnull()) & \
			   (data_frame['NAME_AST1_EXER'].isnull())

	def _get_monthly_table_empty(self) -> pd.DataFrame:
		"""월간보고서 작성을 위한 기본 테이블 작성
		"""
		months = pd.date_range(self._fr_this_year, periods=int(self._eval_date[5:7]), freq='M')
		data = months.map(lambda x: datetime.strftime(x, '%Y-%m-%d'))
		index = months.map(lambda x: datetime.strftime(x, '%y-%m'))
		return pd.DataFrame(data=data, index=index, columns=['EOMONTH'])

	def get_table_initial(self, asset:str=None):
		"""특정 기초자산을 포함하는, 연초기준 활성화 목록 반환

		:param asset: 해당 기초자산을 포함하는 항목에 대해서만
		:return: 연초기준 활성화목록
		"""
		table = self._table_basic[0]
		if asset is not None:
			mask = self._get_mask_asset(table,asset)
			table = table[mask]
		return table

	def get_table_issue(self, asset:str=None):
		"""특정 기초자산을 포함하는, 올해 발행 목록 반환

		:param asset: 해당 기초자산을 포함하는 항목에 대해서만
		:return: 발행목록
		"""
		table = self._table_basic[1]
		if asset is not None:
			mask = self._get_mask_asset(table, asset)
			table = table[mask]
		return table

	def get_table_exercise(self, asset:str=None):
		"""특정 기초자산을 포함하는, 올해 상환 목록 반환

		:param asset: 해당 기초자산을 포함하는 항목에 대해서만
		:return: 상환목록
		"""
		table = self._table_basic[2]
		if asset is not None:
			mask = self._get_mask_asset(table, asset)
			table = table[mask]
		return table

	def get_active_list(self, date:str=None, asset:str=None) -> pd.DataFrame:
		"""기준일자 시점에서 활성화 목록 반환

		:param date_ref: 기준일자 입력
		:return: 기준일자 시점의 활성화 목록
		"""
		#작년말기준 활성화목록에 올해 발행목록 결합
		table_initial = self.get_table_initial()
		table_issue = self.get_table_issue()
		table_exercise = self.get_table_exercise()

		#데이터프레임 마스크 설정
		if date is not None:
			mask = self._get_mask_date(table_exercise, date)
			table_exercise = table_exercise[mask]

		if asset is not None:
			mask = self._get_mask_asset(table_initial, asset)
			table_initial = table_initial[mask]

			mask = self._get_mask_asset(table_issue, asset)
			table_issue = table_issue[mask]

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

		#결과값 반환
		mask = self._get_mask_valid(rtn_table)
		return rtn_table[mask]

	def get_monthly_report(self, asset:str=None, order:int=0):
		"""특정 기초자산을 포함하는, 월간합계 보고서 반환

		:param asset: 해당 기초자산을 포함하는 경우에 대해서만
		:return: 월간합계 보고서
		"""
		table_initial = self.get_table_initial(asset)
		table_issue = self.get_table_issue(asset)
		table_exercise = self.get_table_exercise(asset)

		table_issue_monthly = \
			table_issue\
				.groupby('STD_DATE_MONTHLY')['FIRST_AMT']\
				.agg(['sum'])\
				.rename(columns={'sum':'ISSUE'})

		table_exercise_monthly = \
			table_exercise\
				.groupby('STD_DATE_MONTHLY')['FIRST_AMT']\
				.agg(['sum'])\
				.rename(columns={'sum':'EXERCISE'})

		rtn_table = self._get_monthly_table_empty()

		rtn_table = rtn_table.join(table_issue_monthly)
		rtn_table = rtn_table.join(table_exercise_monthly)

		rtn_table['ACTIVE'] = \
			rtn_table\
				.apply(lambda row:self.get_active_list(row['EOMONTH'], asset)['REMAIN_AMT'].sum(), axis=1)

		for col in ('ISSUE','EXERCISE','ACTIVE'):
			mask_null = rtn_table[col].isnull()
			rtn_table.loc[mask_null, [col]] = 0.0
			rtn_table[col] = rtn_table[col]/(10**order)
			rtn_table[col] = rtn_table[col].astype(int)

		rtn_table.index = rtn_table.index.map(lambda x: calendar.month_abbr[int(x[3:5])])
		rtn_table['EXERCISE'] = rtn_table['EXERCISE'].map(lambda x:x*-1)

		return rtn_table


'''메인함수'''
def main():
	#시장분포 객체생성
	distMarket = MarketDist()
	'''
	#기본정보 조회 및 기록
	listActive = distMarket.load_info_basic('listInit')
	print('기본정보가 입력되었습니다.')
	print('입력된 데이터 개수: ' + str(len(listActive.index)) + '\n')
	#print(listActive)

	#발행내역 조회 및 기록
	listIssue = distMarket.load_info_basic('listIssue')
	print('발행정보가 입력되었습니다.')
	print('입력된 데이터 개수: ' + str(len(listIssue.index)) + '\n')
	# print(listActive)

	#상환내역 조회 및 기록
	listExercise = distMarket.load_info_basic('listExercise')
	print('상환정보가 입력되었습니다.')
	print('입력된 데이터 개수: ' + str(len(listExercise.index)) + '\n')
	# print(listActive)

	#오늘 작업 내역 존재하지 않는 경우에만 작업
	if distMarket.eval_date != distMarket.last_date:

		#시장분포 기초자산 정보조회
		listUnderlying = distMarket.load_info_underlying()
		print('기초자산정보가 입력되었습니다.')
		print('입력된 데이터 개수: ' + str(len(listUnderlying.index)) + '\n')
		#print(listUnderlying)

		listExercise = distMarket.load_info_structure()
		print('구조정보가 입력되었습니다.')
		print('입력된 데이터 개수:' + str(len(listExercise['ISIN_NO'])) + '\n')
		#print(listExercise)

	#작업일자 기록
	distMarket._delete_log()
	distMarket._create_log()
	print('작업일자가 기록되었습니다.')
	print(distMarket.eval_date + '\n')
	'''

	distMarket.get_monthly_report()



'''스트립트 파일 직접 수행되는 경우에만 실행'''
if __name__ == '__main__':
	main()



