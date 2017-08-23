# -*- coding: utf-8 -*-

"""ELS 시장분포 모듈
:filename:          - els_market_dist.py
:modified:          - 2017.08.17
:note:              - 시장전체 ELS 분포에 대해서 조사 및 집계

"""

'''모듈 불러오기'''
import pandas as pd							#PANDAS
import auxiliary.database as db				#데이터베이스 모듈 불러오기
from datetime import date, timedelta		#날짜작업
import copy

'''클래스 보조함수'''
def get_cust_name(cust_no):
	return {50: '신한',
			51: '대신',
			52: '미래',
			59: '미래',
			53: '삼성',
			54: '신영',
			55: 'NH',
			69: 'NH',
			57: '한투',
			58: 'KB',
			77: 'KB',
			60: '맥쿼',
			61: '메리',
			62: '교보',
			64: '유진',
			65: '한화',
			66: '유안',
			67: '동부',
			68: 'SK',
			70: '하나',
			72: 'IBK',
			73: 'HMC',
			74: 'SC',
			75: '노무',
			76: '키움',
			78: 'BNP',
			79: '하이'
			}.get(cust_no, '--')

'''클래스'''
class MarketDist(object):
	"""ELS 시장분포 클래스 정의"""

	'''클래스 상수'''


	'''클래스 생성자 및 소멸자'''
	def __init__(self):
		"""클래스 생성자
		- 필요한 데이터베이스 연결
		- 필요한 날짜 변수 생성. 모든 날짜는 YYYY-MM-DD형태

		"""
		self._dbRMS01 = db.Database('spt','RMS01')
		self._dbMYSQL = db.Database('python','TESTDB')
		self._eval_date = date.today().strftime('%Y-%m-%d')
		self._last_date = self._get_last_oper_date()
		self._fr_this_year = self._eval_date[:4] + '-01-01'
		self._to_this_year = self._eval_date[:4] + '-12-31'
		self._fr_full_year = '2000-01-01'
		self._to_full_year = '2100-12-31'

		self.table_basic = pd.read_sql_table(table_name='els_market_basic_info',
											 con=self.dbMYSQL.engine,
											 index_col='ISIN_NO')
		self.table_under = pd.read_sql_table(table_name='view_els_market_underlying_info',
											 con=self.dbMYSQL.engine,
											 index_col='ISIN_NO')



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
	def _load_info_basic(self, oper_name:str) -> pd.DataFrame:
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

		#현재시점에서 활성화되어 있는 상품에 대해서만
		if oper_name == 'activeList':
			name_table = 'els_market_basic_info'
			prod_info_bind = {'eval_date': self._eval_date,
							  'prsv_rate': '0', 						#원금 비보장형의 경우만
							  'req_amt_min': '0',						#발행잔액 남아있는 경우만
							  'req_amt_max': '100000000000',
							  'fr_std_date': self._fr_full_year,
							  'fr_eff_date': self._fr_full_year,		#발행일자 아주 먼 과거에서부터, 조회기준일 이전까지
							  'to_eff_date': self._eval_date,
							  'fr_exp_date': self._eval_date,			#만기일자 조회기준일 이후에서부터, 아주 먼 미래까지
							  'to_exp_date': self._to_full_year}
		#특정기간동안 발행되었던 전체 상품에 대해서
		elif oper_name == 'issueList':
			name_table = 'els_market_issue_info'
			prod_info_bind = {'eval_date': self._eval_date,
							  'prsv_rate': '100', 						#원금 보장여부: 관계없음
							  'req_amt_min': '-1',						#발행잔액: 관계없음
							  'req_amt_max': '100000000000',
							  'fr_std_date': self._fr_this_year,		#최근변동일자: 발행이 올해초에 되었다면, 적어도 올해초는 넘어야함
							  'fr_eff_date': self._fr_this_year,		#최초발행일자: 올해초에서부터 올해말까지
							  'to_eff_date': self._to_this_year,
							  'fr_exp_date': self._fr_full_year,		#만기상환일자: 관계없음
							  'to_exp_date': self._to_full_year}
		#특정기간동안 상환되었던 전체 상품에 대해서
		elif oper_name == 'exerciseList':
			name_table = 'els_market_exercise_info'
			prod_info_bind = {'eval_date': self._eval_date,
							  'prsv_rate': '100', 						#원금 보장여부: 관계없음
							  'req_amt_min': '-1',						#발행잔액: 0인것만 조회
							  'req_amt_max': '1',
							  'fr_std_date': self._fr_this_year,		#최근변동일자: 만기일자가 될텐데. 올해초는 넘어야함
							  'fr_eff_date': self._fr_full_year,		#최초발행일자: 아주먼과거에서부터, 조회기준일 이전까지
							  'to_eff_date': self._eval_date,
							  'fr_exp_date': self._eval_date,			#만기상환일자: 조회기준일부터, 아주먼미래까지
							  'to_exp_date': self._to_full_year}
		else:
			raise Exception('ERROR _ invalid operation name')

		#database 모듈에서 사전에 정의된 쿼리문에 바인드 변수 결합
		tmp_query = copy.deepcopy(db.query_info_basic)
		tmp_query.set_bind(prod_info_bind)

		#기본정보조회
		basic_result = self._dbRMS01.select(tmp_query)
		basic_result['CUST_NO'] = basic_result['CUST_NO'].map(get_cust_name)

		#기본정보조회 결과물 INSERT
		self._dbMYSQL.insert(name_table=name_table,
							 data_frame=basic_result,
							 dtype=tmp_query.dtype,
							 if_exists='replace')

		#기본정보조회 결과 테이블 제약조건 수정
		tmp_query = copy.deepcopy(db.query_alter_table_basic_info)
		tmp_query.set_bind({'name_table':name_table})
		self._dbMYSQL.execute(tmp_query)

		#쿼리결과값 반환
		return basic_result

	def _load_info_underlying(self):
		"""기초자산 정보 조회
		- 시장전체 ELS상품들의 기초자산 정보를 조회
		- 최종적으로는 MYSQL데이터베이스에 다시 입력하기 위한 용도로 활용
		- 최근작업일자 이후, 현재작업일자 이전에 발생한 데이터들에 대해서만 처리리

		:return: 기초자산정보 데이터프레임 형태로 반환

		"""
		#바인드변수 설정
		bind_param = {'last_date':self._last_date,
					  'eval_date':self._eval_date}

		# 바인드변수 결합
		tmp_query = copy.deepcopy(db.query_info_underlying)
		tmp_query.set_bind(bind_param)

		# 기초자산정보조회
		underlying_result = self._dbRMS01.select(tmp_query)

		#기본정보조회 결과물 INSERT
		self._dbMYSQL.insert(name_table='els_market_underlying_info',
							 data_frame=underlying_result,
							 dtype=tmp_query.dtype,
							 if_exists='append')

		#쿼리결과값 반환
		return underlying_result

	def _delete_log(self):
		"""작업일자를 삭제
		:return: 없음

		"""
		#해당일자 로그가 존재하면 삭제
		tmp_query = copy.deepcopy(db.query_oper_hist_delete)
		tmp_query.set_bind({'date_oper': self._eval_date})
		self._dbMYSQL.execute(tmp_query)

	def _create_log(self):
		"""작업일자를 기록으로 남김
		:return: 없음

		"""
		#해당일자 로그 새로 기록
		tmp_query = copy.deepcopy(db.query_oper_hist_insert)
		tmp_query.set_bind({'date_oper': self._eval_date})
		self._dbMYSQL.execute(tmp_query)

	def _get_last_oper_date(self) -> str:
		"""가장 최근 작업일자 조회
		:return: 가장 최근 작업일자

		"""
		last_oper_date = self._dbMYSQL.select(db.query_oper_hist_select)
		last_oper_date = last_oper_date.iloc[0][db.query_oper_hist_select.col[0]]

		#문자열로 결과값 반환
		return '{:%Y-%m-%d}'.format(last_oper_date)


'''메인함수'''
def main():
	#시장분포 객체생성
	distMarket = MarketDist()

	#오늘 작업 내역 존재하지 않는 경우에만 작업
	if distMarket.eval_date != distMarket.last_date:

		#기본정보 조회 및 기록
		listActive = distMarket._load_info_basic('activeList')
		print('기본정보가 입력되었습니다.')
		print('입력된 데이터 개수: ' + str(len(listActive.index)) + '\n')
		#print(listActive)

		#발행내역 조회 및 기록
		listIssue = distMarket._load_info_basic('issueList')
		print('발행정보가 입력되었습니다.')
		print('입력된 데이터 개수: ' + str(len(listIssue.index)) + '\n')
		# print(listActive)

		#상환내역 조회 및 기록
		listExercise = distMarket._load_info_basic('exerciseList')
		print('상환정보가 입력되었습니다.')
		print('입력된 데이터 개수: ' + str(len(listExercise.index)) + '\n')
		# print(listActive)

		#시장분포 기초자산 정보조회
		listUnderlying = distMarket._load_info_underlying()
		print('기초자산정보가 입력되었습니다.')
		print('입력된 데이터 개수: ' + str(len(listUnderlying.index)) + '\n')
		#print(listUnderlying)

		#작업일자 기록
		distMarket._delete_log()
		distMarket._create_log()
		print('작업일자가 기록되었습니다.')
		print(distMarket.eval_date + '\n')


'''스트립트 파일 직접 수행되는 경우에만 실행'''
if __name__ == '__main__':
	main()
