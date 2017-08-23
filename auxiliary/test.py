# -*- coding: utf-8 -*-

"""데이터베이스 모듈
:filename:          - database.py
:modified:          - 2017.06.21
:note:              - 이 모듈에서는 데이터베이스 클래스를 정의함
					- 데이터베이스 업무에 필요한 쿼리 클래스도 정의함
					- ORACLE, MYSQL 관계없이 하나의 클래스에서 관리
:todo:              - SQLITE 의 경우도 처리할 수 있도록 반영

"""

'''모듈 불러오기'''
import pandas as pd                     #PANDAS
#import cx_Oracle as oracle              #ORACLE
#import mysql.connector as mysql         #MYSQL


'''클래스'''
class Query(object):
	"""쿼리 클래스 정의"""

	'''클래스 생성자 및 소멸자'''
	def __init__(self, sql:str, col:tuple=()):
		"""클래스 생성자
		- 입력받은 쿼리문, 인덱스, 바인드변수를 맴버변수로 설정

		:param sql: 쿼리문
		:param col: pandas.DataFrame 에서 사용하게 될 컬럼 이름
		:var sql: = param sql
		:var col: = param col
		:var index_col: pandas.DataFrame 에서 인덱스로 사용하게 될 컬럼 이름
		:Example:

			>>> sql = '.....'
			>>> col = ('TRADE_DATE','PX_LAST')
			>>> bind = {'tdate':'20170101','ticker':'HSCEI'}
			>>> ql_px_last = Query(sql, col)
			>>> ql_px_last.set_bind(bind)

		"""

		#멤버변수 설정
		self.sql = sql
		self.col = col
		self.index_col = col[0]				#첫번째 컬럼을 인덱스로 설정

	'''출력 포멧 설정'''
	def __format__(self, format_spec):
		"""출력 포멧 설정
		- 포멧스펙에 따라서 출력값 결정

		:param format_spec: 출력할 데이터와 포멧을 결정
		:raises Exception: 사전에 정의된 스펙변수가 없으면 예외발생
		:return: 포멧에 맞는 문자열 출력
		"""
		if (format_spec == 'sql'):
			return self.sql
		elif (format_spec == 'index_col'):
			return self.index_col
		elif (format_spec == 'col'):
			return self.col
		else:
			raise Exception('ERROR _ invalid format name')

	'''쿼리문 바인드변수 설정'''
	def set_bind(self, bind:dict):
		"""바인드변수 설정
		- 외부에서 바인드변수 입력받아서 클래스 내부의 쿼리문에 바인드

		:param bind: 외부에서 입력받는 바인드 변수

		"""
		self.sql = self.sql.format(**bind)



'''메인함수'''
def main():
	#데이터베이스 설정


	#쿼리 설정
	sql = \
		'''	SELECT TO_DATE(TRADE_DATE,'yyyymmdd') TRADE_DATE, PX_LAST
			FROM spt.BL_DATA
			WHERE TRADE_DATE='{trade_date}' AND TICKER='{ticker}'
		'''
	col = ('TRADE_DATE','PX_LAST')
	bind = {'trade_date': '20170531', 'ticker': 'HSCEI'}
	qrTEST = Query(sql, col)
	qrTEST.set_bind(bind)




'''스크립트 파일 직접 수행되는 경우에만 실행'''
#if __name__ == '__main__':
#	main()


'''쿼리 인스턴스 임포트'''
# MarketDist 클래스를 위한 쿼리
# 기본정보조회
_prod_info_col = \
	('ISIN_NO', 'CUST_NO', 'FIRST_AMT', 'REMAIN_AMT', 'STD_DATE', 'EFF_DATE', 'MAT_DATE')
_prod_info_sql = \
	(
		"select tblLATEST.ISIN_NO, "  													# ISIN번호
		"		tblLATEST.CUST_NO, "  													# 발행사번호
		"		tblLATEST.FIRST_AMT, "  												# 최초발행금액
		"		greatest(tblLATEST.FIRST_AMT-nvl(tblREFUND.REFUND,0),0) REMAIN_AMT, "  	# 현재발행잔액
		"		greatest(tblLATEST.STND_DATE, tblREFUND.STND_DATE) STD_DATE, "			# 처리일자
		"		tblLATEST.EFF_DATE, "  													# 설정일자
		"		tblLATEST.MAT_DATE "  													# 만기일자
		"from "
		"( "
		"	select 	ISIN_NO, "
		"			max(STND_DATE) STND_DATE, "
		"			to_number(SUBSTR(MAX(CUST_NO),-2),'99') CUST_NO, "
		"			max(FIRST_ISSUE_AMT) FIRST_AMT, "
		"			to_date(max(STD_ESTIM_T_DATE),'yyyymmdd') EFF_DATE, "
		"			to_date(max(XPIR_ESTIM_T_DATE),'yyyymmdd') MAT_DATE "
		"	from 	INFO_KSD_ISSUE_LIST "
		"	where 	STND_DATE<='{eval_date}' AND "
		"			SECURITY_TYPE=41 AND "  											# ELS에 대해서만 조회
		"			STD_ESTIM_T_DATE>='{fr_eff_date}' AND " 		 					# 설정일자 조건
		"			STD_ESTIM_T_DATE<='{to_eff_date}' AND "  							# 설정일자 조건
		"			XPIR_ESTIM_T_DATE>='{fr_exp_date}' AND "  							# 만기일자 조건
		"			XPIR_ESTIM_T_DATE<='{to_exp_date}' AND "  							# 만기일자 조건
		"			PRSV_RATE<={prsv_rate} "  											# 원금보장비율 조건
		"	group by ISIN_NO "
		"	having min(ISSUE_AMT)>{req_amt} " 	 										# 현재발행잔액 조건
		") tblLATEST "  																# tblLATEST 최근발행정보
		"left outer join "
		"( "
		"	select 	ISIN_NO, "
		"			max(STND_DATE) STND_DATE, "
		"			sum(REFUND_QTY) REFUND "
		"	from 	INFO_KSD_REFUND "
		"	where 	STND_DATE<='{eval_date}' AND "
		"			SEC_TYPE IN ('1','3') "
		"	group by ISIN_NO "
		") tblREFUND "  																# tblREFUND 환매정보
		"on tblREFUND.ISIN_NO=tblLATEST.ISIN_NO "
		"where 	greatest(tblLATEST.FIRST_AMT-nvl(tblREFUND.REFUND,0),0)>{req_amt} AND "	# 현재발행잔액 조건
		"		greatest(tblLATEST.STND_DATE, tblREFUND.STND_DATE)>{fr_std_date} "
		"order by tblLATEST.EFF_DATE asc "
	)
#기본정보 쿼리객체 생성
query_basic_info = Query(_prod_info_sql, _prod_info_col)
