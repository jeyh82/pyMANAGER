"""샘플쿼리 모듈
:filename:          - query.py
:modified:          - 2017.08.17
:note:              - 이 모듈에서는 자주사용하는 샘플쿼리를 미리 정의함

"""


'''모듈 불러오기'''
from sqlalchemy import types						#ALCHEMY for engine


'''쿼리 인스턴스 임포트'''
#기본상품정보조회
	#컬럼 데이터타입
_basic_col = \
	{'ISIN_NO':types.NVARCHAR(length=50),
	 'STD_DATE':types.DateTime(),
	 'CUST_NO':types.NVARCHAR(length=10),
	 'FIRST_AMT':types.BigInteger(),
	 'REMAIN_AMT':types.BigInteger(),
	 'EFF_DATE':types.DateTime(),
	 'MAT_DATE':types.DateTime()}
	#쿼리문
_basic_sql = \
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


#기초자산정보조회
	#컬럼 데이터타입
_asset_col = \
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
_asset_sql = \
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


#작업일자정보삭제
	#쿼리문
_hist_delete_sql = \
	("delete from els_market_oper_hist where OPER_DATE='{date_oper}' ")

#작업일자 정보입력
	#쿼리문
_hist_insert_sql = \
	("insert into els_market_oper_hist (OPER_DATE) values ('{date_oper}') ")

#최근작업일자 정보조회
	#쿼리문
_hist_select_sql = \
	("select max(OPER_DATE) STD_DATE from els_market_oper_hist ")
	#컬럼 데이터타입
_hist_select_col = {'STD_DATE':types.DateTime()}


#기본정보테이블 제약조건 설정
	#쿼리문
_alter_basic_sql = \
	(
		"alter table {name_table} " 
		"change column ISIN_NO ISIN_NO varchar(50) not null, "
		"change column FIRST_AMT FIRST_AMT bigint(20) unsigned null default null, "
		"change column REMAIN_AMT REMAIN_AMT bigint(20) unsigned null default null, "
		"add primary key (ISIN_NO) "
	)


#기초자산정보테이블 제약조건 설정
	#쿼리문
_alter_asset_sql = \
	(
		"alter table els_market_underlying_info " 
		"change column ISIN_NO ISIN_NO varchar(50) not null, "
		"add primary key (ISIN_NO) "
	)

