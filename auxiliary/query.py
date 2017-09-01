"""샘플쿼리 모듈
:filename:          - query.py
:modified:          - 2017.08.24
:note:              - 이 모듈에서는 자주사용하는 샘플쿼리를 미리 정의함

"""


'''모듈 불러오기'''
from sqlalchemy import types						#ALCHEMY for engine


'''쿼리 인스턴스 임포트'''
#기본상품정보조회
	#컬럼 데이터타입
basic_col = \
	{'ISIN_NO'		:types.NVARCHAR(length=50),
	 'STD_DATE'		:types.DateTime(),
	 'CUST_NO'		:types.NVARCHAR(length=10),
	 'FIRST_AMT'	:types.BigInteger(),
	 'REMAIN_AMT'	:types.BigInteger(),
	 'EFF_DATE'		:types.DateTime(),
	 'MAT_DATE'		:types.DateTime()}
	#쿼리문
basic_sql = \
	(
		"select tblLATEST.ISIN_NO, "  																		#ISIN번호
		"		to_date(greatest(tblLATEST.STND_DATE, nvl(tblREFUND.STND_DATE,0)),'yyyymmdd') STD_DATE, "	#처리일자
		"		tblLATEST.CUST_NO, "  																		#발행사번호
		"		tblLATEST.FIRST_AMT FIRST_AMT, " 															#최초발행금액 (백만원)
		"		greatest(tblLATEST.FIRST_AMT-nvl(tblREFUND.REFUND,0),0) REMAIN_AMT, " 						#현재발행잔액 (백만원)
		"		tblLATEST.EFF_DATE, "  																		#설정일자
		"		tblLATEST.MAT_DATE "  																		#만기일자
		"from "
		"( "
		"	select 	ISIN_NO, "
		"			max(STND_DATE) STND_DATE, "
		"			to_number(SUBSTR(max(CUST_NO),-2),'99') CUST_NO, "
		"			max(FIRST_ISSUE_AMT) FIRST_AMT, "
		"			to_date(max(STD_ESTIM_T_DATE),'yyyymmdd') EFF_DATE, "
		"			to_date(max(XPIR_ESTIM_T_DATE),'yyyymmdd') MAT_DATE "
		"	from 	INFO_KSD_ISSUE_LIST "
		"	where 	STND_DATE<replace('{eval_date}','-') and "
		"			SECURITY_TYPE=41 and "  																#ELS에 대해서만 조회
		"			STD_ESTIM_T_DATE>replace('{fr_eff_date}','-') and " 		 							#설정일자 조건
		"			STD_ESTIM_T_DATE<replace('{to_eff_date}','-') and "  									#설정일자 조건
		"			XPIR_ESTIM_T_DATE>replace('{fr_exp_date}','-') and "  									#만기일자 조건
		"			XPIR_ESTIM_T_DATE<replace('{to_exp_date}','-') and "  									#만기일자 조건
		"			PRSV_RATE<={prsv_rate} and " 															#원금보장비율 조건
		"			regexp_like(ISIN_NO, 'KR6[1-9]{2}3[[:alnum:]]{1,}') "
		"	group by ISIN_NO "
		"	having min(ISSUE_AMT)>{req_amt_min} " 	 														#현재발행잔액 조건
		") tblLATEST "  																					#tblLATEST 최근발행정보
		"left outer join "
		"( "
		"	select 	ISIN_NO, "
		"			max(STND_DATE) STND_DATE, "
		"			sum(REFUND_QTY) REFUND "
		"	from 	INFO_KSD_REFUND "
		"	where 	STND_DATE<replace('{eval_date}','-') and "
		"			SEC_TYPE IN ('1','3') "
		"	group by ISIN_NO "
		") tblREFUND "  																					#tblREFUND 환매정보
		"on tblREFUND.ISIN_NO=tblLATEST.ISIN_NO "
		"where 	greatest(tblLATEST.FIRST_AMT-nvl(tblREFUND.REFUND,0),0)>{req_amt_min} and "					#현재발행잔액 조건
		" 		greatest(tblLATEST.FIRST_AMT-nvl(tblREFUND.REFUND,0),0)<{req_amt_max} and "					#현재발행잔액 조건
		"		greatest(tblLATEST.STND_DATE, nvl(tblREFUND.STND_DATE,0))>replace('{fr_std_date}','-') "
		"order by tblLATEST.EFF_DATE asc "
	)


#기초자산정보조회
	#컬럼 데이터타입
asset_col = \
	{'ISIN_NO'		:types.NVARCHAR(length=50),
	 'NAME_AST1'	:types.NVARCHAR(length=20),
	 'NAME_AST2'	:types.NVARCHAR(length=20),
	 'NAME_AST3'	:types.NVARCHAR(length=20),
	 'NAME_AST4'	:types.NVARCHAR(length=20),
	 'NAME_AST5'	:types.NVARCHAR(length=20)}
_asset_list_decode = \
	"'KP2','1','NKY','2','HSC','3','SXE','4','SPX','5','HSI','6','7'"
_asset_list_in = \
	"('KP2','NKY','HSC','SXE','SPX','HSI')"
_asset_regex_name = \
	''' '^.*(KOSPI2|KP2|200).*$','KP2'),
		'^.*(NKY|NIKKEI).*$','NKY'),
		'^.*(HSCEI|HANGSENGCHINAENT).*$','HSC'),
		'^.*(EUROSTOXX|SX5E).*$','SXE'),
		'^.*(S.P500).*$','SPX'),
		'^.*(HSI|HANGSENG).*$','HSI' 
	'''
	#쿼리문
asset_sql = \
	(
		"select	ISIN_NO, "
		"		max(NAME_AST1) NAME_AST1, "
		"		max(NAME_AST2) NAME_AST2, "
		"		max(NAME_AST3) NAME_AST3, "
		"		max(NAME_AST4) NAME_AST4, "
		"		max(NAME_AST5) NAME_AST5 "
		"from "
		"( "
		"	select 	tblNOR.ISIN_NO, "
		"			tblNOR.STND_DATE STD_DATE, "
		"			SEQ, "
		"			lead(BASIC_ASSET,0) over (partition by tblNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST1, " 
		"			lead(BASIC_ASSET,1) over (partition by tblNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST2, "
		"			lead(BASIC_ASSET,2) over (partition by tblNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST3, "
		"			lead(BASIC_ASSET,3) over (partition by tblNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST4, "
		"			lead(BASIC_ASSET,4) over (partition by tblNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ ASC) NAME_AST5 " 
		"	from "
		"	( "	
		"		select 	ISIN_NO, "
		"				SEQ, "
		"				STND_DATE," 
		"				regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
		"				regexp_replace(regexp_replace(upper(BASIC_ASSET),'( ){1,}'),'[^A-Za-z0-9]','')," + _asset_regex_name + ") BASIC_ASSET "
		"		from 	INFO_KSD_UNDERLYING " 
		"		where 	INDEX_TYPE='1' and "
		"				regexp_like(ISIN_NO, 'KR6[1-9]{2}3[[:alnum:]]{1,}') and "
		"				STND_DATE>=replace('{last_date}','-') and "
		"				STND_DATE<replace('{eval_date}','-') "
		"	) tblNOR, " 
		"	( "
		"		select 	ISIN_NO, "
		"				min(STND_DATE) STND_DATE " 
		"		from 	INFO_KSD_UNDERLYING "
		"		where INDEX_TYPE='1' "
		"		group by ISIN_NO "
		"	) tblGRP "
		"	where 	tblNOR.ISIN_NO=tblGRP.ISIN_NO and "
	   	"			tblNOR.STND_DATE=tblGRP.STND_DATE and "
		"			length(tblNOR.BASIC_ASSET)=3 and "
		"			tblNOR.BASIC_ASSET in " + _asset_list_in + " and " 
		"			tblGRP.STND_DATE>=replace('{last_date}','-') and "
		"			tblGRP.STND_DATE<replace('{eval_date}','-') "
		") "
		"where 	SEQ=1 "
		"group by ISIN_NO "
	)

#상환구조정보조회
	#컬럼 데이터타입
exer_col = \
	{'ISIN_NO'		:types.NVARCHAR(length=50),
	 'EXE_DATE'		:types.DateTime(),
	 'EXE_LVL'		:types.Float()}
'''
exer_oper = \
	('OPER1','OPER2','OPER3')

_exer_type1 = \
	{'OPER1':"'EE'",
	 'OPER2':"'FE'",
	 'OPER3':"'KI'"}

_exer_type2 = \
	{'OPER1':"'N/A'",
	 'OPER2':"'N/A'",
	 'OPER3':"'N'"}

_exer_type3 = \
	{'OPER1':"ESTIM_TYPE='1' ",
	 'OPER2':"ESTIM_NO=1 and ESTIM_TYPE='2' ",
	 'OPER3':"ESTIM_NO=2 and ESTIM_TYPE='2' "}

exer_type = \
	(_exer_type1, _exer_type2, _exer_type3)
'''
	#쿼리문
exer_sql = \
	(
		"select distinct	tmpA.ISIN_NO, "
		"					tmpA.ESTIM_T_DATE EXE_DATE, "
		"					case 	when tmpA.AUTO_REFUND_COND > 0 "
		"							then tmpA.AUTO_REFUND_COND "
		"							else nvl(tmpB.XCISE_RATE, tmpC.XCISE_RATE) end EXE_LVL "
		"from "
		"( "
		"	select tmpA1.* "
		"	from "
		"	( "
		"		select 	ISIN_NO, ESTIM_TYPE, TO_DATE(ESTIM_T_DATE, 'YYYYMMDD') "
		"				ESTIM_T_DATE, STND_DATE, max(ESTIM_NO) ESTIM_NO, "
		"				to_number("
		"					replace(nvl(regexp_substr(regexp_replace(max(AUTO_REFUND_COND),'KOSPI200|NIKKEI225|HSCEI|STOXX50|S.P500|HSI'), "
		"				  	'[1-9]{1}[[:digit:]]{1,}\.{0,1}[[:digit:]]{0,}[[:space:]]{0,1}%{1,}'), -9), '%')) AUTO_REFUND_COND "
		"		from INFO_KSD_AUTOCALL "
		"		where	((ESTIM_TYPE = '2' AND ESTIM_NO = 1) OR ESTIM_TYPE = '1') AND "
		"				ESTIM_T_DATE > '19000101' and "
		"				regexp_like(ISIN_NO, 'KR6[1-9]{2}3[[:alnum:]]{1,}') "
		"		group by ISIN_NO, STND_DATE, ESTIM_T_DATE, ESTIM_TYPE "
		"	) tmpA1, "
  		"	( "
	  	"		select ISIN_NO, max(STND_DATE) STND_DATE, min(STND_DATE) MIN_DATE "
		"		from INFO_KSD_AUTOCALL "
		"		group by ISIN_NO "
		"	) tmpA2 "
		"	where 	tmpA1.ISIN_NO = tmpA2.ISIN_NO AND "
		"			tmpA1.STND_DATE = tmpA2.STND_DATE and "
		"			tmpA2.MIN_DATE<replace('{eval_date}','-') and "
		"			tmpA2.MIN_DATE>=replace('{last_date}','-') "
		") tmpA "
		"left outer join "
		"( "
		"	select 	ISIN_NO, STND_DATE, XCISE_RATE, "
		"			nvl(regexp_substr(nvl(ETC, 99), '[1-9]{1}[0-9]{0,}', 1, 1), 0) LB, "
		"			nvl(regexp_substr(nvl(ETC, 99), '[1-9]{1}[0-9]{0,}', 1, 2), nvl(regexp_substr(nvl(ETC, 99), '[1-9]{1}[0-9]{0,}', 1, 1), 0)) * "
		"			case when DECODE(ETC, NULL, -1, 1) < 0 then - 1 else 1 end UB "
		"	from INFO_KSD_UNDERLYING "
		"	where INDEX_TYPE = '5' "
		") tmpB "
		"on tmpA.ISIN_NO = tmpB.ISIN_NO AND "
		"	tmpA.STND_DATE = tmpB.STND_DATE AND "
		"	tmpA.ESTIM_NO >= tmpB.LB  AND " 
		"	tmpA.ESTIM_NO <= tmpB.UB  AND "
		"	tmpA.ESTIM_TYPE = '1' "
		"left outer join "
		"( "
		"	select ISIN_NO, MIN(XCISE_RATE) XCISE_RATE "
		"	from INFO_KSD_UNDERLYING " 
		"	where INDEX_TYPE = '5' "
		"	group by ISIN_NO"
		") tmpC "
		"on tmpA.ISIN_NO = tmpC.ISIN_NO "
	)
"""
	(
		"select tblMAIN.ISIN_NO, "
		"		to_date(tblEXE1.ESTIM_T_DATE,'yyyymmdd') EXE_DATE, "
		"		to_number("
		"			case 	when tblEXE1.EXE_LVL>0 "
		"					then tblEXE1.EXE_LVL "
		"					else nvl(tblEXE2.EXE_LVL,tblEXE3.EXE_LVL) end,'9999.99') EXE_LVL, "
		"		{oper_num1} EXE_TYPE, "
		"		{oper_num2} EXE_FLAG "
		"from "
		"( "
		"	select tblMAIN1.* "
		"	from  "
		"	( "
		"		select 	ISIN_NO, "
		"				max(STND_DATE) DATE_max, "
		"				MIN(STND_DATE) DATE_MIN "
		"		from 	INFO_KSD_AUTOCALL "
		"		group by ISIN_NO "
		"	) tblMAIN1, "
		"	( "
		"		select 	ISIN_NO "
		"		from 	INFO_KSD_ISSUE_LIST "
		"		where 	SECURITY_TYPE='41' and "
		"				PRSV_RATE=0 "
		"		group by ISIN_NO "
		"	) tblMAIN2 "
		"	where tblMAIN1.ISIN_NO=tblMAIN2.ISIN_NO " 
		") tblMAIN "
		"inner join "
		"( "
		"	select 	ISIN_NO, "
		"			ESTIM_T_DATE, "
		"			STND_DATE, "
		"			min(ESTIM_NO) ESTIM_NO, "
		"			max(to_number(replace(nvl(regexp_substr(regexp_replace(AUTO_REFUND_COND, 'KOSPI200|NIKKEI225|HSCEI|STOXX50|S.P500|HSI'),'[1-9]{1}[[:digit:]]{1,}\.{0,1}[[:digit:]]{0,}[[:space:]]{0,1}%{1,}'),-9),'%'))) EXE_LVL "
		"	from 	INFO_KSD_AUTOCALL "
		"	where {oper_num3} "
		"	group by ISIN_NO, ESTIM_T_DATE, STND_DATE"
		") tblEXE1 "
		"on tblMAIN.ISIN_NO=tblEXE1.ISIN_NO and "
		"	tblMAIN.DATE_max=tblEXE1.STND_DATE " 
		"left outer join   "
		"(  "
		"	select * "
		"	from "
		"	( "
		"		select	ISIN_NO, "
		"				max(XCISE_RATE) EXE_LVL, "
		"				nvl(regexp_substr(nvl(ETC,99),'[1-9]{1}[0-9]{0,}',1,1),0) LBOUND, "
		"				nvl(regexp_substr(nvl(ETC,99),'[1-9]{1}[0-9]{0,}',1,2),nvl(regexp_substr(nvl(ETC,99),'[1-9]{1}[0-9]{0,}',1,1),0))*case when decode(ETC,null,-1,1)<0 then -1 else 1 end UBOUND "
		"		from 	INFO_KSD_UNDERLYING  "
		"		where  	nvl(ETC,'-1')<>'-1' and "
		"				INDEX_TYPE='5' "
		"		group by ISIN_NO, ETC  "
		"	) "
		"	where LBOUND>0 and UBOUND>0 and LBOUND<UBOUND and UBOUND<50 "
		") tblEXE2  "
		"on	tblMAIN.ISIN_NO=tblEXE2.ISIN_NO and "
		"	tblEXE1.ESTIM_NO>=tblEXE2.LBOUND and  "
		"	tblEXE1.ESTIM_NO<=tblEXE2.UBOUND "
		"inner join  "
		"( "
		"	select	ISIN_NO, "
		"			min(XCISE_RATE) EXE_LVL "
		"	from 	INFO_KSD_UNDERLYING  "
		"	where INDEX_TYPE='5'  "
		"		group by ISIN_NO  "
		") tblEXE3  "
		"on tblMAIN.ISIN_NO=tblEXE3.ISIN_NO " 			
		"where	tblEXE1.ESTIM_T_DATE>'19001001' and " 
		"		tblMAIN.DATE_MIN<replace('{eval_date}','-') and "
		"		tblMAIN.DATE_MIN>=replace('{last_date}','-') "
		"order by 	ISIN_NO asc, "
		"			EXE_DATE asc "
	)
"""

#작업일자정보삭제
	#쿼리문
hist_delete_sql = \
	("delete from els_market_oper_hist where OPER_DATE='{oper_date}' ")


#작업일자 정보입력
	#쿼리문
hist_insert_sql = \
	("insert into els_market_oper_hist (OPER_DATE) values ('{oper_date}') ")


#최근작업일자 정보조회
	#쿼리문
hist_select_sql = \
	("select max(OPER_DATE) STD_DATE from els_market_oper_hist ")
	#컬럼 데이터타입


#기본정보테이블 제약조건 설정
	#쿼리문
alter_basic_sql = \
	(
		"alter table {table_name} " 
		"change column ISIN_NO ISIN_NO varchar(50) not null, "
		"change column FIRST_AMT FIRST_AMT bigint(20) unsigned null default null, "
		"change column REMAIN_AMT REMAIN_AMT bigint(20) unsigned null default null, "
		"add primary key (ISIN_NO) "
	)
