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
	 'FIRST_AMT'	:types.BigInteger(),
	 'REMAIN_AMT'	:types.BigInteger(),
	 'EFF_DATE'		:types.DateTime(),
	 'MAT_DATE'		:types.DateTime(),
	 'PRSV_RATE'	:types.Float()}
	#쿼리문
basic_sql = \
	(
		"select ISIN_NO,"
		"		to_date(STD_DATE,'yyyymmdd') STD_DATE, "
		"		FIRST_AMT, REMAIN_AMT, "
		"		EFF_DATE, MAT_DATE, PRSV_RATE "
		"from "
		"( "
		"	select 	tblLATEST.ISIN_NO, "  																		#ISIN번호
		"			greatest(tblLATEST.STND_DATE, nvl(tblREFUND.STND_DATE,0)) STD_DATE, "						#처리일자
		"			tblLATEST.FIRST_AMT/1000000 FIRST_AMT, " 													#최초발행금액 (백만원)
		"			greatest(tblLATEST.FIRST_AMT-nvl(tblREFUND.REFUND,0),0)/1000000 REMAIN_AMT, " 				#현재발행잔액 (백만원)
		"			tblLATEST.EFF_DATE, "  																		#설정일자
		"			tblLATEST.MAT_DATE, "																		#만기일자
		"			tblLATEST.PRSV_RATE "  																	
		"	from "
		"	(	 "
		"		select 	ISIN_NO, "
		"				max(STND_DATE) STND_DATE, "
		"				max(FIRST_ISSUE_AMT) FIRST_AMT, "
		"				to_date(min(least(STD_ESTIM_F_DATE,STD_ESTIM_T_DATE)),'yyyymmdd') EFF_DATE, "
		"				to_date(max(greatest(XPIR_ESTIM_F_DATE,XPIR_ESTIM_T_DATE)),'yyyymmdd') MAT_DATE,"
		"				min(PRSV_RATE) PRSV_RATE "
		"		from 	INFO_KSD_ISSUE_LIST "
		"		where 	STND_DATE<replace('{eval_date}','-') and "
		"				SECURITY_TYPE=41 and "  																#ELS에 대해서만 조회
		"				STD_ESTIM_T_DATE>replace('{fr_eff_date}','-') and " 		 							#설정일자 조건
		"				STD_ESTIM_T_DATE<replace('{to_eff_date}','-') and "  									#설정일자 조건
		"				XPIR_ESTIM_T_DATE>replace('{fr_exp_date}','-') and "  									#만기일자 조건
		"				XPIR_ESTIM_T_DATE<replace('{to_exp_date}','-') and "  									#만기일자 조건
		"				PRSV_RATE<={prsv_rate} " 																#원금보장비율 조건		
		"		group by ISIN_NO "
		"		having min(ISSUE_AMT)>{req_amt_min} " 	 														#현재발행잔액 조건
		"	) tblLATEST "  																						#tblLATEST 최근발행정보
		"	left outer join "
		"	( "
		"		select 	ISIN_NO, "
		"				max(STND_DATE) STND_DATE, "
		"				sum(REFUND_QTY) REFUND "
		"		from 	INFO_KSD_REFUND "
		"		where 	STND_DATE<replace('{eval_date}','-') and "
		"				SEC_TYPE IN ('1','3') "
		"		group by ISIN_NO "
		"	) tblREFUND "  																						#tblREFUND 환매정보
		"	on tblREFUND.ISIN_NO=tblLATEST.ISIN_NO "
		") "
		"where 	REMAIN_AMT>{req_amt_min} and "																	#현재발행잔액 조건
		" 		REMAIN_AMT<{req_amt_max} and "																	#현재발행잔액 조건
		"		STD_DATE>replace('{fr_std_date}','-') "
		"order by EFF_DATE asc "
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
	''' '^.*(KP2|KOSPI2|K[[:alnum:]]{0,}200|200).*$','KP2'),
		'^.*(NKY|NIKKEI|N[[:alnum:]]{0,}225|225).*$','NKY'),
		'^.*(HSC|HSCE|HSCEI|HANGSENG[[:alnum:]]{0,}ENT).*$','HSC'),
		'^.*(SXE|STOXX|EURO[[:alnum:]]{0,}50|EUROSTOXX|EUROSTOCK|SX5E).*$','SXE'),
		'^.*(SPX|S[[:alnum:]]{0,}500|500).*$','SPX'),
		'^.*(HSI|HANGSENG).*$','HSI'),
		'^H$','HSC' 
	'''
	#쿼리문
asset_sql = \
	(
		#"select from ISIN_NO, max(NAME_AST1), max(NAME_AST2), max(NAME_AST3), max(NAME_AST4), max(NAME_AST5) "
		#"("
		"	select	ISIN_NO, NAME_AST1, NAME_AST2, NAME_AST3, NAME_AST4, NAME_AST5 "
		"	from "
		"	( "
		"		select 	tblNOR.ISIN_NO, "
		"				tblNOR.STND_DATE STD_DATE, "
		"				SEQ, "
		"				row_number() over (partition by tblNOR.ISIN_NO order by decode(BASIC_ASSET," + _asset_list_decode + ") asc) AST_SEQ, "
		"				lead(BASIC_ASSET,0) over (partition by tblNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST1, " 
		"				lead(BASIC_ASSET,1) over (partition by tblNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST2, "
		"				lead(BASIC_ASSET,2) over (partition by tblNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST3, "
		"				lead(BASIC_ASSET,3) over (partition by tblNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ asc) NAME_AST4, "
		"				lead(BASIC_ASSET,4) over (partition by tblNOR.ISIN_NO order by decode (BASIC_ASSET, " + _asset_list_decode + ") asc, SEQ ASC) NAME_AST5 " 
		"		from "
		"		( "	
		"			select 	ISIN_NO, "
		"					SEQ, "
		"					STND_DATE," 
		"					trim(substr(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
		"					regexp_replace(regexp_replace(regexp_replace(upper(BASIC_ASSET),'( ){1,}'),'[^A-Za-z0-9]','')," + _asset_regex_name + "),1,15)) BASIC_ASSET "
		"			from 	INFO_KSD_UNDERLYING " 
		"			where 	INDEX_TYPE='1' and "
		"					STND_DATE>=replace('{last_date}','-') and "
		"					STND_DATE<replace('{eval_date}','-') "
		"		) tblNOR, " 
		"		( "
		"			select 	ISIN_NO, "
		"					min(STND_DATE) STND_DATE " 
		"			from 	INFO_KSD_UNDERLYING "
		"			where INDEX_TYPE='1' "
		"			group by ISIN_NO "
		"		) tblGRP "
		"		where 	tblNOR.ISIN_NO=tblGRP.ISIN_NO and "
	   	"				tblNOR.STND_DATE=tblGRP.STND_DATE and "
		"				tblGRP.STND_DATE>=replace('{last_date}','-') and "
		"				tblGRP.STND_DATE<replace('{eval_date}','-') "
		"	) "
		"	where 	AST_SEQ=1 and "
		"			nvl(NAME_AST1,'N/A')<>'N/A' "
	)

#상환구조정보조회
	#컬럼 데이터타입
exer_col = \
	{'ISIN_NO':	types.NVARCHAR(length=50),
	 'EXE_DATE':types.DateTime(),
	 'EXE_LVL':	types.Float()}

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
		"		select 	ISIN_NO, ESTIM_TYPE, to_date(ESTIM_T_DATE, 'YYYYMMDD') "
		"				ESTIM_T_DATE, STND_DATE, max(ESTIM_NO) ESTIM_NO, "
		"				to_number("
		"					replace(nvl(regexp_substr(regexp_replace(regexp_replace(max(upper(AUTO_REFUND_COND)),'[^A-Za-z0-9]',''),'KOSPI200|NIKKEI225|HSCEI|STOXX50|S.P500|HSI'), "
		"				  	'[1-9]{1}[[:digit:]]{1,}\.{0,1}[[:digit:]]{0,}[[:space:]]{0,1}%{1,}'), -9), '%')) AUTO_REFUND_COND "
		"		from INFO_KSD_AUTOCALL "
		"		where	((ESTIM_TYPE = '2' and ESTIM_NO = 1) OR ESTIM_TYPE = '1') and "
		"				ESTIM_T_DATE > '19000101' "
		"		group by ISIN_NO, STND_DATE, ESTIM_T_DATE, ESTIM_TYPE "
		"	) tmpA1, "
  		"	( "
	  	"		select ISIN_NO, max(STND_DATE) STND_DATE, min(STND_DATE) MIN_DATE "
		"		from INFO_KSD_AUTOCALL "
		"		group by ISIN_NO "
		"	) tmpA2 "
		"	where 	tmpA1.ISIN_NO = tmpA2.ISIN_NO and "
		"			tmpA1.STND_DATE = tmpA2.STND_DATE and "
		"			tmpA2.MIN_DATE<replace('{eval_date}','-') and "
		"			tmpA2.MIN_DATE>=replace('{last_date}','-') "
		") tmpA "
		"left outer join "
		"( "
		"	select 	ISIN_NO, STND_DATE, XCISE_RATE, "
		"			nvl(regexp_substr(nvl(ETC, 99), '[1-9]{1}[[:digit:]]{0,}', 1, 1), 0) LB, "
		"			nvl(regexp_substr(nvl(ETC, 99), '[1-9]{1}[[:digit:]]{0,}', 1, 2), nvl(regexp_substr(nvl(ETC, 99), '[1-9]{1}[[:digit:]]{0,}', 1, 1), 0)) * "
		"			case when DECODE(ETC, NULL, -1, 1) < 0 then - 1 else 1 end UB "
		"	from INFO_KSD_UNDERLYING "
		"	where INDEX_TYPE = '5' "
		") tmpB "
		"on tmpA.ISIN_NO = tmpB.ISIN_NO and "
		"	tmpA.STND_DATE = tmpB.STND_DATE and "
		"	tmpA.ESTIM_NO >= tmpB.LB  and " 
		"	tmpA.ESTIM_NO <= tmpB.UB  "
		"left outer join "
		"( "
		"	select ISIN_NO, MIN(XCISE_RATE) XCISE_RATE "
		"	from INFO_KSD_UNDERLYING " 
		"	where INDEX_TYPE = '5' "
		"	group by ISIN_NO"
		") tmpC "
		"on tmpA.ISIN_NO = tmpC.ISIN_NO "
	)

#다음상환구조정보조회
	#컬럼 데이터타입
next_exer_col = \
	{'ISIN_NO':	types.NVARCHAR(50),
	 'EXE_DATE':types.DateTime(),
	 'EXE_LVL':	types.Float()}
	#쿼리문
next_exer_sql = \
	(
		"select ST.ISIN_NO, ST.EXE_DATE, ST.EXE_LVL "
		"from 	eqd_jh_jo.view_els_market_structure_info ST, "
		"		( "
		"			select ISIN_NO, min(EXE_DATE) NEXT_DATE " 
		"			from eqd_jh_jo.view_els_market_structure_info "
		"			where EXE_DATE>='{eval_date}' "
		"			group by ISIN_NO "
		"		) tblNEXT "
		"where 	ST.ISIN_NO=tblNEXT.ISIN_NO and "
		"		ST.EXE_DATE=tblNEXT.NEXT_DATE "
		"group by ISIN_NO, EXE_DATE"
	)


#인덱스가격정보 조회
	#컬럼 데이터타입
index_col = \
	{'STD_DATE':types.DateTime(),
	 'KP2':		types.Float(),
	 'NKY':		types.Float(),
	 'HSC':		types.Float(),
	 'SXE':		types.Float(),
	 'SPX':		types.Float(),
	 'HSI':		types.Float()}

	#쿼리문
index_sql = \
	(
		"select	to_date(TDATE.TRADE_DATE,'YYYYMMDD') STD_DATE, " 
       	"		last_value(QKP2.PX_LAST ignore nulls) "
		"			over (order by TDATE.TRADE_DATE rows between unbounded preceding and 0 preceding) KP2, " 
		"		last_value(QNKY.PX_LAST ignore nulls) "
		"			over (order by TDATE.TRADE_DATE rows between unbounded preceding and 0 preceding) NKY, "
		"		last_value(QHSC.PX_LAST ignore nulls) "
		"			over (order by TDATE.TRADE_DATE rows between unbounded preceding and 0 preceding) HSC, "
		"		last_value(QSXE.PX_LAST ignore nulls) "
		"			over (order by TDATE.TRADE_DATE rows between unbounded preceding and 0 preceding) SXE, "
		"		last_value(QSPX.PX_LAST ignore nulls) "
		"			over (order by TDATE.TRADE_DATE rows between unbounded preceding and 0 preceding) SPX, "
		"		last_value(QHSI.PX_LAST ignore nulls) "
		"			over (order by TDATE.TRADE_DATE rows between unbounded preceding and 0 preceding) HSI "
		"from "
		"( "
		"	select distinct TRADE_DATE from BL_DATA "
		") TDATE "
		"left outer join (select * from BL_DATA where TICKER='KOSPI2') 	QKP2 "
		"on TDATE.TRADE_DATE=QKP2.TRADE_DATE "
		"left outer join (select * from BL_DATA where TICKER='NKY') 	QNKY "
		"on TDATE.TRADE_DATE=QNKY.TRADE_DATE "
		"left outer join (select * from BL_DATA where TICKER='HSCEI') 	QHSC "
		"on TDATE.TRADE_DATE=QHSC.TRADE_DATE "
		"left outer join (select * from BL_DATA where TICKER='SX5E') 	QSXE "
		"on TDATE.TRADE_DATE=QSXE.TRADE_DATE "
		"left outer join (select * from BL_DATA where TICKER='SPX') 	QSPX "
		"on TDATE.TRADE_DATE=QSPX.TRADE_DATE "
		"left outer join (select * from BL_DATA where TICKER='HSI') 	QHSI "
		"on TDATE.TRADE_DATE=QHSI.TRADE_DATE "
		"where TDATE.TRADE_DATE>=replace('{date_from}','-') "
		"order by 1 ASC "
	)


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
