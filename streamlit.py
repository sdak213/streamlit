# %%
import pymysql
import pandas as pd
import duckdb # pip install duckdb
from scipy.stats import chi2_contingency
from scipy import stats

# %%
dbConn = pymysql.connect(user='root', passwd='cksdyd123', host='127.0.0.1', db='Insu', charset='utf8')
cursor = dbConn.cursor(pymysql.cursors.DictCursor)

# %%
# MySQL에 쿼리하고 결과를 dataframe으로 반환
def sqldf(sql):
    cursor.execute(sql)
    result = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    
    # DataFrame 생성 시 열 이름 지정
    return pd.DataFrame(result, columns=column_names)
    # return pd.DataFrame(result)

# MySQL에 쿼리하고 결과를 딕셔너리로 반환
def sqldic(sql):
    cursor.execute(sql)
    return cursor.fetchall()

# 데이터프레임에 쿼리한 결과를 데이터프레임으로 반환
def dfsql(query):
   return duckdb.query(query).df()

# %%
def chi2(col, table):     
    keys = table['SIU_CUST_YN'].unique()
    d = {key: [] for key in keys}
    
    # 각 SIU_CUST_YN 별로 cnt 값을 수집합니다.
    for key in keys:
        counts = table[table['SIU_CUST_YN'] == key]['cnt'].tolist()
        d[key] = counts

    # 각 SIU_CUST_YN에 대해 cnt가 없으면 0으로 채웁니다.
    max_length = max(len(lst) for lst in d.values())
    for key in d:
        while len(d[key]) < max_length:
            d[key].append(0)  # 0으로 채움

    # DataFrame 생성
    table_df = pd.DataFrame(d)
    table_df.index = table[col].unique()  # 인덱스를 설정합니다.
    print(table_df) 
    return chi2_contingency(table_df)
def ttest(col, data):
    return stats.ttest_ind(data[col][data['SIU_CUST_YN']=='N'], data[col][data['SIU_CUST_YN']=='Y'], equal_var= False)


# %%
#거주시와 보험사기자는 관련이 있는가
table = sqldf(''' select  dense_rank() over (order by ctpr) as number,ctpr, SIU_CUST_YN,count(*) as cnt from Cust group by ctpr,SIU_CUST_YN order by ctpr,SIU_CUST_YN''')
table1= sqldf('''select  dense_rank() over (order by ctpr) as number, SIU_CUST_YN,count(*) as cnt from Cust group by ctpr,SIU_CUST_YN order by ctpr,SIU_CUST_YN''')
table

# %%
chi2('number', table1)

# %%
#결혼과 보험사기자 관련이 있는가
table = sqldf(''' select  dense_rank() over (order by WEDD_YN) as number,WEDD_YN, SIU_CUST_YN,count(*) as cnt from Cust group by WEDD_YN,SIU_CUST_YN order by WEDD_YN,SIU_CUST_YN''')
table1= sqldf(''' select  dense_rank() over (order by WEDD_YN) as number, SIU_CUST_YN,count(*) as cnt from Cust group by WEDD_YN,SIU_CUST_YN order by WEDD_YN,SIU_CUST_YN''')
table

# %%
chi2('number', table1)

# %%
#결혼한 사람의 자녀 여부는 보험사기자 관련이 있는가
table = sqldf(''' select  dense_rank() over (order by CHLD_CNT) as number,CHLD_CNT, SIU_CUST_YN,count(*) as cnt from Cust where WEDD_YN='Y' group by CHLD_CNT,SIU_CUST_YN order by CHLD_CNT,SIU_CUST_YN''')
table1= sqldf('''select  dense_rank() over (order by CHLD_CNT) as number, SIU_CUST_YN,count(*) as cnt from Cust  where WEDD_YN='Y' group by CHLD_CNT,SIU_CUST_YN having count(*)>1 order by CHLD_CNT,SIU_CUST_YN''')
table

# %%
chi2('number', table1)

# %%
#계약상태코드와 보험사기자와 관련이 있는가
table = sqldf(''' select  dense_rank() over (order by CNTT_STAT_CODE) as number,CNTT_STAT_CODE, SIU_CUST_YN,count(*) as cnt from Cust c1,cntt c2 where c1.cust_id=c2.cust_id  group by CNTT_STAT_CODE,SIU_CUST_YN order by CNTT_STAT_CODE,SIU_CUST_YN''')
table1= sqldf(''' select  dense_rank() over (order by CNTT_STAT_CODE) as number, SIU_CUST_YN,count(*) as cnt from Cust c1,cntt c2 where c1.cust_id=c2.cust_id  group by CNTT_STAT_CODE,SIU_CUST_YN order by CNTT_STAT_CODE,SIU_CUST_YN''')
table

# %%
chi2('number', table1)

# %%
#주계약의 보험금액과 보험사기자 분석
data = sqldf(''' select SIU_CUST_YN, MAIN_INSR_AMT from Cust c, Cntt n where c.cust_id = n.cust_id ''')
dfsql('''select SIU_CUST_YN, AVG(MAIN_INSR_AMT) from data group by SIU_CUST_YN ''')

# %%
ttest('MAIN_INSR_AMT', data)

# %%
#주계약의 보험금액과 보험사기자 추가분석
data = sqldf(''' select SIU_CUST_YN, MAIN_INSR_AMT from Cust c, Cntt n where c.cust_id = n.cust_id and CNTT_STAT_CODE IN ('3','4','A','B','J','L') ''')
dfsql('''select SIU_CUST_YN, AVG(MAIN_INSR_AMT) from data group by SIU_CUST_YN ''')

# %%
ttest('MAIN_INSR_AMT', data)

# %%
#유의병원 여부와  보험사기자와 관련이 있는가
table = sqldf(''' select  dense_rank() over (order by HEED_HOSP_YN) as number,HEED_HOSP_YN, SIU_CUST_YN,count(*) as cnt from Cust c1,claim c2 where c1.cust_id=c2.cust_id  group by HEED_HOSP_YN,SIU_CUST_YN order by HEED_HOSP_YN,SIU_CUST_YN''')
table1 = sqldf(''' select  dense_rank() over (order by HEED_HOSP_YN) as number, SIU_CUST_YN,count(*) as cnt from Cust c1,claim c2 where c1.cust_id=c2.cust_id  group by HEED_HOSP_YN,SIU_CUST_YN order by HEED_HOSP_YN,SIU_CUST_YN''')
table

# %%
chi2('number', table1)  

# %%
#병원 지역이 보험사기자와 관련이 있는가
table = sqldf(''' select  dense_rank() over (order by ACCI_HOSP_ADDR) as number,ACCI_HOSP_ADDR, SIU_CUST_YN,count(*) as cnt from Cust c1,claim c2 where c1.cust_id=c2.cust_id and ACCI_HOSP_ADDR not in('None','세종','충북')  group by ACCI_HOSP_ADDR,SIU_CUST_YN order by ACCI_HOSP_ADDR,SIU_CUST_YN''')
table1 = sqldf(''' select  dense_rank() over (order by ACCI_HOSP_ADDR) as number, SIU_CUST_YN,count(*) as cnt from Cust c1,claim c2 where c1.cust_id=c2.cust_id and ACCI_HOSP_ADDR not in('None','세종','충북')  group by ACCI_HOSP_ADDR,SIU_CUST_YN order by ACCI_HOSP_ADDR,SIU_CUST_YN''')
table

# %%
chi2('number', table1)  

# %%
#병원 종별 구분이 보험사기자와 관련이 있는가
table = sqldf(''' select  dense_rank() over (order by HOSP_SPEC_DVSN) as number,HOSP_SPEC_DVSN, SIU_CUST_YN,count(*) as cnt from Cust c1,claim c2 where c1.cust_id=c2.cust_id  group by HOSP_SPEC_DVSN,SIU_CUST_YN order by HOSP_SPEC_DVSN,SIU_CUST_YN''')
table1 = sqldf(''' select  dense_rank() over (order by HOSP_SPEC_DVSN) as number, SIU_CUST_YN,count(*) as cnt from Cust c1,claim c2 where c1.cust_id=c2.cust_id  group by HOSP_SPEC_DVSN,SIU_CUST_YN order by HOSP_SPEC_DVSN,SIU_CUST_YN''')
table

# %%
chi2('number', table1)  

# %%
#병원거리와 보험사기자는 관련이 있는가
data = sqldf(''' select SIU_CUST_YN, HOUSE_HOSP_DIST from Cust c,claim n where c.cust_id = n.cust_id ''')
dfsql('''select SIU_CUST_YN, AVG(HOUSE_HOSP_DIST) from data group by SIU_CUST_YN ''')


# %%
ttest('HOUSE_HOSP_DIST', data)

# %%
#강원 전북에 있는 병원거리와 보험사기자는 관련이 있는가
data = sqldf(''' select SIU_CUST_YN, HOUSE_HOSP_DIST from Cust c,claim n where c.cust_id = n.cust_id and ACCI_HOSP_ADDR in ('강원','전북') ''')
dfsql('''select SIU_CUST_YN, AVG(HOUSE_HOSP_DIST) from data group by SIU_CUST_YN ''')


# %%
ttest('HOUSE_HOSP_DIST', data)

# %%
#청구사유코드와 보험사기자는 관련이 있는가
table = sqldf(''' select  dense_rank() over (order by DMND_RESN_CODE) as number,DMND_RESN_CODE, SIU_CUST_YN,count(*) as cnt from Cust c1,claim c2 where c1.cust_id=c2.cust_id  group by DMND_RESN_CODE,SIU_CUST_YN order by DMND_RESN_CODE,SIU_CUST_YN''')
table1 = sqldf(''' select  dense_rank() over (order by DMND_RESN_CODE) as number, SIU_CUST_YN,count(*) as cnt from Cust c1,claim c2 where c1.cust_id=c2.cust_id  group by DMND_RESN_CODE,SIU_CUST_YN order by DMND_RESN_CODE,SIU_CUST_YN''')
table

# %%
chi2('number', table1)  

# %%
#직업그룹코드와 보험사기자는 관련이 있는가
table = sqldf(''' select  dense_rank() over (order by ACCI_OCCP_GRP2) as number,ACCI_OCCP_GRP2, SIU_CUST_YN,count(*) as cnt from Cust c1,claim c2 where c1.cust_id=c2.cust_id and ACCI_OCCP_GRP2 Not in('고소득 전문직', '고소득의료직','고위 공무원','교사','기업/단체 임원', '기타','대학교수/강사'
              ,'법무직 종사자', '전문직', '학자/연구직') group by ACCI_OCCP_GRP2,SIU_CUST_YN order by ACCI_OCCP_GRP2,SIU_CUST_YN''')
table1 = sqldf(''' select  dense_rank() over (order by ACCI_OCCP_GRP2) as number, SIU_CUST_YN,count(*) as cnt from Cust c1,claim c2 where c1.cust_id=c2.cust_id and ACCI_OCCP_GRP2 Not in('고소득 전문직', '고소득의료직','고위 공무원','교사','기업/단체 임원', '기타','대학교수/강사'
              ,'법무직 종사자', '전문직', '학자/연구직') group by ACCI_OCCP_GRP2,SIU_CUST_YN order by ACCI_OCCP_GRP2,SIU_CUST_YN''')
table

# %%
chi2('number', table1)  

# %%
#(사고접수-원사고)와 보험사기자 관련이 있는가
data = sqldf(''' select SIU_CUST_YN, DATEDIFF(n.RECP_DATE, n.ORIG_RESN_DATE) date from Cust c,claim n where c.cust_id = n.cust_id ''')
dfsql('''select SIU_CUST_YN, AVG(date) from data group by SIU_CUST_YN ''')


# %%
ttest('date', data)

# %%
#(사고접수-원사고)와 보험사기자 관련이 있는가
data = sqldf(''' select SIU_CUST_YN, DMND_AMT from Cust c,claim n where c.cust_id = n.cust_id and DMND_AMT >SELF_CHAM ''')
dfsql('''select SIU_CUST_YN, AVG(DMND_AMT) from data group by SIU_CUST_YN ''')

# %%
ttest('DMND_AMT', data)


