import pymssql
import pickle
import pandas as pd
import os
 
def modeling(file_path, model_path):
    # 신규 설비 데이터
    new_data = file_path
    # 모델
    model_name = model_path

    # 신규 설비 데이터 불러오기
    new_data = pd.read_csv(new_data, engine = 'python')

    #컬럼명 변경
    new_data.columns = ['SEQ', 'POWER_PLANT', 'DT_DISCOVERY', 'REGULATION', 'UNITS', 'PLACE', 'FACILITIES',     'CATEGORY', 'DT_INSTALL', 'OUTSIDE_COMMENT', 'REDISCOVERY']

    # 최종 분류모델 불러오기
    classification_model = pickle.load(open(model_name, "rb"))

    # 입력변수 형식 지정
    col_list = ['CATEGORY_설비운영', 'CATEGORY_설비투자', 'CATEGORY_인력&교육강화', 'CATEGORY_인허가&표식',     'OUTSIDE_COMMENT', 'FACILITIES_석회석 Silo', 'FACILITIES_윤활유/수질 실험실', 'FACILITIES_환경분석실',  'PLACE_석탄취급설비', 'PLACE_실험실', 'PLACE_폐기물보관장', 'PLACE_폐수처리장', 'REDISCOVERY',   'REGULATION_대기', 'REGULATION_수질', 'REGULATION_폐기물', 'REGULATION_화학물질', 'UNITS_#5~8',   'UNITS_#9,10']

    input_data = pd.DataFrame(columns = col_list)

    ### <신규 설비 데이터 one-hot-encoding>
    input_list = ['REGULATION', 'UNITS', 'PLACE', 'FACILITIES', 'CATEGORY', 'OUTSIDE_COMMENT',  'REDISCOVERY']
    dummy_new_data = pd.get_dummies(new_data[input_list])

    # 입력 데이터 설정
    input_data = pd.concat([input_data, dummy_new_data])
    input_data = input_data.fillna(0)
    input_data = input_data[col_list]

    # 위험도 분류 실행
    classification_result = classification_model.predict(input_data)

    # 신규 데이터에 위험도 컬럼 추가
    new_data['PRED_RESULT'] = list(classification_result)

    return new_data

def main():
    # MSSQL 접속
    conn = pymssql.connect(host="localhost", user='test_server', password='test', database='test_db')
    sql_query = 'SELECT * FROM [test_schema].[ENVIROMENT_RISK_FILE] WHERE USE_AT=\'N\' AND FINISH_AT=\'N\';'
    file_df = pd.read_sql(sql=sql_query, con=conn)
    cursor = conn.cursor()

    file_df['SEQ'] = file_df['SEQ'].astype(int)
    SEQ = file_df['SEQ']
    FILE_TYPE = file_df['FILE_TYPE']
    model_path = 'D:/test/best_model.pickle.dat'

    for i, SN in enumerate(SEQ):
        try:
            # 테스트데이터
            if FILE_TYPE[i] == 'TEST':
                sql_query = "TRUNCATE TABLE [test_schema].[ENVIROMENT_RISK_TEST]"
                cursor.execute(sql_query)
                conn.commit() 

                file_path = file_df['FILE_PATH'][i]
                result_df = modeling(file_path, model_path)
                sql_query = "INSERT INTO [test_schema].[ENVIROMENT_RISK_TEST] VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                result_data = [tuple(x) for x in result_df.values]
                cursor.executemany(sql_query, result_data)
                conn.commit()

                sql_query = "UPDATE TEST_SCHEMA.ENVIROMENT_RISK_FILE SET USE_AT='Y', FINISH_AT='Y' WHERE SEQ=%d;" %  SN
                cursor.execute(sql_query)
                conn.commit()

            # 현황데이터
            elif FILE_TYPE[i] == 'REAL':
                sql_query = "TRUNCATE TABLE [test_schema].[ENVIROMENT_RISK_REAL]"
                cursor.execute(sql_query)
                conn.commit() 

                file_path = file_df['FILE_PATH'][i]
                result_df = pd.read_csv(file_path, engine = 'python')

                for col in result_df.columns:
                    result_df[col] = result_df[col].astype("str")

                result_df['GRADE_RISK_BEFORE'] = "NONE"

                for i, (comment, risk) in enumerate(zip(result_df['OUTSIDE_COMMENT'], result_df['RISK_BEFORE'])):
                    if comment == '1':
                        result_df.loc[i, 'GRADE_RISK_BEFORE'] = 'A'
                    else:
                        if float(risk) < 2.4:
                            result_df.loc[i, 'GRADE_RISK_BEFORE'] = 'C'
                        elif float(risk) >= 2.4 and float(risk) < 3.8:
                            result_df.loc[i, 'GRADE_RISK_BEFORE'] = 'B'
                        else:
                            result_df.loc[i, 'GRADE_RISK_BEFORE'] = 'A'

                sql_query = "INSERT INTO [test_schema].[ENVIROMENT_RISK_REAL] VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                result_data = [tuple(x) for x in result_df.values]
                cursor.executemany(sql_query, result_data)
                conn.commit()

                sql_query = "UPDATE [test_schema].[ENVIROMENT_RISK_FILE] SET USE_AT='Y', FINISH_AT='Y' WHERE SEQ=%d;" %  SN
                cursor.execute(sql_query)
                conn.commit()

        except Exception as errorMessage:
            # print(errorMessage)
            sql_query = "UPDATE [test_schema].[ENVIROMENT_RISK_FILE] SET USE_AT='N', FINISH_AT='Y' WHERE SEQ=%d;" %  SN
            cursor.execute(sql_query)
            conn.commit()

    # 연결 끊기
    conn.close()
    # print("Finished..!")

if __name__=="__main__":
    main()