from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# REGEX PATTERNS ---
REGEX_ALPHA = r'[a-zA-Z]+'
REGEX_EMPTY_STR = r'[\t ]+$'
REGEX_INTEGER = r'[0-9]+'


# --------------------

# Helper functions ---
def check_empty_column(col):
    return F.col(col).isNull() | (F.col(col) == '') | F.col(col).rlike(REGEX_EMPTY_STR)


def show_references():
    print('Disclaimer: The data on this work is available at the UCI Machine Learning Repository')
    print("""
                    References:
U. S. Department of Commerce, Bureau of the Census, Census Of Population And Housing 
1990 United States: Summary Tape File 1a & 3a (Computer Files),

U.S. Department Of Commerce, Bureau Of The Census Producer, Washington, DC and 
Inter-university Consortium for Political and Social Research Ann Arbor, Michigan. 
(1992)

U.S. Department of Justice, Bureau of Justice Statistics, Law Enforcement Management 
And Administrative Statistics (Computer File) U.S. Department Of Commerce, Bureau Of 
The Census Producer, Washington, DC and Inter-university Consortium for Political and 
Social Research Ann Arbor, Michigan. (1992)

U.S. Department of Justice, Federal Bureau of Investigation, Crime in the United 
States (Computer File) (1995)

Redmond, M. A. and A. Baveja: A Data-Driven Software Tool for Enabling Cooperative 
Information Sharing Among Police Departments. European Journal of Operational Research 
141 (2002) 660-678. 
    """)


def ensure_quality(df):
    data = df
    for column in df.columns:
        data = data.withColumn('qa_' + column, (
            F.when(
                check_empty_column(column), 'MN'
            ).when(
                F.col(column) == '?', 'M'
            ).otherwise(
                F.col(column)
            )
        ))
    return data


def transform_df(df):
    data = df
    for column in df.columns:
        if not column.startswith('qa_'):
            if column == 'communityname':
                pass
            else:
                data = data.withColumn(column, (
                    F.when(
                        check_empty_column(column), None
                    ).when(
                        F.col(column) == '?', None
                    ).when(
                        F.col(column).contains('.'), F.col(column).cast(FloatType())
                    ).otherwise(
                        F.col(column).cast(IntegerType())
                    )
                ))
    return data


def clean_dataframe(df):
    data = ensure_quality(df)
    data = transform_df(data)
    # df.printSchema()
    return data


def get_qa_summary(df):
    # show the frequency table for qa_ columns
    df.show()


# ------------------------------

# --------questions ------------
def question1(df):
    data = df.groupBy(F.col('state'), F.col('communityname')) \
        .agg(F.format_number(F.sum(F.col('PolicOperBudg')), 2).alias('orcamento_policial')) \
        .orderBy(F.col('orcamento_policial').desc()) \
        .limit(1)
    data.show()


def question2(df):
    data = df.groupBy(F.col('state'), F.col('community'), F.col('communityname')) \
        .agg(F.sum(F.col('ViolentCrimesPerPop')).alias('violent_crimes')) \
        .orderBy(F.col('violent_crimes').desc()) \
        .limit(1)
    data.show()


def question3(df):
    data = df.where(F.col('population') > 0) \
        .groupBy(F.col('state'), F.col('communityname')) \
        .agg(F.max(F.col('population')).alias('population_by_community')) \
        .orderBy(F.col('population_by_community').desc()) \
        .limit(1)
    data.show()
    print('--------------- The summ -------------')
    data = df.where(F.col('population') > 0) \
        .groupBy(F.col('state'), F.col('communityname')) \
        .agg(F.sum(F.col('population')).alias('population_by_community')) \
        .orderBy(F.col('population_by_community').desc()) \
        .limit(1)
    data.show()


def question4(df):
    print('Maior percentual de população negra.')
    data = df.where(F.col('racepctblack') > 0) \
        .groupBy(F.col('state'), F.col('communityname')) \
        .agg(F.max(F.col('racepctblack')).alias('blackplp_by_community_percent')) \
        .orderBy(F.col('blackplp_by_community_percent').desc()) \
        .limit(1)
    data.show()

    print('-' * 50)

    print('Maior quantidade de população negra.')
    data = df.where(F.col('racepctblack') > 0) \
        .groupBy(F.col('state'), F.col('communityname')) \
        .agg(F.max(F.col('population') * F.col('racepctblack')).alias('blackplp_by_community')) \
        .orderBy(F.col('blackplp_by_community').desc()) \
        .limit(1)
    data.show()


def question5(df):
    data = df.groupBy(F.col('state'), F.col('communityname')) \
        .agg(F.max(F.col('pctWWage')).alias('max_salary')) \
        .orderBy(F.col('max_salary').desc()) \
        .limit(1)
    data.show()


def question6(df):
    print('Se considerarmos os jovens na idade de 12 a 21:')
    data = df.groupBy(F.col('state'), F.col('communityname')) \
        .agg(F.max(F.col('agePct12t21')).alias('max_youngplp')) \
        .orderBy(F.col('max_youngplp').desc()) \
        .limit(1)
    data.show()

    print('-' * 50)

    print('Se considerarmos os jovens na idade de 16 a 24:')
    data = df.groupBy(F.col('state'), F.col('communityname')) \
        .agg(F.max(F.col('agePct16t24')).alias('max_youngplp')) \
        .orderBy(F.col('max_youngplp').desc()) \
        .limit(1)
    data.show()


def question7(df):
    polc_budgt_violent_crimes_corr = df.stat.corr('PolicOperBudg', 'ViolentCrimesPerPop')
    print("Correlação entre Orçamento Policial e Crimes violentos: \n", polc_budgt_violent_crimes_corr)


def question8(df):
    white_polc_budgt = df.stat.corr('PctPolicWhite', 'PolicOperBudg')
    print("Correlação entre Porcentagem de policiais caucasianos e Orçamento Policial: \n", white_polc_budgt)


def question9(df):
    popl_and_polc_budget_corr = df.stat.corr('population', 'PolicBudgPerPop')
    print("Correlação entre População e Orçamento Policial: \n", popl_and_polc_budget_corr)


def question10(df):
    popl_and_vlnt_crimes = df.stat.corr('population', 'ViolentCrimesPerPop')
    print("Correlação entre População e Número de crimes violentos: \n", popl_and_vlnt_crimes)


def question11(df):
    med_house_income_vlnt_crimes_corr = df.stat.corr('medIncome', 'ViolentCrimesPerPop')
    print("Correlação entre Mediana do salário familiar e Número de crimes violentos: \n",
          med_house_income_vlnt_crimes_corr)


def question12(df):
    data = df.filter(F.col('ViolentCrimesPerPop') > 0)
    greatest_crimeperpopl_community = data.where(F.col('population') > 0) \
        .groupBy(F.col('state'), F.col('communityname')) \
        .agg(F.max(F.col('population') * F.col('ViolentCrimesPerPop')).alias('crime_per_popl')) \
        .orderBy(F.col('crime_per_popl').desc()) \
        .limit(10) \
        .collect()
    for _row in greatest_crimeperpopl_community:
        print("Raça predominante em: ", _row['communityname'].replace('city', ''))
        data = df.filter(F.col('communityname') == _row['communityname'])
        data = data.withColumn('max_pctRace', (
            F.when(
                ((F.col('racepctblack') > F.col('racePctWhite')) &
                 (F.col('racepctblack') > F.col('racePctAsian')) &
                 (F.col('racepctblack') > F.col('racePctHisp'))), 'Black'
            ).when(
                (F.col('racePctWhite') > F.col('racepctblack')) &
                (F.col('racePctWhite') > F.col('racePctAsian')) &
                (F.col('racePctWhite') > F.col('racePctHisp')), 'Caucasian'
            ).when(
                (F.col('racePctAsian') > F.col('racepctblack')) &
                (F.col('racePctAsian') > F.col('racePctWhite')) &
                (F.col('racePctAsian') > F.col('racePctHisp')), 'Asian'
            ).when(
                (F.col('racePctHisp') > F.col('racepctblack')) &
                (F.col('racePctHisp') > F.col('racePctAsian')) &
                (F.col('racePctHisp') > F.col('racePctWhite')), 'Hisp'
            )
        ))
        result = data.groupBy(F.col('max_pctRace')) \
            .count() \
            .orderBy(F.col('count').desc()) \
            .limit(1) \
            .first()['max_pctRace']
        print('\t', result)


def solve_questions(df, method_name, _method):
    print("Questão", method_name[8:])
    _method(df)


# ------------------------------
if __name__ == "__main__":
    sc = SparkContext()
    spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

    schema_communities_crime = StructType([
        StructField("state", StringType(), True),
        StructField("county", StringType(), True),
        StructField("community", StringType(), True),
        StructField("communityname", StringType(), True),
        StructField("fold", StringType(), True),
        StructField("population", StringType(), True),
        StructField("householdsize", StringType(), True),
        StructField("racepctblack", StringType(), True),
        StructField("racePctWhite", StringType(), True),
        StructField("racePctAsian", StringType(), True),
        StructField("racePctHisp", StringType(), True),
        StructField("agePct12t21", StringType(), True),
        StructField("agePct12t29", StringType(), True),
        StructField("agePct16t24", StringType(), True),
        StructField("agePct65up", StringType(), True),
        StructField("numbUrban", StringType(), True),
        StructField("pctUrban", StringType(), True),
        StructField("medIncome", StringType(), True),
        StructField("pctWWage", StringType(), True),
        StructField("pctWFarmSelf", StringType(), True),
        StructField("pctWInvInc", StringType(), True),
        StructField("pctWSocSec", StringType(), True),
        StructField("pctWPubAsst", StringType(), True),
        StructField("pctWRetire", StringType(), True),
        StructField("medFamInc", StringType(), True),
        StructField("perCapInc", StringType(), True),
        StructField("whitePerCap", StringType(), True),
        StructField("blackPerCap", StringType(), True),
        StructField("indianPerCap", StringType(), True),
        StructField("AsianPerCap", StringType(), True),
        StructField("OtherPerCap", StringType(), True),
        StructField("HispPerCap", StringType(), True),
        StructField("NumUnderPov", StringType(), True),
        StructField("PctPopUnderPov", StringType(), True),
        StructField("PctLess9thGrade", StringType(), True),
        StructField("PctNotHSGrad", StringType(), True),
        StructField("PctBSorMore", StringType(), True),
        StructField("PctUnemployed", StringType(), True),
        StructField("PctEmploy", StringType(), True),
        StructField("PctEmplManu", StringType(), True),
        StructField("PctEmplProfServ", StringType(), True),
        StructField("PctOccupManu", StringType(), True),
        StructField("PctOccupMgmtProf", StringType(), True),
        StructField("MalePctDivorce", StringType(), True),
        StructField("MalePctNevMarr", StringType(), True),
        StructField("FemalePctDiv", StringType(), True),
        StructField("TotalPctDiv", StringType(), True),
        StructField("PersPerFam", StringType(), True),
        StructField("PctFam2Par", StringType(), True),
        StructField("PctKids2Par", StringType(), True),
        StructField("PctYoungKids2Par", StringType(), True),
        StructField("PctTeen2Par", StringType(), True),
        StructField("PctWorkMomYoungKids", StringType(), True),
        StructField("PctWorkMom", StringType(), True),
        StructField("NumIlleg", StringType(), True),
        StructField("PctIlleg", StringType(), True),
        StructField("NumImmig", StringType(), True),
        StructField("PctImmigRecent", StringType(), True),
        StructField("PctImmigRec5", StringType(), True),
        StructField("PctImmigRec8", StringType(), True),
        StructField("PctImmigRec10", StringType(), True),
        StructField("PctRecentImmig", StringType(), True),
        StructField("PctRecImmig5", StringType(), True),
        StructField("PctRecImmig8", StringType(), True),
        StructField("PctRecImmig10", StringType(), True),
        StructField("PctSpeakEnglOnly", StringType(), True),
        StructField("PctNotSpeakEnglWell", StringType(), True),
        StructField("PctLargHouseFam", StringType(), True),
        StructField("PctLargHouseOccup", StringType(), True),
        StructField("PersPerOccupHous", StringType(), True),
        StructField("PersPerOwnOccHous", StringType(), True),
        StructField("PersPerRentOccHous", StringType(), True),
        StructField("PctPersOwnOccup", StringType(), True),
        StructField("PctPersDenseHous", StringType(), True),
        StructField("PctHousLess3BR", StringType(), True),
        StructField("MedNumBR", StringType(), True),
        StructField("HousVacant", StringType(), True),
        StructField("PctHousOccup", StringType(), True),
        StructField("PctHousOwnOcc", StringType(), True),
        StructField("PctVacantBoarded", StringType(), True),
        StructField("PctVacMore6Mos", StringType(), True),
        StructField("MedYrHousBuilt", StringType(), True),
        StructField("PctHousNoPhone", StringType(), True),
        StructField("PctWOFullPlumb", StringType(), True),
        StructField("OwnOccLowQuart", StringType(), True),
        StructField("OwnOccMedVal", StringType(), True),
        StructField("OwnOccHiQuart", StringType(), True),
        StructField("RentLowQ", StringType(), True),
        StructField("RentMedian", StringType(), True),
        StructField("RentHighQ", StringType(), True),
        StructField("MedRent", StringType(), True),
        StructField("MedRentPctHousInc", StringType(), True),
        StructField("MedOwnCostPctInc", StringType(), True),
        StructField("MedOwnCostPctIncNoMtg", StringType(), True),
        StructField("NumInShelters", StringType(), True),
        StructField("NumStreet", StringType(), True),
        StructField("PctForeignBorn", StringType(), True),
        StructField("PctBornSameState", StringType(), True),
        StructField("PctSameHouse85", StringType(), True),
        StructField("PctSameCity85", StringType(), True),
        StructField("PctSameState85", StringType(), True),
        StructField("LemasSwornFT", StringType(), True),
        StructField("LemasSwFTPerPop", StringType(), True),
        StructField("LemasSwFTFieldOps", StringType(), True),
        StructField("LemasSwFTFieldPerPop", StringType(), True),
        StructField("LemasTotalReq", StringType(), True),
        StructField("LemasTotReqPerPop", StringType(), True),
        StructField("PolicReqPerOffic", StringType(), True),
        StructField("PolicPerPop", StringType(), True),
        StructField("RacialMatchCommPol", StringType(), True),
        StructField("PctPolicWhite", StringType(), True),
        StructField("PctPolicBlack", StringType(), True),
        StructField("PctPolicHisp", StringType(), True),
        StructField("PctPolicAsian", StringType(), True),
        StructField("PctPolicMinor", StringType(), True),
        StructField("OfficAssgnDrugUnits", StringType(), True),
        StructField("NumKindsDrugsSeiz", StringType(), True),
        StructField("PolicAveOTWorked", StringType(), True),
        StructField("LandArea", StringType(), True),
        StructField("PopDens", StringType(), True),
        StructField("PctUsePubTrans", StringType(), True),
        StructField("PolicCars", StringType(), True),
        StructField("PolicOperBudg", StringType(), True),
        StructField("LemasPctPolicOnPatr", StringType(), True),
        StructField("LemasGangUnitDeploy", StringType(), True),
        StructField("LemasPctOfficDrugUn", StringType(), True),
        StructField("PolicBudgPerPop", StringType(), True),
        StructField("ViolentCrimesPerPop", StringType(), True)
    ])

    dataframe = (spark.getOrCreate().read
                 .format("csv")
                 .option("header", "true")
                 .schema(schema_communities_crime)
                 .load("data/communities-crime/communities-crime.csv"))
    treated_df = clean_dataframe(dataframe)

    treated_df.show(10)

    get_qa_summary(treated_df)

    online_retail_questions = {
        "question1": question1,
        "question2": question2,
        "question3": question3,
        "question4": question4,
        "question5": question5,
        "question6": question6,
        "question7": question7,
        "question8": question8,
        "question9": question9,
        "question10": question10,
        "question11": question11,
        "question12": question12
    }

    for _name, _method in online_retail_questions.items():
        solve_questions(treated_df, _name, _method)

    show_references()
