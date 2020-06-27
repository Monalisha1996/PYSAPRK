from pyspark.sql import SparkSession
from pyspark.sql import functions as Func


def getCount(hr_data):
    return hr_data[(hr_data.satisfaction_level > .9) & (hr_data.salary == "low")].count()

def increaseSalary(hr_data):
    hr_data = hr_data.withColumn('ActualSalary', hr_data.last_evaluation * 10000)
    hr_data = hr_data.withColumn('multifactor', 
                             Func.when(hr_data.salary == "low",1)
                              .when(hr_data.salary == "medium",2)
                              .otherwise(3))
    hr_data = hr_data.withColumn('ActualSalary', hr_data.ActualSalary * hr_data.multifactor)
    hr_data = hr_data.drop('multifactor')


if __name__ == '__main__':

    spark = SparkSession.builder.appName('DataAnalysis').getOrCreate()
    print('Session created')

    hr_data = spark.read.csv('HR_comma_sep.csv',header=True)
 
    hr_data = hr_data.withColumnRenamed("sales","department")
    
    hr_data = hr_data.cache()
    count = getCount(hr_data)
    

    increaseSalary(hr_data)
    print (hr_data.show())
    #find count of hardworking and less paid folks
    print ('Count of hardworking & Less Paid folks ',count)
    #store output in local folder
    hr_data.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('file:hr_output.csv')
