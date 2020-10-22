import psycopg2 as pg
import pandas.io.sql as psql
from flask import Flask, jsonify
from flask_cors import CORS


connection = pg.connect( "host=bootcamp-0001.postgres.database.azure.com port=5432 dbname=airline_delays user=post_admin@bootcamp-0001 password=Bootcamp10152020 sslmode=require")
 
dataframe = psql.read_sql("SELECT month ,'' as type,'' as var,count(month)   from airline_delays where arrdelay > 15 group by month", connection)


app = Flask(__name__)
CORS(app)
@app.route("/carriers")
def welcome():
    """List all available api routes."""
    
    df1 =dataframe.transpose()
    var0=0
    for col in df1:
        if col ==0 :
            print(df1[col]["count"])
            df1[col]["var"]=df1[col]["count"]/1000
            var0=df1[col]["count"]/1000
            df1[col]["type"]="bar"
            df1[col]["month"]="M" +str(df1[col]["month"] )
        else:
            df1[col]["var"]=var0 -df1[col]["count"]/1000
            df1[col]["type"]="var"
            var0 =(df1[col]["count"]-var0)/1000
            df1[col]["month"]="M" +str(df1[col]["month"])
       
    df2 = df1.transpose()
    df2= df2[['month','var','type']] 
    df2 = df2.rename(columns={'month':'label','var':'value'})   
    return (df2.to_json(orient="records"))



if __name__ == '__main__':
    app.run(debug=True)