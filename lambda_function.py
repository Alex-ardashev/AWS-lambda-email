import json
import boto3
import pandas as pd
import numpy as np
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def build_table(df, color='#305496', font_size='medium', font_family='Century Gothic', text_align='left'):
    # setting color
    padding = "0px 20px 0px 0px"
    even_background_color = '#FFFFFF'
    color = '#305496'
    border_bottom = '2px solid #305496'
    odd_background_color = '#D9E1F2'
    header_background_color = '#FFFFFF'

    # build html table
    a = 0
    while a != len(df):
        if a == 0:
            df_html_output = df.iloc[[a]].to_html(na_rep="", index=False, border=0)
            # change format of header
            df_html_output = df_html_output.replace('<th>'
                                                    , '<th style = "background-color: ' + header_background_color
                                                    + ';font-family: ' + font_family
                                                    + ';font-size: ' + str(font_size)
                                                    + ';color: ' + color
                                                    + ';text-align: ' + text_align
                                                    + ';border-bottom: ' + border_bottom
                                                    + ';padding: ' + padding + '">')

            # change format of table
            df_html_output = df_html_output.replace('<td>'
                                                    , '<td style = "background-color: ' + odd_background_color
                                                    + ';font-family: ' + font_family
                                                    + ';font-size: ' + str(font_size)
                                                    + ';text-align: ' + text_align
                                                    + ';padding: ' + padding + '">')

            body = """<p>""" + format(df_html_output)

            a = 1

        elif a % 2 == 0:
            df_html_output = df.iloc[[a]].to_html(na_rep="", index=False, header=False)

            # change format of table
            df_html_output = df_html_output.replace('<td>'
                                                    , '<td style = "background-color: ' + odd_background_color
                                                    + ';font-family: ' + font_family
                                                    + ';font-size: ' + str(font_size)
                                                    + ';text-align: ' + text_align
                                                    + ';padding: ' + padding + '">')

            body = body + format(df_html_output)

            a += 1

        elif a % 2 != 0:
            df_html_output = df.iloc[[a]].to_html(na_rep="", index=False, header=False)

            # change format of table
            df_html_output = df_html_output.replace('<td>'
                                                    , '<td style = "background-color: ' + even_background_color
                                                    + ';font-family: ' + font_family
                                                    + ';font-size: ' + str(font_size)
                                                    + ';text-align: ' + text_align
                                                    + ';padding: ' + padding + '">')

            body = body + format(df_html_output)

            a += 1

    body = body + """</p>"""

    body = body.replace("""</td>
    </tr>
  </tbody>
</table>
            <table border="1" class="dataframe">
  <tbody>
    <tr>""", """</td>
    </tr>
    <tr>""").replace("""</td>
    </tr>
  </tbody>
</table><table border="1" class="dataframe">
  <tbody>
    <tr>""", """</td>
    </tr>
    <tr>""")

    return body
        
  
def lambda_handler(event, context):
   #(bucket, initial_dataset(timestamp, id), forecast(date, p10 p50 p90) ) 
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-west-2',
        aws_access_key_id='xxxxx',
        aws_secret_access_key='xxxxx'
    )

    # get dataset_fact
    obj = s3.Bucket(event['key3']).Object(event['key4']).get()
    dataset_fact = pd.read_csv(obj['Body'])
    # df = df.rename(columns={'ds': 'timestamp', 'target_value': 'c'})
    dataset_fact['item_id'] = '1'

    # get df_forecast
    obj = s3.Bucket(event['key3']).Object(event['key5']).get()
    forecast = pd.read_csv(obj['Body'])
    forecast['date'] = pd.to_datetime(forecast['date'], errors='coerce')
    forecast['date'] = forecast['date'].dt.strftime('%Y-%m-%d')
    
    if forecast['date'].iloc[-1] == dataset_fact['timestamp'].iloc[-1]:
        #invoke somehow initial script
        print ('peepo')
    else:
        # anomaly
        df_merged = pd.merge(dataset_fact, forecast, how='left', left_on='timestamp', right_on='date')
        df_merged['Anomaly?'] = np.where(
            (df_merged['target_value'] < df_merged['p10']) | (df_merged['target_value'] > df_merged['p90']), 'Yes', 'No')
        df_merged = df_merged[['date', 'p10', 'p50', 'p90', 'target_value', 'Anomaly?']].tail(1)
        df_merged.columns = ['date', 'Lowest expected value', 'Expected prediction', 'Highest expected value',
                             'Actual value', 'Anomaly']


    #make table and pull it to email body
####################################################################################################################    
####################################################################################################################
    email_body = build_table(df_merged)

    # email attributes, connection to gmail
    message = MIMEMultipart()
    message['Subject'] = event['key1']
    user = "xxxx"
    password = "xxxx"
    message['from'] = user
    message['To'] = event['key2']

    body_content = email_body
    message.attach(MIMEText(body_content, "html"))
    msg_body = message.as_string()

    server = SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(message['From'], password)
    server.sendmail(message['From'], message['To'], msg_body)
    server.quit()
    return
