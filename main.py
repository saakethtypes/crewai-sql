from groot import Groot, Agent, Task
from textwrap import dedent
import time
import json 
from pydantic import BaseModel
from fastapi import FastAPI
import os

import requests
from langchain.tools import tool
from sqlalchemy import create_engine, inspect
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import re

app = FastAPI()
connection_uri = "postgresql://<user>:<pass>@<host:port>/<db>"
# pip install langchain openai uvicorn fastapi sqlalchemy pandas pydantic psycopg2
class SQLTools():
  def extract_json(input_string):
    # Regular expression to find JSON objects
    json_regex = r'\{.*?\}'
    matches = re.findall(json_regex, input_string, re.DOTALL)

    if matches:
        # Assuming you want to extract the first JSON object found
        try:
            return json.loads(matches[0])
        except json.JSONDecodeError:
            print("Found string is not a valid JSON.")
            return None
    else:
        print("No JSON found in the input string.")
        return None

  @tool("Give db information result in json format")
  def get_table_list(sqlschema = "public"):
    """Useful to get table names"""
    connection_uri = "postgresql://<user>:<pass>@<host:port>/<db>"
    engine = create_engine(connection_uri)
    sql = f"""
        SELECT table_name from INFORMATION_SCHEMA.tables where table_schema = '{sqlschema}' ORDER BY table_name
    """
    with engine.connect() as connection:
        rows = list(connection.execute(text(sql)))
        return [r[0] for r in  rows]
#and give the result in json format OUTPUT Result PUERLY IN JSON FORMAT. Add your own judjement of 1-5 rating as a new feild farm_health in resulting json along with items from results
  @tool("Execute SQL query and give OUTPUT only the sql code in json format withe key being sql_code")
  def exec_sql(query):
    """Useful to execute SQL queries and give the result in json format Execute SQL query only the sql code in json format withe key being sql_code """
    # connection_uri = "postgresql://ukMqtt_user:bvWkDNvmVI7xQT38Ir@185.136.157.11:5432/ukMqtt"
    try:
      query = json.loads(query) 
    except Exception as e:
      print("Failed to load json", e)
      pass

    query = query['sql_code'] if 'sql_code' in query else query['query'] if 'query' in query else query
    engine = create_engine(connection_uri)

    if query and query.lower().startswith("select"):
        try:
          last_results = pd.read_sql_query(f"""{query}""", engine)
          print(last_results.to_json())
          if last_results.shape[0] > 20:
              return last_results.head(20).to_json()
          else:
              return last_results.to_json()
        except Exception as e:
          return {"error", f" Database query failed: {e}"}
    else:
        print("Invalid query: ", query)
        return {"error": f"Failed to run non-select query Check your query with syntax , logic and relavancy to the NLQ '{query}' also only the sql code in json format withe key being sql_code"}
    

class UrbiAgent():
  def db_agent(self):
    return Agent(
        role='SQL Coder',
        goal='Provide the BEST sql code and then execute to get the results for the nlq by the user.. while coding while parsing the table names or field names wrap them around double qoutes which  is very important. for conditionals use CAST(feild_name AS INTEGER) MAIN GOAL :: give me the result in json format using your tool',
        backstory="""A knowledgeable sql developer with extensive information
        about professional sql coding best practices and standards""",
        tools=[
            SQLTools.exec_sql,
        ],
        verbose=True)

  def agent_friend(self):
    return Agent(
        role='SQL Coder',
        goal='Provide the BEST sql code to other agents remind them to while parsing the table names or field names wrap them around double qoutes which  is very important or user and then execute to get the results for the nlq by the user MAIN GOAL :: give me the result in json format using your tool',
        backstory="""A knowledgeable sql developer with extensive information 
        about professional sql coding best practices and standards""",
        tools=[],
        verbose=True)


class UrbiTasks():
  def gen_sql(self,agent, db_context,nlq):
    return Task(description=dedent(f"""
        {db_context} -> given this context, generate a SQL query for the {nlq} natural language query AND THEN MAINLY USE YOUR TOOL TO EXECUTE THE QUERY USING THE TOOL AND GIVE THE RESULTS
      """),
                agent=agent)

class GrootHS:

  def __init__(self, db_context, nlq):
    self.db_context = db_context
    self.nlq = nlq

  def run(self):
    agents = UrbiAgent()
    tasks = UrbiTasks()

    db_agent = agents.db_agent()
    agent_friend = agents.agent_friend()
    
    coder_task = tasks.gen_sql(
      db_agent,
      self.db_context,
      self.nlq
    )
    
    groot = Groot(
      agents=[
         db_agent,
         agent_friend
      ],
      tasks=[coder_task],
      verbose=True
    )

    result = groot.run_holarchy()
    return result

def chat_db(table_name="temp",limit = 5):
    
    context_sql = """
        DO $$
        DECLARE
            r RECORD;
        BEGIN
            -- Create a temporary table with a single JSON column
            CREATE TEMP TABLE IF NOT EXISTS temp_results (data JSON);

            -- Clear the temporary table
            TRUNCATE TABLE temp_results;

            FOR r IN SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
            LOOP
                -- Insert the results as JSON into the temporary table
                EXECUTE 'INSERT INTO temp_results SELECT row_to_json(t.*) FROM (SELECT * FROM ' || quote_ident(r.table_name) || ' LIMIT 1) t';
            END LOOP;
        END $$;

        -- Select from the temporary table
        SELECT * FROM temp_results;
    """
    print("We are here")
    engine = create_engine(connection_uri)
    # Execute the SQL
    con_results = pd.read_sql_query(f"""{context_sql}""", engine)
    print(con_results.to_json())
    # context_data = 
    context_data_string = "Given context a few samples from the database samples provided -> " + json.dumps(con_results.to_json())
    
    return  context_data_string + "MAIN GOAL :: give me the result in json format using your tool"




class QueryRequest(BaseModel):
    nlq: str

@app.post("/ask_farm")
async def urbi_ask(request: QueryRequest):
  print('-------------------------------')
  tablenames = "Farm_growareas_log_1"
  nlq = request.nlq #"How many rows have a temperature greater than 30? MAIN GOAL :: give me the result in json format using your tool"
  db_context = chat_db() +"For the table name: {tablenames} " 
  ghs = GrootHS(db_context,nlq)
  result = ghs.run()
  print(result)
  return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)