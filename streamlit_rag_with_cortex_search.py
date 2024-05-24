# Import python packages
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, call_function
from snowflake.cortex import Complete

from snowflake.core import Root

import json 

# Write directly to the app
st.title("	:chart_with_upwards_trend: 10-K Explorer")

# connect to Snowflake
@st.cache_resource()
def get_snowflake_session():
    connection_parameters = json.load(open('connection.json'))
    session = Session.builder.configs(connection_parameters).create()
    session.sql_simplifier_enabled = True

    root = Root(session)
    search_service = (root
                    .databases["SEC_CORTEX_DEMO"]
                    .schemas["PUBLIC"]
                    .cortex_search_services["SEC_10K_SEARCH_SERVICE"]
    )
    return session, search_service

session, search_service = get_snowflake_session()

with st.sidebar:
    selected_model = st.selectbox("Select LLM",
                                  [
                                      'mistral-7b',
                                      'gemma-7b',
                                      'reka-core',
                                      'reka-flash',
                                      'mistral-large',
                                      'mixtral-8x7b',
                                      'llama2-70b-chat',
                                  ]      
                                )
    
    use_rag = st.checkbox("Use RAG?", True)

    if use_rag:
        num_rag_results = st.slider("Number of Retrieval Results", 1, 25, 3)


reports = session.table('sec_reports_base')
companies = reports.select(col('COMPANY_NAME')).distinct().sort([(col("COMPANY_NAME")=="SNOWFLAKE INC.").desc(), col("COMPANY_NAME")])

# user input
selected_company = st.selectbox("Company", companies)
question = st.text_input("Enter Question:")

if question: 

    if not use_rag:
        llm_resp = Complete(selected_model, question)
        st.write(llm_resp)
    
    else: 
        retrieval = search_service.search(
                                    question,
                                    columns=["CONTENT_CHUNK","SEC_DOCUMENT_ID"],
                                    filter={"@eq": {"COMPANY_NAME": selected_company} },
                                    limit=num_rag_results,
                    )
        
        context = '\n'.join([c["CONTENT_CHUNK"] for c in retrieval.results])

        prompt = f"""
        Use the context provided to answer the question. Limit your response to 40 words. 
        Context:
        ####
        {context}
        ####
        Question: {question}
        """

        question_response = Complete(selected_model, prompt)

        st.write(question_response.replace("$","\$"))

        with st.expander("See context from original document", expanded=False):
            
            for r in retrieval.results:
                st.markdown(f'''
                    **Document ID:** {r['SEC_DOCUMENT_ID']}  
                    **Context from Document:**  
                    {r['CONTENT_CHUNK']}
                '''.replace("$","\$"))
