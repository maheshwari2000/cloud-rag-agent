
import logging
from typing import Dict, Any
from http import HTTPStatus
from retreival import return_retriever
import json
import random

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def web_search(user_query: str):
    # Use API for the real news
    # from duckduckgo_search import DDGS
    # with DDGS() as ddgs:
    #     results = [r for r in ddgs.text(user_query, max_results=3)]
    #     print(results)
    #     return results
    # results = [f"{user_query} is trending at number 2 in ranking", f"$50M has been granted for {user_query}", f"US is the leading country in {user_query}"]
    # Random rankings
    rankings = [1, 2, 3, 5, 10]
    
    # Random funding amounts
    funding_amounts = ["$10M", "$25M", "$50M", "$100M", "$250M", "$500M", "$1B"]
    
    # Random countries
    countries = ["US", "China", "UK", "Germany", "Japan", "India", "France", "Canada", "Australia", "South Korea"]
    
    # Random verbs for trends
    trend_verbs = ["trending", "gaining attention", "making headlines", "rising in popularity", "dominating discussions"]
    
    # Random verbs for funding
    funding_verbs = ["has been granted", "has been allocated", "was awarded", "secured", "raised"]
    
    # Random leadership phrases
    leadership_phrases = ["is the leading country", "leads globally", "dominates the market", "is at the forefront", "ranks first"]
    
    # Additional news templates
    news_templates = [
        f"{user_query} {random.choice(trend_verbs)} at number {random.choice(rankings)} in ranking",
        f"{random.choice(funding_amounts)} {random.choice(funding_verbs)} for {user_query}",
        f"{random.choice(countries)} {random.choice(leadership_phrases)} in {user_query}",
        f"Breakthrough in {user_query} announced by researchers in {random.choice(countries)}",
        f"New {user_query} initiative receives {random.choice(funding_amounts)} in investments",
        f"Global {user_query} market expected to grow by {random.randint(10, 50)}% this year",
        f"Major conference on {user_query} scheduled for {random.choice(['Q1', 'Q2', 'Q3', 'Q4'])} 2025",
        f"Industry leaders call for increased focus on {user_query}",
        f"{random.choice(countries)} announces new policy framework for {user_query}",
        f"Study reveals {user_query} adoption increased by {random.randint(20, 80)}% in past year"
    ]
    
    # Randomly select 3 unique news items
    results = random.sample(news_templates, min(3, len(news_templates)))
    return results

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for processing Bedrock agent requests.
    
    Args:
        event (Dict[str, Any]): The Lambda event containing action details
        context (Any): The Lambda context object
    
    Returns:
        Dict[str, Any]: Response containing the action execution results
    
    Raises:
        KeyError: If required fields are missing from the event
    """
    try:
        action_group = event['actionGroup']
        function = event['function']
        message_version = event.get('messageVersion',1)
        parameters = event.get('parameters', [])

        params = {}
        for param in parameters:
            params[param['name']] = param['value']
        print("Paramas")
        print(function)
        print(params)

        if function == 'arxiv_search':
            retriever = return_retriever()
            papers = retriever.search(params['query'], k=3)
            print("Papers:", papers)
            result = {}
            i = 1
            if papers:
                for p in papers[::-1]:
                    text = (f"[Distance/Score: {p['score']:.4f}]\nTitle: {p['title']}\n" +
                        f"Date: {p['date']}\n" +
                        f"Authors: {p['authors']}\n" +
                        f"Abstract: {p['abstract']}\n" +
                        ("-" * 20))
                    result[f"Paper {i}"] = text
                    i+=1
            result = json.dumps(result)
        elif function == 'web_search':
            result = web_search(params['query'])
            print("Web",result)
            result = json.dumps(result)
        response_body = {
            'TEXT': {
                'body': result
            }
        }
        # print(result)

        action_response = {
            'actionGroup': action_group,
            'function': function,
            'functionResponse': {
                'responseBody': response_body
            }
        }
        response = {
            'response': action_response,
            'messageVersion': message_version
        }

        logger.info('Response: %s', response)
        return response

    except KeyError as e:
        logger.error('Missing required field: %s', str(e))
        return {
            'statusCode': HTTPStatus.BAD_REQUEST,
            'body': f'Error: {str(e)}'
        }
    except Exception as e:
        logger.error('Unexpected error: %s', str(e))
        return {
            'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR,
            'body': 'Internal server error'
        }
