# Using the Spoonacular API

## Overview

The Spoonacular API allows you to access a wide range of food-related data, including recipes, ingredients, and nutritional information. This guide will help you get started with using the API, including constructing requests, using new parameters, and understanding your API usage.

## Spoonacular API Key

### Obtain the API Key

1. Go to the Spoonacular website at https://spoonacular.com/food-api
2. Navigate to "Start Now" section.
3. If you don't have an account, you'll need to create one. Look for a "Sign Up" or "Create Account" option on the website.
4. After creating an account, log in with your credentials.
5. Once logged in, look for an "Profile" section. This is where you can access information about Spoonacular APIs and obtain your API key.
6. Inside the "Profile" section, there should be information on how to generate an API key.
7. Once generated, you'll likely see your API key on the website. It's usually a long string of characters. Copy this key, as you'll need it in your Python script to make requests to the Spoonacular API and fetch data.

![image](https://github.com/Maryem6/ETL-Python-PostgreSQL/assets/96294018/f7caa2be-37e9-46f0-9efd-47ab6af2ae24)

### Use the key in your Python script

To authenticate with the Spoonacular API, you need an API key. This key should be kept secure and included in your requests. Here’s how you define your API key in your code:

```python
API_KEY = 'your_api_key_here'
```

##  Constructing API Requests

The base URL for the Spoonacular API is https://api.spoonacular.com . Depending on the endpoint you want to access, you will append specific paths and parameters.

You can retrieve the needed endpoint from the api documentation: 
https://spoonacular.com/food-api/docs#Search-Recipes-Complex

In my project, I extracted random recipes using the `/recipes/random` endpoint

You need to include your API key as a query parameter. Here’s how to construct the URL in Python:

```python
API_URL = 'https://api.spoonacular.com/recipes/random?apiKey=' + API_KEY

```

There are several other endpoints and parameters you can use which are cited in the documentation 

## Monitoring API Usage

To monitor your API usage, you can use the API Console provided by Spoonacular. This console shows statistics about your requests, including the number of requests made and your daily quota.

- **Daily Quota:** The maximum number of requests you can make per day.
- **Points:** The cost of each request in points.
- **Requests:** The total number of requests you have made.

By keeping track of these metrics, you can manage your usage effectively and avoid exceeding your daily limits.

![image](https://github.com/Maryem6/ETL-Python-PostgreSQL/assets/96294018/623d4f61-550c-4272-a1f7-f7e151dbb6da)


## Conclusion

The Spoonacular API is a powerful tool for accessing a wide range of food-related data. By understanding how to construct requests, use new parameters, and monitor your usage, you can make the most of the API in your applications.












