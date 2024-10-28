import json
from urllib.parse import urljoin

import requests


def complete_prompt(api_base_url, proxy_url, prompt, n_predict=4096):
    """
    Completes a given prompt using the LLaMA.cpp HTTP Server API.

    Args:
    - api_base_url (str): The base URL of the API (e.g., http://api.serg.com)
    - proxy_url (str): The URL of the SOCKS5 proxy (e.g., socks5h://localhost:9050)
    - prompt (str): The prompt to be completed
    - n_predict (int, optional): The number of tokens to predict. Defaults to 4096.

    Returns:
    - response (dict): The API response containing the completion result.
    """

    # Construct the full API endpoint URL
    endpoint_url = urljoin(api_base_url, "completion")

    # Set up the proxy
    proxies = {"http": proxy_url, "https": proxy_url}

    # Prepare the payload
    payload = {
        "prompt": prompt,
        "n_predict": n_predict,
        # Add any other options as needed (e.g., temperature, top_k, etc.)
    }

    # Make the POST request
    try:
        response = requests.post(endpoint_url, proxies=proxies, json=payload, timeout=600)  # 10-minute timeout
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.exceptions.RequestException as err:
        print("Request Exception:", err)
        return None

    # Return the JSON response
    try:
        return response.json()
    except json.JSONDecodeError as err:
        print("JSON Decode Error:", err)
        return None


# Example usage
if __name__ == "__main__":
    api_base_url = "http://do5n2xm36umjdo2wb63aqpimzqgvgxdg7aexlvljb2itfe3nistifkyd.onion"
    proxy_url = "socks5h://localhost:9050"
    prompt_string = "Hello"
    n_tokens_to_predict = 128  # Optional, defaults to 4096 in the function

    result = complete_prompt(api_base_url, proxy_url, prompt_string, n_tokens_to_predict)
    if result:
        print(json.dumps(result, indent=4))  # Pretty print the JSON response
        print()
        print("Predicted tokens count:", result["tokens_predicted"])
        print("Output content:")
        print(result["content"])
