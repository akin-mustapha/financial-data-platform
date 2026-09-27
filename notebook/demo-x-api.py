import numpy as np
import pandas as pd
from xdk import Client
import json
import requests
from datetime import datetime

x_api_key = "AAAAAAAAAAAAAAAAAAAAADtFVQEAAAAAITt2xVBVcJNYmsX77WYlwZCHDq8%3DNbfP59p1nc2jk4X6z6RgwLIJryw1LStUOyjoB8kdnnxBuW90Dd"


if __name__ == "__main__":
    # Initialize the XDK client
  client = Client(
    bearer_token=x_api_key
  )

  for res in client.posts.search_recent(
    query="ADBE",
    max_results=10
  ):
  
    for post in res.data:
      print(post)
  print("Fetching tweets...")