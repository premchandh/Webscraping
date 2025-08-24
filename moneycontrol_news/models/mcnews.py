# models/mcnews.py
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class News(BaseModel):
    """
    Represents the basic data structure of a News article extracted by the LLM.
    Field names match the LLM's output (case-sensitive).
    """
    title: str = Field(..., description="The title of the news article.")
    description: str = Field(..., description="A brief description or summary of the article.")
    publishtime: str = Field(..., description="The publication time of the article as a string.")
    url: str = Field(..., description="The URL of the news article.")
    provider: str = Field(..., description="The name of the website providing the news.")
    # You can add an optional 'id' field if you plan to generate one later
    # id: Optional[str] = Field(None, description="A unique identifier for the news item.")

class DetailedNews(BaseModel):
    """
    Represents a more detailed data structure of a News article,
    potentially for a different extraction stage or source.
    """
    title: str = Field(..., description="The title of the news article.")
    shortdescription: str = Field(..., description="A short description of the article.")
    detaileddescription: str = Field(..., description="A comprehensive description of the article.")
    datetime: str = Field(..., description="The exact date and time of the article's publication.")
    author: str = Field(..., description="The author of the news article.")
