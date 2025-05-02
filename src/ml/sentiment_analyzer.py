# Sentiment analyzer using HuggingFace, Vader, and SpaCy

import logging
import re
import spacy
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, FloatType

logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    """Sentiment analyzer using multiple NLP libraries"""
    
    def __init__(self):
        """Initialize sentiment analyzer"""
        # Initialize VADER sentiment analyzer
        self.vader = SentimentIntensityAnalyzer()
        
        # Initialize HuggingFace sentiment pipeline
        try:
            model_name = "distilbert-base-uncased-finetuned-sst-2-english"
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
            self.huggingface = pipeline("sentiment-analysis", model=self.model, tokenizer=self.tokenizer)
            logger.info("Initialized HuggingFace sentiment pipeline")
        except Exception as e:
            logger.error(f"Error initializing HuggingFace: {e}")
            self.huggingface = None
        
        # Initialize SpaCy
        try:
            self.nlp = spacy.load("en_core_web_sm")
            logger.info("Initialized SpaCy")
        except Exception as e:
            logger.error(f"Error initializing SpaCy: {e}")
            self.nlp = None
        
        # Create UDF for Spark
        self.analyze_sentiment_udf = udf(self.analyze_sentiment, 
                                        StructType([
                                            StructField("positive", FloatType()),
                                            StructField("neutral", FloatType()),
                                            StructField("negative", FloatType()),
                                            StructField("compound", FloatType())
                                        ]))
        
        logger.info("Initialized sentiment analyzer")
    
    def preprocess_text(self, text):
        """
        Preprocess text for sentiment analysis
        
        Args:
            text (str): Text to preprocess
            
        Returns:
            str: Preprocessed text
        """
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'https?://\S+|www\.\S+', '', text)
        
        # Remove mentions
        text = re.sub(r'@\w+', '', text)
        
        # Remove hashtags (keep the text)
        text = re.sub(r'#(\w+)', r'\1', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Use SpaCy for more advanced preprocessing if available
        if self.nlp:
            try:
                # Parse with SpaCy
                doc = self.nlp(text)
                
                # Remove stopwords and punctuation
                tokens = [token.text for token in doc if not token.is_stop and not token.is_punct]
                
                # Join tokens back into text
                text = " ".join(tokens)
            except Exception as e:
                logger.error(f"Error preprocessing with SpaCy: {e}")
        
        return text
    
    def analyze_sentiment(self, text):
        """
        Analyze sentiment of text
        
        Args:
            text (str): Text to analyze
            
        Returns:
            dict: Sentiment scores
        """
        if not text:
            return {"positive": 0.0, "neutral": 0.0, "negative": 0.0, "compound": 0.0}
        
        try:
            # Preprocess text
            preprocessed_text = self.preprocess_text(text)
            
            if not preprocessed_text:
                return {"positive": 0.0, "neutral": 0.0, "negative": 0.0, "compound": 0.0}
            
            # Get VADER sentiment
            vader_scores = self.vader.polarity_scores(preprocessed_text)
            
            # Get HuggingFace sentiment if available
            huggingface_score = 0.0
            if self.huggingface:
                try:
                    # Truncate text if too long (HuggingFace models have token limits)
                    if len(preprocessed_text) > 512:
                        preprocessed_text = preprocessed_text[:512]
                    
                    result = self.huggingface(preprocessed_text)[0]
                    
                    # Convert label to score
                    if result["label"] == "POSITIVE":
                        huggingface_score = result["score"]
                    else:
                        huggingface_score = 1.0 - result["score"]
                except Exception as e:
                    logger.error(f"Error getting HuggingFace sentiment: {e}")
            
            # Combine scores (giving more weight to VADER for social media text)
            if self.huggingface:
                # Adjust positive score with HuggingFace
                positive = (vader_scores["pos"] * 0.7) + (huggingface_score * 0.3)
                
                # Adjust negative score
                negative = (vader_scores["neg"] * 0.7) + ((1.0 - huggingface_score) * 0.3)
                
                # Adjust neutral score
                neutral = vader_scores["neu"] * 0.7
                
                # Normalize scores to sum to 1
                total = positive + negative + neutral
                if total > 0:
                    positive /= total
                    negative /= total
                    neutral /= total
                
                # Use VADER's compound score
                compound = vader_scores["compound"]
            else:
                # Use only VADER scores
                positive = vader_scores["pos"]
                neutral = vader_scores["neu"]
                negative = vader_scores["neg"]
                compound = vader_scores["compound"]
            
            return {
                "positive": float(positive),
                "neutral": float(neutral),
                "negative": float(negative),
                "compound": float(compound)
            }
        
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return {"positive": 0.0, "neutral": 0.0, "negative": 0.0, "compound": 0.0}
    
    def analyze_batch(self, texts):
        """
        Analyze sentiment of a batch of texts
        
        Args:
            texts (list): List of texts to analyze
            
        Returns:
            list: List of sentiment scores
        """
        return [self.analyze_sentiment(text) for text in texts]