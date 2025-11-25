import pandas as pd
import numpy as np
from datetime import datetime
import re
import logging
import json
from typing import Dict, List, Any, Tuple
from collections import Counter
import string

# NLP and Sentiment Analysis
try:
    import nltk
    from nltk.sentiment import SentimentIntensityAnalyzer
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    nltk.download('vader_lexicon', quiet=True)
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
except ImportError:
    print("Warning: NLTK not installed. Some features may not work.")
    nltk = None

# === ADVANCED E-COMMERCE REVIEW MINING ENGINE ===

class AdvancedReviewMiningEngine:
    """
    Advanced engine for mining and analyzing e-commerce customer review data
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize VADER sentiment analyzer
        self.vader_analyzer = None
        if nltk:
            try:
                self.vader_analyzer = SentimentIntensityAnalyzer()
            except:
                self.logger.warning("Could not initialize VADER analyzer")
        
        # Initialize text processing patterns
        self.patterns = {
            'email': re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            'phone': re.compile(r'(\d{3}-?\d{3}-?\d{4})'),
            'profanity': re.compile(r'\b(damn|hell|crap|stupid)\b', re.IGNORECASE),
            'positive_words': re.compile(r'\b(excellent|amazing|great|love|perfect|awesome|fantastic|wonderful|good|nice|satisfied|happy|pleased|recommend|best|solid|worth|impressed)\b', re.IGNORECASE),
            'negative_words': re.compile(r'\b(terrible|awful|hate|horrible|worst|disappointing|useless|broken|bad|poor|waste|regret|returned|defective|cheap|garbage|trash)\b', re.IGNORECASE),
            'product_aspects': re.compile(r'\b(quality|price|shipping|delivery|packaging|design|color|size|fit|comfort|durability|battery|camera|screen|sound|performance|build|material|value|service|support)\b', re.IGNORECASE),
            'emoji': re.compile(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\u2600-\u26FF\u2700-\u27BF]'),
            'negation': re.compile(r'\b(not|no|never|nothing|nobody|nowhere|neither|barely|hardly|scarcely|seldom|rarely|dont|doesn\'t|didn\'t|won\'t|wouldn\'t|shouldn\'t|couldn\'t|isn\'t|aren\'t|wasn\'t|weren\'t|hasn\'t|haven\'t|hadn\'t|can\'t|cannot)\b', re.IGNORECASE)
        }
        
        # Product aspects for detailed ABSA
        self.product_aspects = {
            'battery': ['battery', 'charge', 'charging', 'power', 'lasting', 'drain', 'life'],
            'camera': ['camera', 'photo', 'picture', 'video', 'lens', 'quality', 'recording'],
            'price': ['price', 'cost', 'expensive', 'cheap', 'value', 'worth', 'money', 'budget'],
            'delivery': ['delivery', 'shipping', 'arrived', 'fast', 'slow', 'quick', 'delayed'],
            'design': ['design', 'look', 'appearance', 'style', 'beautiful', 'ugly', 'attractive'],
            'quality': ['quality', 'build', 'material', 'sturdy', 'flimsy', 'solid', 'durable'],
            'performance': ['performance', 'speed', 'fast', 'slow', 'lag', 'smooth', 'responsive'],
            'service': ['service', 'support', 'help', 'staff', 'customer', 'representative']
        }
        
        # Emotion lexicon (basic)
        self.emotion_lexicon = {
            'joy': ['happy', 'joy', 'pleased', 'delighted', 'thrilled', 'excited', 'love', 'amazing', 'wonderful', 'fantastic', 'awesome', 'excellent', 'perfect'],
            'anger': ['angry', 'mad', 'furious', 'annoyed', 'frustrated', 'irritated', 'hate', 'terrible', 'awful', 'horrible', 'disgusting', 'outraged'],
            'sadness': ['sad', 'disappointed', 'depressed', 'unhappy', 'miserable', 'devastated', 'heartbroken', 'regret', 'sorrow'],
            'fear': ['afraid', 'scared', 'worried', 'anxious', 'nervous', 'concerned', 'terrified', 'panic'],
            'surprise': ['surprised', 'shocked', 'amazed', 'astonished', 'unexpected', 'wow', 'incredible', 'unbelievable'],
            'trust': ['trust', 'reliable', 'dependable', 'confident', 'secure', 'safe', 'solid', 'recommend']
        }
        
        # Slang dictionary
        self.slang_terms = {
            'positive': ['lit', 'fire', 'dope', 'sick', 'bomb', 'legit', 'goat', 'slaps', 'banging', 'clean', 'crisp', 'fresh'],
            'negative': ['trash', 'garbage', 'mid', 'ass', 'whack', 'bogus', 'wack', 'sus', 'cap', 'fake', 'basic']
        }
        
        # Emoji emotion mapping
        self.emoji_emotions = {
            '😀😃😄😁😆😅😂🤣😊😇🙂🙃😉😌😍🥰😘😗😙😚': 'joy',
            '😠😡🤬😤😾💢': 'anger', 
            '😢😭😪😥😰😨😱': 'sadness',
            '😱😨😰😟😧😦😮': 'fear',
            '😮😯😲😳🤯': 'surprise',
            '❤️💕💖💗💙💚💛🧡💜🖤🤍🤎💝💘💌': 'love'
        }

    def extract_complex_features(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        """Extract comprehensive advanced features from review data"""
        self.logger.info("Starting comprehensive feature extraction for reviews")

        enhanced_df = reviews_df.copy()
        
        # Basic text statistics
        enhanced_df['text_length'] = enhanced_df['content'].astype(str).str.len()
        enhanced_df['word_count'] = enhanced_df['content'].astype(str).str.split().str.len()
        enhanced_df['sentence_count'] = enhanced_df['content'].astype(str).str.count(r'\.') + 1
        
        # Token & Lexical Stats
        enhanced_df['token_count'] = enhanced_df['content'].astype(str).apply(self._get_token_count)
        enhanced_df['unique_word_count'] = enhanced_df['content'].astype(str).apply(self._get_unique_word_count)
        enhanced_df['avg_word_length'] = enhanced_df['content'].astype(str).apply(self._calculate_avg_word_length)
        
        # VADER Sentiment Scores
        if self.vader_analyzer:
            vader_scores = enhanced_df['content'].astype(str).apply(self._get_vader_scores)
            enhanced_df['vader_compound'] = [score['compound'] for score in vader_scores]
            enhanced_df['vader_pos'] = [score['pos'] for score in vader_scores]
            enhanced_df['vader_neg'] = [score['neg'] for score in vader_scores]
            enhanced_df['vader_neu'] = [score['neu'] for score in vader_scores]
        else:
            enhanced_df['vader_compound'] = 0.0
            enhanced_df['vader_pos'] = 0.0
            enhanced_df['vader_neg'] = 0.0
            enhanced_df['vader_neu'] = 1.0
        
        # Polarity–Rating Check
        enhanced_df['polarity_rating_disagree'] = enhanced_df.apply(self._check_polarity_rating_disagreement, axis=1)
        enhanced_df['disagreement_score'] = enhanced_df.apply(self._calculate_disagreement_score, axis=1)
        
        # Negation Count and Adjusted Sentiment
        enhanced_df['negation_count'] = enhanced_df['content'].astype(str).str.findall(self.patterns['negation']).str.len()
        enhanced_df['negation_adjusted_sentiment'] = enhanced_df.apply(self._adjust_sentiment_for_negation, axis=1)
        
        # Basic sentiment analysis (legacy)
        enhanced_df['positive_word_count'] = enhanced_df['content'].astype(str).str.findall(self.patterns['positive_words']).str.len()
        enhanced_df['negative_word_count'] = enhanced_df['content'].astype(str).str.findall(self.patterns['negative_words']).str.len()
        enhanced_df['sentiment_ratio'] = (enhanced_df['positive_word_count'] - enhanced_df['negative_word_count']) / (enhanced_df['word_count'] + 1)
        
        # Aspect Mentions (Basic ABSA)
        enhanced_df['mentioned_aspects'] = enhanced_df['content'].astype(str).apply(self._extract_product_aspects)
        enhanced_df['aspect_count'] = enhanced_df['mentioned_aspects'].str.len()
        
        # Individual aspect sentiment analysis
        for aspect in self.product_aspects.keys():
            enhanced_df[f'aspect_{aspect}_sentiment'] = enhanced_df['content'].astype(str).apply(
                lambda text: self._get_aspect_sentiment(text, aspect)
            )
        
        # Emotion Detection (Lexicon-Based)
        emotion_scores = enhanced_df['content'].astype(str).apply(self._detect_emotions)
        for emotion in self.emotion_lexicon.keys():
            enhanced_df[f'emotion_{emotion}'] = [scores.get(emotion, 0) for scores in emotion_scores]
        enhanced_df['dominant_emotion'] = [max(scores.items(), key=lambda x: x[1])[0] if scores else 'neutral' for scores in emotion_scores]
        
        # Emoji Detection
        enhanced_df['emoji_count'] = enhanced_df['content'].astype(str).str.findall(self.patterns['emoji']).str.len()
        enhanced_df['has_emoji'] = enhanced_df['emoji_count'] > 0
        
        # Emoji–Emotion / Tone Analysis
        emoji_emotions = enhanced_df['content'].astype(str).apply(self._analyze_emoji_emotion)
        enhanced_df['emoji_emotion'] = [emotion['dominant'] for emotion in emoji_emotions]
        enhanced_df['emoji_emotion_scores'] = emoji_emotions
        
        # Slang Detection
        slang_analysis = enhanced_df['content'].astype(str).apply(self._detect_slang)
        enhanced_df['slang_count'] = [analysis['count'] for analysis in slang_analysis]
        enhanced_df['slang_terms'] = [analysis['terms'] for analysis in slang_analysis]
        
        # Content quality indicators
        enhanced_df['has_profanity'] = enhanced_df['content'].astype(str).str.contains(self.patterns['profanity'])
        enhanced_df['has_personal_info'] = (
            enhanced_df['content'].astype(str).str.contains(self.patterns['email']) |
            enhanced_df['content'].astype(str).str.contains(self.patterns['phone'])
        )
        
        # Temporal features
        enhanced_df['review_age_days'] = (datetime.now() - pd.to_datetime(enhanced_df['review_date'], errors='coerce')).dt.days
        enhanced_df['is_recent_review'] = enhanced_df['review_age_days'] <= 30
        
        # Advanced quality scoring
        enhanced_df['comprehensive_quality_score'] = self._calculate_comprehensive_quality_score(enhanced_df)
        
        self.logger.info(f"Feature extraction completed. Generated {len(enhanced_df.columns) - len(reviews_df.columns)} new features")
        
        return enhanced_df

    def _calculate_avg_word_length(self, text: str) -> float:
        if not text or pd.isna(text):
            return 0.0
        words = text.split()
        if not words:
            return 0.0
        return sum(len(word) for word in words) / len(words)

    def _extract_product_aspects(self, text: str) -> List[str]:
        """Extract mentioned product aspects"""
        if not text:
            return []
        text_lower = str(text).lower()
        mentioned = []
        for aspect, keywords in self.product_aspects.items():
            if any(keyword in text_lower for keyword in keywords):
                mentioned.append(aspect)
        return mentioned
    
    def _get_token_count(self, text: str) -> int:
        """Get token count using NLTK tokenizer"""
        if not nltk or not text:
            return len(str(text).split())
        try:
            tokens = word_tokenize(str(text).lower())
            return len([token for token in tokens if token.isalnum()])
        except:
            return len(str(text).split())
    
    def _get_unique_word_count(self, text: str) -> int:
        """Get unique word count"""
        if not text:
            return 0
        words = str(text).lower().split()
        return len(set(word.strip(string.punctuation) for word in words if word.isalnum()))
    
    def _get_vader_scores(self, text: str) -> Dict[str, float]:
        """Get VADER sentiment scores"""
        if not self.vader_analyzer or not text:
            return {'compound': 0.0, 'pos': 0.0, 'neg': 0.0, 'neu': 1.0}
        try:
            scores = self.vader_analyzer.polarity_scores(str(text))
            return scores
        except:
            return {'compound': 0.0, 'pos': 0.0, 'neg': 0.0, 'neu': 1.0}
    
    def _check_polarity_rating_disagreement(self, row) -> bool:
        """Check if sentiment polarity disagrees with rating"""
        try:
            rating = float(row.get('rating', 3.0))
            compound = float(row.get('vader_compound', 0.0))
            
            # High rating (4-5) but negative sentiment
            if rating >= 4.0 and compound < -0.1:
                return True
            # Low rating (1-2) but positive sentiment  
            elif rating <= 2.0 and compound > 0.1:
                return True
            return False
        except:
            return False
    
    def _calculate_disagreement_score(self, row) -> float:
        """Calculate magnitude of disagreement between rating and sentiment"""
        try:
            rating = float(row.get('rating', 3.0))
            compound = float(row.get('vader_compound', 0.0))
            
            # Normalize rating to -1 to 1 scale
            normalized_rating = (rating - 3.0) / 2.0
            
            # Calculate absolute difference
            return abs(normalized_rating - compound)
        except:
            return 0.0
    
    def _adjust_sentiment_for_negation(self, row) -> float:
        """Adjust sentiment score based on negation count"""
        try:
            original_sentiment = float(row.get('vader_compound', 0.0))
            negation_count = int(row.get('negation_count', 0))
            
            if negation_count == 0:
                return original_sentiment
            
            # Reduce positive sentiment or enhance negative sentiment
            adjustment_factor = 0.1 * negation_count
            if original_sentiment > 0:
                return max(original_sentiment - adjustment_factor, -1.0)
            else:
                return min(original_sentiment - adjustment_factor, -1.0)
        except:
            return 0.0
    
    def _get_aspect_sentiment(self, text: str, aspect: str) -> float:
        """Get sentiment for a specific product aspect"""
        if not text or aspect not in self.product_aspects:
            return 0.0
        
        text_lower = str(text).lower()
        aspect_keywords = self.product_aspects[aspect]
        
        # Check if aspect is mentioned
        aspect_mentioned = any(keyword in text_lower for keyword in aspect_keywords)
        if not aspect_mentioned:
            return 0.0
        
        # Get overall sentiment and assume it applies to the aspect
        if self.vader_analyzer:
            try:
                scores = self.vader_analyzer.polarity_scores(text)
                return scores['compound']
            except:
                pass
        
        # Fallback to basic sentiment
        positive_words = len(self.patterns['positive_words'].findall(text))
        negative_words = len(self.patterns['negative_words'].findall(text))
        if positive_words + negative_words == 0:
            return 0.0
        return (positive_words - negative_words) / (positive_words + negative_words)
    
    def _detect_emotions(self, text: str) -> Dict[str, int]:
        """Detect emotions using lexicon-based approach"""
        if not text:
            return {emotion: 0 for emotion in self.emotion_lexicon.keys()}
        
        text_lower = str(text).lower()
        emotion_scores = {}
        
        for emotion, words in self.emotion_lexicon.items():
            score = sum(1 for word in words if word in text_lower)
            emotion_scores[emotion] = score
        
        return emotion_scores
    
    def _analyze_emoji_emotion(self, text: str) -> Dict[str, Any]:
        """Analyze emotion based on emojis"""
        if not text:
            return {'dominant': 'neutral', 'emotions': {}}
        
        emoji_matches = self.patterns['emoji'].findall(str(text))
        if not emoji_matches:
            return {'dominant': 'neutral', 'emotions': {}}
        
        emotion_counts = {'joy': 0, 'anger': 0, 'sadness': 0, 'fear': 0, 'surprise': 0, 'love': 0}
        
        for emoji in emoji_matches:
            for emoji_set, emotion in self.emoji_emotions.items():
                if emoji in emoji_set:
                    emotion_counts[emotion] += 1
                    break
        
        if sum(emotion_counts.values()) == 0:
            return {'dominant': 'neutral', 'emotions': emotion_counts}
        
        dominant_emotion = max(emotion_counts.items(), key=lambda x: x[1])[0]
        return {'dominant': dominant_emotion, 'emotions': emotion_counts}
    
    def _detect_slang(self, text: str) -> Dict[str, Any]:
        """Detect slang terms in text"""
        if not text:
            return {'count': 0, 'terms': [], 'sentiment': 'neutral'}
        
        text_lower = str(text).lower()
        found_terms = []
        sentiment_score = 0
        
        for sentiment, terms in self.slang_terms.items():
            for term in terms:
                if term in text_lower:
                    found_terms.append(term)
                    sentiment_score += 1 if sentiment == 'positive' else -1
        
        overall_sentiment = 'neutral'
        if sentiment_score > 0:
            overall_sentiment = 'positive'
        elif sentiment_score < 0:
            overall_sentiment = 'negative'
        
        return {
            'count': len(found_terms),
            'terms': found_terms,
            'sentiment': overall_sentiment
        }

    def _calculate_comprehensive_quality_score(self, df: pd.DataFrame) -> pd.Series:
        quality_score = (
            (df['word_count'] / 100).clip(0, 1) * 0.30 +
            (df['text_length'] / 1000).clip(0, 1) * 0.20 +
            df['sentiment_ratio'].abs() * 0.15 +
            (1 - df['has_personal_info'].astype(int)) * 0.15 +
            (df['aspect_count'] / 10).clip(0, 1) * 0.20
        )
        return quality_score

    def generate_insights_report(self, enhanced_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive insights report with all advanced features"""
        insights = {
            'overall_statistics': {
                'total_reviews': len(enhanced_df),
                'unique_products': enhanced_df['product_name'].nunique() if 'product_name' in enhanced_df else 'N/A',
                'average_rating': float(enhanced_df['rating'].mean()) if 'rating' in enhanced_df else 'N/A',
                'rating_distribution': enhanced_df['rating'].value_counts().to_dict() if 'rating' in enhanced_df else {},
                'average_quality_score': float(enhanced_df['comprehensive_quality_score'].mean()),
                'average_sentiment': float(enhanced_df['vader_compound'].mean()) if 'vader_compound' in enhanced_df else 'N/A'
            },
            'content_analysis': {
                'avg_review_length': float(enhanced_df['text_length'].mean()),
                'avg_word_count': float(enhanced_df['word_count'].mean()),
                'avg_token_count': float(enhanced_df['token_count'].mean()) if 'token_count' in enhanced_df else 'N/A',
                'avg_unique_words': float(enhanced_df['unique_word_count'].mean()) if 'unique_word_count' in enhanced_df else 'N/A',
                'avg_word_length': float(enhanced_df['avg_word_length'].mean()),
                'reviews_with_profanity': int(enhanced_df['has_profanity'].sum()),
                'reviews_with_personal_info': int(enhanced_df['has_personal_info'].sum()),
                'reviews_with_emojis': int(enhanced_df['has_emoji'].sum()) if 'has_emoji' in enhanced_df else 0,
                'avg_emoji_count': float(enhanced_df['emoji_count'].mean()) if 'emoji_count' in enhanced_df else 0
            },
            'sentiment_analysis': {
                # VADER sentiment scores
                'avg_vader_compound': float(enhanced_df['vader_compound'].mean()) if 'vader_compound' in enhanced_df else 'N/A',
                'avg_vader_positive': float(enhanced_df['vader_pos'].mean()) if 'vader_pos' in enhanced_df else 'N/A',
                'avg_vader_negative': float(enhanced_df['vader_neg'].mean()) if 'vader_neg' in enhanced_df else 'N/A',
                'avg_vader_neutral': float(enhanced_df['vader_neu'].mean()) if 'vader_neu' in enhanced_df else 'N/A',
                
                # Polarity disagreement analysis
                'polarity_disagreements': int(enhanced_df['polarity_rating_disagree'].sum()) if 'polarity_rating_disagree' in enhanced_df else 0,
                'avg_disagreement_score': float(enhanced_df['disagreement_score'].mean()) if 'disagreement_score' in enhanced_df else 0,
                
                # Negation analysis\n                'avg_negation_count': float(enhanced_df['negation_count'].mean()) if 'negation_count' in enhanced_df else 0,\n                'reviews_with_negation': int((enhanced_df['negation_count'] > 0).sum()) if 'negation_count' in enhanced_df else 0,
                
                # Legacy sentiment metrics
                'positive_reviews_pct': float((enhanced_df['sentiment_ratio'] > 0.1).mean() * 100),
                'negative_reviews_pct': float((enhanced_df['sentiment_ratio'] < -0.1).mean() * 100),
                'neutral_reviews_pct': float((enhanced_df['sentiment_ratio'].abs() <= 0.1).mean() * 100)
            },
            'emotion_analysis': {},
            'product_aspects': {},
            'emoji_analysis': {},
            'slang_analysis': {
                'reviews_with_slang': 0,
                'avg_slang_count': 0,
                'positive_slang_usage': 0,
                'negative_slang_usage': 0
            },
            'quality_distribution': {
                'high_quality_pct': float((enhanced_df['comprehensive_quality_score'] > 0.7).mean() * 100),
                'medium_quality_pct': float(((enhanced_df['comprehensive_quality_score'] > 0.4) & (enhanced_df['comprehensive_quality_score'] <= 0.7)).mean() * 100),
                'low_quality_pct': float((enhanced_df['comprehensive_quality_score'] <= 0.4).mean() * 100)
            }
        }
        
        # Emotion analysis insights
        if any(col.startswith('emotion_') for col in enhanced_df.columns):
            emotion_cols = [col for col in enhanced_df.columns if col.startswith('emotion_')]
            insights['emotion_analysis'] = {
                col.replace('emotion_', ''): {
                    'total_mentions': int(enhanced_df[col].sum()),
                    'avg_per_review': float(enhanced_df[col].mean()),
                    'reviews_with_emotion': int((enhanced_df[col] > 0).sum())
                }
                for col in emotion_cols
            }
            
            # Dominant emotion distribution
            if 'dominant_emotion' in enhanced_df.columns:
                insights['emotion_analysis']['dominant_emotion_distribution'] = enhanced_df['dominant_emotion'].value_counts().to_dict()
        
        # Product aspects insights
        if any(col.startswith('aspect_') and col.endswith('_sentiment') for col in enhanced_df.columns):
            aspect_cols = [col for col in enhanced_df.columns if col.startswith('aspect_') and col.endswith('_sentiment')]
            aspects_mentioned = {}
            
            for aspect in self.product_aspects.keys():
                aspect_col = f'aspect_{aspect}_sentiment'
                if aspect_col in enhanced_df.columns:
                    # Count mentions (non-zero sentiment scores)
                    mentions = int((enhanced_df[aspect_col] != 0).sum())
                    aspects_mentioned[aspect] = mentions
                    
                    if mentions > 0:
                        insights['product_aspects'][aspect] = {
                            'mentions': mentions,
                            'avg_sentiment': float(enhanced_df[enhanced_df[aspect_col] != 0][aspect_col].mean()),
                            'positive_mentions': int((enhanced_df[aspect_col] > 0.1).sum()),
                            'negative_mentions': int((enhanced_df[aspect_col] < -0.1).sum())
                        }
            
            insights['product_aspects']['total_aspect_mentions'] = aspects_mentioned
        
        # Emoji emotion analysis
        if 'emoji_emotion' in enhanced_df.columns and 'emoji_emotion_scores' in enhanced_df.columns:
            emoji_emotion_dist = enhanced_df['emoji_emotion'].value_counts().to_dict()
            insights['emoji_analysis'] = {
                'emoji_emotion_distribution': emoji_emotion_dist,
                'reviews_with_emoji_emotions': int((enhanced_df['emoji_emotion'] != 'neutral').sum())
            }
        
        # Slang analysis
        if 'slang_count' in enhanced_df.columns:
            insights['slang_analysis'] = {
                'reviews_with_slang': int((enhanced_df['slang_count'] > 0).sum()),
                'avg_slang_count': float(enhanced_df['slang_count'].mean()),
                'total_slang_terms': int(enhanced_df['slang_count'].sum())
            }
        
        return insights

# === MAIN EXECUTION (GLUE CODE) ===

if __name__ == '__main__':
    import os
    from pathlib import Path
    
    # Ensure output directory exists
    output_dir = Path('data/processed')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Try to find the most recent ETL output file
    reviews_dir = Path('data/reviews')
    if reviews_dir.exists():
        parquet_files = list(reviews_dir.glob('*.parquet'))
        if parquet_files:
            # Use the most recent file
            etl_output_path = max(parquet_files, key=os.path.getctime)
            print(f"Found ETL output file: {etl_output_path}")
        else:
            print("No parquet files found in data/reviews directory")
            print("Please run the ETL pipeline first to generate data")
            exit(1)
    else:
        print("Data directory not found. Please run the ETL pipeline first.")
        exit(1)
    
    try:
        # Load the reviews data output from ETL_automation.py
        reviews_df = pd.read_parquet(etl_output_path)
        print(f'Loaded {len(reviews_df)} reviews for mining.')

        # Run mining engine
        engine = AdvancedReviewMiningEngine()
        enhanced_reviews = engine.extract_complex_features(reviews_df)

        # Save enhanced dataset
        enhanced_output_path = output_dir / 'enhanced_reviews.csv'
        enhanced_reviews.to_csv(enhanced_output_path, index=False)
        print(f'Enhanced review features saved to {enhanced_output_path}')

        # Generate and save insights
        insights = engine.generate_insights_report(enhanced_reviews)
        insights_output_path = output_dir / 'review_insights.json'
        
        import json
        with open(insights_output_path, 'w', encoding='utf-8') as f:
            json.dump(insights, f, indent=2, ensure_ascii=False, default=str)
        print(f'Insights report saved to {insights_output_path}')
        
        # Print insights report
        print('\n==== REVIEW MINING INSIGHTS REPORT ====')
        for section, section_data in insights.items():
            print(f'\n-- {section.upper()} --')
            for key, val in section_data.items():
                print(f'{key}: {val}')
                
    except FileNotFoundError as e:
        print(f"Error: Could not find the data file - {e}")
        print("Please ensure the ETL pipeline has been run and data files exist.")
    except Exception as e:
        print(f"Error during review mining: {e}")
        print("Check the file paths and data format.")
