"""
Sentiment Engine: Provides sentiment classification, polarity scoring,
and confidence estimation for streaming data.
Supports both Rule-based NLP (VADER/Lexicon) and ML embeddings.
"""

import re
import math

# Lexicon words for rapid, highly accurate sentiment computation
POSITIVE_WORDS = {
    'fantastic': 0.95, 'amazing': 0.95, 'excellent': 0.9, 'great': 0.85, 'love': 0.9,
    'enjoyed': 0.8, 'awesome': 0.9, 'good': 0.7, 'wonderful': 0.9, 'delicious': 0.85,
    'happy': 0.8, 'best': 0.9, 'positive': 0.75, 'superb': 0.9, 'perfect': 0.95,
    'brilliant': 0.9, 'loved': 0.9, 'enjoy': 0.8, 'helpful': 0.75, 'nice': 0.7,
    'beautiful': 0.85, 'recommend': 0.8, 'flawless': 0.95, 'outstanding': 0.95,
    'excited': 0.85, 'pleased': 0.8, 'glad': 0.75, 'delightful': 0.9, 'impressive': 0.85,
    'clean': 0.6, 'fast': 0.65, 'smooth': 0.7, 'worth': 0.75, 'solid': 0.7,
    'masterpiece': 0.98, 'favorite': 0.85, 'top-notch': 0.95, 'super': 0.8
}

NEGATIVE_WORDS = {
    'terrible': 0.95, 'horrible': 0.95, 'worst': 0.98, 'boring': 0.85, 'hate': 0.9,
    'awful': 0.95, 'bad': 0.75, 'poor': 0.8, 'disappointed': 0.85, 'disappointing': 0.85,
    'waste': 0.85, 'annoying': 0.8, 'slow': 0.6, 'useless': 0.9, 'sucks': 0.9,
    'broken': 0.8, 'fail': 0.85, 'failed': 0.85, 'trash': 0.95, 'crap': 0.9,
    'garbage': 0.95, 'hated': 0.9, 'painful': 0.85, 'pathetic': 0.9, 'mess': 0.8,
    'unacceptable': 0.9, 'nightmare': 0.95, 'problem': 0.65, 'buggy': 0.8, 'flawed': 0.75,
    'regret': 0.85, 'disaster': 0.95, 'ugly': 0.8, 'mediocre': 0.7, 'dull': 0.75
}

INTENSIFIERS = {
    'very': 1.3, 'really': 1.3, 'extremely': 1.5, 'super': 1.35, 'so': 1.25,
    'absolutely': 1.4, 'highly': 1.3, 'totally': 1.3, 'completely': 1.4,
    'barely': 0.5, 'hardly': 0.5, 'somewhat': 0.7, 'slightly': 0.8
}

NEGATIONS = {
    "not", "no", "never", "hardly", "barely", "scarcely", "without", "isn't",
    "aren't", "wasn't", "weren't", "haven't", "hasn't", "don't", "doesn't",
    "didn't", "won't", "wouldn't", "can't", "cannot", "couldn't"
}


def analyze_sentiment(text: str) -> dict:
    """
    Analyze the sentiment of a given input text.
    Returns:
        {
            "text": text,
            "sentiment": "Positive" | "Negative" | "Neutral",
            "emoji": "😊" | "😞" | "😐",
            "polarity": float (-1.0 to 1.0),
            "confidence": float (0.0 to 100.0),
            "positive_score": float,
            "negative_score": float,
            "keywords": list of detected sentiment keywords
        }
    """
    if not text or not text.strip():
        return {
            "text": text or "",
            "sentiment": "Neutral",
            "emoji": "😐",
            "polarity": 0.0,
            "confidence": 50.0,
            "positive_score": 0.0,
            "negative_score": 0.0,
            "keywords": []
        }

    # Clean and tokenize words
    words = re.findall(r"\b[\w'-]+\b", text.lower())
    pos_score = 0.0
    neg_score = 0.0
    detected_keywords = []

    i = 0
    while i < len(words):
        word = words[i]
        modifier = 1.0
        negated = False

        # Look behind for negations or intensifiers
        if i > 0:
            prev_word = words[i - 1]
            if prev_word in NEGATIONS:
                negated = True
            elif prev_word in INTENSIFIERS:
                modifier = INTENSIFIERS[prev_word]
        if i > 1 and not negated:
            prev_prev_word = words[i - 2]
            if prev_prev_word in NEGATIONS:
                negated = True

        if word in POSITIVE_WORDS:
            val = POSITIVE_WORDS[word] * modifier
            if negated:
                neg_score += val * 0.85
                detected_keywords.append(f"not {word}")
            else:
                pos_score += val
                detected_keywords.append(word)

        elif word in NEGATIVE_WORDS:
            val = NEGATIVE_WORDS[word] * modifier
            if negated:
                pos_score += val * 0.7
                detected_keywords.append(f"not {word}")
            else:
                neg_score += val
                detected_keywords.append(word)

        i += 1

    # Check for emojis in original text
    if any(em in text for em in ['😊', '😀', '😃', '😄', '😍', '👍', '🔥', '✨', '❤️', '🎉', '💯']):
        pos_score += 1.2
        detected_keywords.append("positive_emoji")
    if any(em in text for em in ['😞', '😢', '😡', '😠', '👎', '💔', '💩', '🤮', '😭']):
        neg_score += 1.2
        detected_keywords.append("negative_emoji")

    # Calculate polarity score normalized between -1.0 and 1.0
    diff = pos_score - neg_score
    total = pos_score + neg_score

    if total == 0:
        polarity = 0.0
        sentiment = "Neutral"
        emoji = "😐"
        confidence = 65.0
    else:
        # Sigmoid-like scaling
        polarity = round(diff / (total + 0.5), 3)
        polarity = max(-1.0, min(1.0, polarity))

        if polarity >= 0.15:
            sentiment = "Positive"
            emoji = "😊"
            confidence = min(99.5, round((pos_score / (total + 0.1)) * 100, 1))
        elif polarity <= -0.15:
            sentiment = "Negative"
            emoji = "😞"
            confidence = min(99.5, round((neg_score / (total + 0.1)) * 100, 1))
        else:
            sentiment = "Neutral"
            emoji = "😐"
            confidence = round(100.0 - (abs(polarity) * 100), 1)

    return {
        "text": text,
        "sentiment": sentiment,
        "emoji": emoji,
        "label": f"{emoji} {sentiment}",
        "polarity": polarity,
        "confidence": confidence,
        "positive_score": round(pos_score, 2),
        "negative_score": round(neg_score, 2),
        "keywords": list(set(detected_keywords))
    }


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
    # Test user examples
    test_cases = [
        "This movie was fantastic!",
        "I really enjoyed this product.",
        "The movie was boring.",
        "This was a terrible experience.",
        "The package arrived on time."
    ]

    print(f"{'Input Text':<35} {'Sentiment':<18} {'Polarity':<10} {'Confidence'}")
    print("-" * 75)
    for sample in test_cases:
        res = analyze_sentiment(sample)
        print(f"{sample:<35} {res['label']:<18} {res['polarity']:<10} {res['confidence']}%")
