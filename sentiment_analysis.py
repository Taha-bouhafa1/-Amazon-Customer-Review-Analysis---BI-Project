import mysql.connector
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk import download

# Download VADER lexicon
download('vader_lexicon')

# Initialize VADER sentiment analyzer
sia = SentimentIntensityAnalyzer()

# Function to determine sentiment using VADER
def get_sentiment(text):
    polarity = sia.polarity_scores(text)['compound']  # Get compound score
    if polarity > 0.5:
        return 'positive'
    elif polarity > 0.2:
        return 'little positive'
    elif polarity < -0.5:
        return 'negative'
    elif polarity < -0.2:
        return 'little negative'
    else:
        return 'neutral'

# Connect to the MySQL database
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="review"
)

cursor = db.cursor()

# Add Sentiment column if it doesn't exist
try:
    cursor.execute("ALTER TABLE cleaned_reviews ADD COLUMN Sentiment VARCHAR(20)")
    db.commit()
except:
    pass  # Ignore if the column already exists

# Process reviews in batches
batch_size = 50000
offset = 0

while True:
    # Fetch a batch of reviews
    fetch_query = f"SELECT Id, Text FROM cleaned_reviews WHERE Sentiment IS NULL LIMIT {batch_size} OFFSET {offset}"
    cursor.execute(fetch_query)
    reviews = cursor.fetchall()

    # Break if no more rows are left
    if not reviews:
        break

    # Process the batch
    for review in reviews:
        review_id, text = review
        sentiment = get_sentiment(text)
        cursor.execute("UPDATE cleaned_reviews SET Sentiment = %s WHERE Id = %s", (sentiment, review_id))

    # Commit after each batch
    db.commit()

    # Move to the next batch
    offset += batch_size
    print("batch done")
# Close the connection
cursor.close()
db.close()

print("Sentiment analysis completed and database updated.")
