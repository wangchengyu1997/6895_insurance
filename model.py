from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from gensim import corpora, models
from gensim.models.coherencemodel import CoherenceModel
import nltk

bigtable_client = bigtable.Client(project='empirical-axon-416223')
instance = bigtable_client.instance('redditinsurance')
table = instance.table('PostData')
column_family = 'PostInfo'
stop_words = set(stopwords.words('english'))

def read_from_bigtable():
    """Read data from Bigtable."""
    rows = table.read_rows()
    rows.consume_all()

    texts = []
    for row in rows.rows.values():
        text_column = row.cells[column_family]["text".encode()][0].value.decode()
        texts.append(text_column)

    return texts
    """
    row_filter = row_filters.RowFilterChain(filters=[
        row_filters.ColumnQualifierRegexFilter(b'type'),
        row_filters.ValueRegexFilter(b'post')
    ])

    # Read rows with the specified filter
    partial_rows = table.read_rows(filter_=row_filter)
    partial_rows.consume_all()

    posts = []
    for row in partial_rows.rows.values():
        text_column = row.cells[column_family]["text".encode()][0].value.decode()
        posts.append(text_column)
    return posts
    """

def preprocess_texts(texts):
    processed_texts = []
    for text in texts:
        tokens = word_tokenize(text)
        tokens = [w.lower() for w in tokens if w.isalpha()]
        tokens = [w for w in tokens if not w in stop_words]
        processed_texts.append(tokens)
    return processed_texts

texts = read_from_bigtable()
processed_texts = preprocess_texts(texts)

# Create a dictionary representation of the documents
dictionary = corpora.Dictionary(processed_texts)
corpus = [dictionary.doc2bow(text) for text in processed_texts]

def main():
    coherence_values = []
    for num_topics in range(2, 11):
        # Apply LDA
        lda_model = models.LdaModel(corpus, num_topics=num_topics, id2word=dictionary, passes=15, random_state=10)
        coherence_model = CoherenceModel(model=lda_model, corpus=corpus, dictionary=dictionary, coherence='u_mass')
        # Print the topics
        topics = lda_model.print_topics(num_words=10)
        print(f"Number of topics: {num_topics}")
        coherence_values.append(coherence_model.get_coherence())
        for topic in topics:
            print(topic)

    print(coherence_values)


if __name__ == '__main__':
    main()



