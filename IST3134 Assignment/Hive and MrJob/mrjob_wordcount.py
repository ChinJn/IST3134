import re
from mrjob.job import MRJob

class MRWordCount(MRJob):
    def clean_text(self, text):
        # Define a list of stopwords
        stopwords = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'you’re', 'you’ve', 'you’ll', 'you’d', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'she’s', 'her', 'hers', 'herself', 'it', 'it’s', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'that’ll', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'don’t', 'should', 'should’ve', 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', 'aren’t', 'couldn', 'couldn’t', 'didn', 'didn’t', 'doesn', 'doesn’t', 'hadn', 'hadn’t', 'hasn', 'hasn’t', 'haven', 'haven’t', 'isn', 'isn’t', 'ma', 'mightn', 'mightn’t', 'mustn', 'mustn’t', 'needn', 'needn’t', 'shan', 'shan’t', 'shouldn', 'shouldn’t', 'wasn', 'wasn’t', 'weren', 'weren’t', 'won', 'won’t', 'wouldn', 'wouldn’t']

        # Remove punctuation, numeric characters, convert to lowercase, and remove stopwords
        cleaned_text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
        cleaned_text = re.sub(r'\d+', '', cleaned_text)  # Remove numeric characters
        cleaned_text = cleaned_text.lower()  # Convert to lowercase
        cleaned_text = ' '.join([word for word in cleaned_text.split() if word not in stopwords])  # Remove stopwords
        return cleaned_text

    def mapper(self, _, line):
        for word in line.split():
            cleaned_word = self.clean_text(word)
            if cleaned_word:  # Check if the cleaned word is not empty
                yield (cleaned_word, 1)

    def reducer(self, word, counts):
        total_count = sum(counts)
        if total_count > 100:
            yield (word, total_count)

if __name__ == '__main__':
    MRWordCount.run()

