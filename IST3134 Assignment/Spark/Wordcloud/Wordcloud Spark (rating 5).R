# Install and load the required libraries
install.packages("wordcloud2")  # Install wordcloud2 package
install.packages("readr")       # Install readr package
install.packages("RColorBrewer")# Install RColorBrewer package

library(wordcloud2)
library(readr)
library(RColorBrewer)

# Read the word count data from "combined_wc5spark.txt"
wordcount <- read_delim("/Users/jiene/OneDrive/Desktop/Spark/combined_wc5spark.txt", 
                        delim = " ", col_names = c("Word", "Count"))

# Remove punctuation from the "Word" column
wordcount$Word <- gsub("[[:punct:]]", "", wordcount$Word)

# Remove punctuation from the "Count" column
wordcount$Count <- gsub("[[:punct:]]", "", wordcount$Count)

# Filter the data to include only words with a count greater than 2000
filtered_wordcount <- subset(wordcount, Count > 2000)

# Convert the "Count" column to numeric (in case it is read as a character)
filtered_wordcount$Count <- as.numeric(filtered_wordcount$Count)

# Create the word cloud using wordcloud2
wordcloud2(filtered_wordcount, size = 1.5, color = "random-dark", backgroundColor = "white")

