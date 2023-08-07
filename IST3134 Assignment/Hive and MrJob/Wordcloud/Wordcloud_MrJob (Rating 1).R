# Install and load the required libraries
install.packages("wordcloud2")  # Install wordcloud2 package
install.packages("readr")       # Install readr package
install.packages("RColorBrewer")# Install RColorBrewer package

library(wordcloud2)
library(readr)
library(RColorBrewer)

# Read the word count data from "1_mrjob_wordcount_result.txt"
wordcount <- read_delim("/Users/jiene/OneDrive/Desktop/Hive and MrJob/1_mrjob_wordcount_result.txt", 
                        delim = "\t", col_names = c("Word", "Count"))

# Remove double quotes from the "Word" column
wordcount$Word <- gsub("\"", "", wordcount$Word)

# Filter the data to include only words with a count greater than 2000
filtered_wordcount <- subset(wordcount, Count > 2000)

# Convert the "Count" column to numeric (in case it is read as a character)
filtered_wordcount$Count <- as.numeric(filtered_wordcount$Count)

# Create the word cloud using wordcloud2
wordcloud2(filtered_wordcount, size = 1.5, color = "random-dark", backgroundColor = "white")

