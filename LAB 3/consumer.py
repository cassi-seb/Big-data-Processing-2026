# %%
from confluent_kafka import Consumer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'book_group',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

# %%
topic='read_book'
consumer.subscribe([topic])

# %%
stop_words = {
    'a', 'an', 'the', 'and', 'or', 'but', 'if', 'then', 'else', 'when',
    'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into',
    'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from',
    'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'is', 'are',
    'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do',
    'does', 'did', 'this', 'that', 'these', 'those', 'i', 'you', 'he',
    'she', 'it', 'we', 'they', 'of', 'as'
}

punctuation = {
    '.', ',', '!', '?', ';', ':', '"', "'", '(', ')', '[', ']', '{', '}',
    '-', '_', '/', '\\', '|', '*', '&', '^', '%', '$', '#', '@', '~', '`',
    '+', '=', '<', '>'
}
translator = str.maketrans('', '', ''.join(punctuation))

def clean_line(line):
    words = line.lower().split()
    cleaned = []
    for word in words:
        word = word.translate(translator)
        if word and word not in stop_words:
            cleaned.append(word)
    return cleaned

# %%
# Configuration
MAX_EMPTY_POLLS = 10  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
empty_polls = 0
error_count = 0

output_file = open('cleaned_book.txt', 'w', encoding='utf-8')

while True:
    msg = consumer.poll(1.0)

    # 1. Handle "No Message" (Timeout)
    if msg is None:
        empty_polls += 1
        if empty_polls >= MAX_EMPTY_POLLS:
            print("Closing: No new messages received.")
            break
        continue

    # 2. Handle Errors
    if msg.error():
        error_count += 1
        print(f"Consumer error: {msg.error()}")
        if error_count >= MAX_ERRORS:
            print("Closing: Too many consecutive errors.")
            break
        continue

    # 3. Handle Success
    empty_polls = 0
    error_count = 0

    raw_line = msg.value().decode('utf-8')
    cleaned_words = clean_line(raw_line)

    if cleaned_words:
        out_line = ' '.join(cleaned_words)
        output_file.write(out_line + '\n')
        output_file.flush()
        print(f"Cleaned: {out_line}")

# Clean up
output_file.close()
consumer.close()
